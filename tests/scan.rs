//! `SCAN`: ordered range reads over both storage engines, the wire codec,
//! and pagination through the server.

use std::collections::BTreeMap;
use std::path::Path;

use distributedb::protocol::{decode_scan_page, encode_scan_page, MAX_SCAN_DATA_LEN};
use distributedb::{
    decode_request_body, parse, Client, Command, Db, DurabilityMode, LsmConfig, OpenConfig,
    ParseError, Request, Server, ServerConfig, SimConfig, SimFs, Status, StorageKind,
};

type Pairs = Vec<(Vec<u8>, Vec<u8>)>;
/// Start bound, optional end bound, and limit.
type ScanCase = (&'static [u8], Option<&'static [u8]>, usize);

fn lsm() -> OpenConfig {
    OpenConfig {
        storage: StorageKind::Lsm(LsmConfig {
            memtable_bytes: 4 * 1024,
            l0_compaction_trigger: 3,
            target_table_bytes: 8 * 1024,
            durable: true,
        }),
        ..OpenConfig::default()
    }
}

fn key(i: u32) -> Vec<u8> {
    format!("key-{i:05}").into_bytes()
}

/// The request body as the server sees it: the frame minus its length prefix.
fn body(request: &Request) -> Vec<u8> {
    request.encode()[4..].to_vec()
}

#[test]
fn scan_requests_and_pages_round_trip_and_reject_malformed_input() {
    let request = Request::Scan {
        start: b"a".to_vec(),
        end: Some(b"m".to_vec()),
        limit: 50,
    };
    assert_eq!(decode_request_body(&body(&request)).unwrap(), request);
    let open = Request::Scan {
        start: Vec::new(),
        end: None,
        limit: 1,
    };
    assert_eq!(decode_request_body(&body(&open)).unwrap(), open);

    for limit in [0, 10_001] {
        let bad = Request::Scan {
            start: Vec::new(),
            end: None,
            limit,
        };
        assert!(decode_request_body(&body(&bad)).is_err(), "limit {limit}");
    }
    let long = Request::Scan {
        start: vec![b'k'; 4097],
        end: None,
        limit: 1,
    };
    assert!(decode_request_body(&body(&long)).is_err());
    let mut trailing = body(&request);
    trailing.push(0);
    assert!(decode_request_body(&trailing).is_err());

    let pairs = vec![
        (b"a".to_vec(), b"".to_vec()),
        (b"b".to_vec(), b"value".to_vec()),
    ];
    let page = encode_scan_page(&pairs, true);
    assert_eq!(decode_scan_page(&page).unwrap(), (pairs, true));
    assert!(decode_scan_page(&page[..page.len() - 1]).is_err());
    let mut bad_flag = page.clone();
    bad_flag[4] = 2;
    assert!(decode_scan_page(&bad_flag).is_err());
}

#[test]
fn scan_command_parses_bounds_and_limit() {
    assert_eq!(
        parse("SCAN a z 10").unwrap(),
        Command::Scan {
            start: b"a".to_vec(),
            end: Some(b"z".to_vec()),
            limit: 10,
        }
    );
    assert_eq!(
        parse("scan * * 1").unwrap(),
        Command::Scan {
            start: Vec::new(),
            end: None,
            limit: 1,
        }
    );
    assert!(matches!(
        parse("SCAN a z 0"),
        Err(ParseError::InvalidLimit(_))
    ));
    assert!(matches!(
        parse("SCAN a z"),
        Err(ParseError::WrongArgCount { got: 2, .. })
    ));
}

/// Writes, overwrites, and deletes; the LSM database spreads them over
/// level 1, level 0, and the memtable.
fn fill(db: &mut Db<SimFs>) -> BTreeMap<Vec<u8>, Vec<u8>> {
    let mut model = BTreeMap::new();
    for i in 0..1500u32 {
        let value = format!("v{i}").into_bytes();
        db.set(key(i % 600), value.clone()).unwrap();
        model.insert(key(i % 600), value);
        if i % 11 == 0 {
            db.delete(key((i * 7) % 600)).unwrap();
            model.remove(&key((i * 7) % 600));
        }
    }
    model
}

fn expected(
    model: &BTreeMap<Vec<u8>, Vec<u8>>,
    start: &[u8],
    end: Option<&[u8]>,
    limit: usize,
) -> (Pairs, bool) {
    let in_range: Pairs = model
        .iter()
        .filter(|(k, _)| {
            k.as_slice() >= start
                && match end {
                    Some(end) => k.as_slice() < end,
                    None => true,
                }
        })
        .map(|(k, v)| (k.clone(), v.clone()))
        .collect();
    let more = in_range.len() > limit;
    (in_range.into_iter().take(limit).collect(), more)
}

#[test]
fn both_engines_return_ordered_live_ranges() {
    for (name, config) in [("memory", OpenConfig::default()), ("lsm", lsm())] {
        let fs = SimFs::new(SimConfig::new(31));
        let mut db =
            Db::open_configured(fs, Path::new("/db"), DurabilityMode::Fsync, config).unwrap();
        let model = fill(&mut db);
        if name == "lsm" {
            let stats = db.lsm_stats().unwrap();
            assert!(stats.tables >= 2 && stats.compactions >= 1, "{stats:?}");
        }
        let cases: [ScanCase; 6] = [
            (b"", None, 10_000),
            (b"", None, 7),
            (b"key-00100", Some(b"key-00200"), 1_000),
            (b"key-00100", Some(b"key-00200"), 13),
            (b"key-00599", None, 5),
            (b"zzz", None, 5),
        ];
        for (start, end, limit) in cases {
            assert_eq!(
                db.scan(start, end, limit).unwrap(),
                expected(&model, start, end, limit),
                "{name} scan {start:?}..{end:?} limit {limit}"
            );
        }
    }
}

#[test]
fn server_pages_through_a_range_on_both_engines() {
    for config in [OpenConfig::default(), lsm()] {
        let fs = SimFs::new(SimConfig::new(32));
        let mut db =
            Db::open_configured(fs, Path::new("/db"), DurabilityMode::Fsync, config).unwrap();
        let model = fill(&mut db);
        let mut server = Server::start("127.0.0.1:0", db, ServerConfig::default()).unwrap();
        let mut client = Client::connect(server.local_addr()).unwrap();

        let mut collected = Vec::new();
        let mut start = Vec::new();
        loop {
            let (pairs, more) = client.scan(start.clone(), None, 97).unwrap();
            assert!(pairs.len() <= 97);
            if let Some((last, _)) = pairs.last() {
                start = last.clone();
                start.push(0);
            }
            collected.extend(pairs);
            if !more {
                break;
            }
        }
        let all: Pairs = model.into_iter().collect();
        assert_eq!(collected, all);
        drop(client);
        server.shutdown();
    }
}

#[test]
fn server_cuts_a_page_at_the_frame_limit() {
    let fs = SimFs::new(SimConfig::new(33));
    let mut db = Db::open(fs, Path::new("/db"), DurabilityMode::Fsync).unwrap();
    for i in 0..30u32 {
        db.set(key(i), vec![b'x'; 100_000]).unwrap();
    }
    let mut server = Server::start("127.0.0.1:0", db, ServerConfig::default()).unwrap();
    let mut client = Client::connect(server.local_addr()).unwrap();
    let (pairs, more) = client.scan(Vec::new(), None, 30).unwrap();
    assert!(more, "30 values of 100 KB do not fit one frame");
    assert!(!pairs.is_empty() && pairs.len() < 30);
    let page_len: usize = 5 + pairs
        .iter()
        .map(|(k, v)| 8 + k.len() + v.len())
        .sum::<usize>();
    assert!(page_len <= MAX_SCAN_DATA_LEN);
    assert_eq!(pairs[0].0, key(0));

    // The connection stays usable after a large page.
    assert_eq!(
        client.set(b"k".to_vec(), b"v".to_vec()).unwrap(),
        Status::Ok
    );
    assert_eq!(client.get(b"k".to_vec()).unwrap(), Some(b"v".to_vec()));
    drop(client);
    server.shutdown();
}

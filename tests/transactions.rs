//! Simple transactions (SOW §15): `BEGIN`, queued writes, `COMMIT` as one
//! WAL group, `ROLLBACK`, and the limits around them.

use std::path::Path;
use std::sync::atomic::{AtomicBool, Ordering};
use std::sync::Arc;
use std::thread;

use distributedb::{
    decode_request_body, parse, Client, Command, Db, DurabilityMode, GetResult, Request, Server,
    ServerConfig, SimConfig, SimFs, Status,
};

fn start(seed: u64) -> (Server, SimFs) {
    let fs = SimFs::new(SimConfig::new(seed));
    let db = Db::open(fs.clone(), Path::new("/db"), DurabilityMode::Fsync).unwrap();
    let server = Server::start("127.0.0.1:0", db, ServerConfig::default()).unwrap();
    (server, fs)
}

fn body(request: &Request) -> Vec<u8> {
    request.encode()[4..].to_vec()
}

#[test]
fn transaction_requests_round_trip_and_parse() {
    for request in [Request::Begin, Request::Commit, Request::Rollback] {
        assert_eq!(decode_request_body(&body(&request)).unwrap(), request);
    }
    assert_eq!(parse("begin").unwrap(), Command::Begin);
    assert_eq!(parse("COMMIT").unwrap(), Command::Commit);
    assert_eq!(parse("ROLLBACK").unwrap(), Command::Rollback);
    assert!(parse("BEGIN now").is_err());
}

#[test]
fn commit_applies_every_queued_write_and_hides_them_until_then() {
    let (mut server, _fs) = start(51);
    let mut writer = Client::connect(server.local_addr()).unwrap();
    let mut other = Client::connect(server.local_addr()).unwrap();
    assert_eq!(
        writer.set(b"gone".to_vec(), b"x".to_vec()).unwrap(),
        Status::Ok
    );

    assert_eq!(writer.begin().unwrap(), Status::Ok);
    assert_eq!(
        writer.set(b"account:A".to_vec(), b"90".to_vec()).unwrap(),
        Status::Queued
    );
    assert_eq!(
        writer.set(b"account:B".to_vec(), b"110".to_vec()).unwrap(),
        Status::Queued
    );
    assert_eq!(writer.delete(b"gone".to_vec()).unwrap(), Status::Queued);

    // The transaction reads its own writes; another connection does not.
    assert_eq!(
        writer.get(b"account:A".to_vec()).unwrap(),
        Some(b"90".to_vec())
    );
    assert!(!writer.exists(b"gone".to_vec()).unwrap());
    assert_eq!(other.get(b"account:A".to_vec()).unwrap(), None);
    assert!(other.exists(b"gone".to_vec()).unwrap());

    assert_eq!(writer.commit().unwrap(), Status::Ok);
    assert_eq!(
        other.get(b"account:A".to_vec()).unwrap(),
        Some(b"90".to_vec())
    );
    assert_eq!(
        other.get(b"account:B".to_vec()).unwrap(),
        Some(b"110".to_vec())
    );
    assert!(!other.exists(b"gone".to_vec()).unwrap());

    // After COMMIT, writes are immediate again.
    assert_eq!(
        writer.set(b"after".to_vec(), b"1".to_vec()).unwrap(),
        Status::Ok
    );
    drop((writer, other));
    server.shutdown();
}

#[test]
fn rollback_and_disconnect_discard_queued_writes() {
    let (mut server, _fs) = start(52);
    let mut client = Client::connect(server.local_addr()).unwrap();
    assert_eq!(client.begin().unwrap(), Status::Ok);
    assert_eq!(
        client.set(b"k".to_vec(), b"v".to_vec()).unwrap(),
        Status::Queued
    );
    assert_eq!(client.rollback().unwrap(), Status::Ok);
    assert_eq!(client.get(b"k".to_vec()).unwrap(), None);

    assert_eq!(client.begin().unwrap(), Status::Ok);
    assert_eq!(
        client.set(b"k".to_vec(), b"v".to_vec()).unwrap(),
        Status::Queued
    );
    drop(client);
    let mut client = Client::connect(server.local_addr()).unwrap();
    assert_eq!(client.get(b"k".to_vec()).unwrap(), None);
    drop(client);
    server.shutdown();
}

#[test]
fn misuse_is_rejected_without_closing_the_connection() {
    let (mut server, _fs) = start(53);
    let mut client = Client::connect(server.local_addr()).unwrap();
    assert_eq!(client.commit().unwrap(), Status::BadRequest);
    assert_eq!(client.rollback().unwrap(), Status::BadRequest);
    assert_eq!(client.begin().unwrap(), Status::Ok);
    assert_eq!(client.begin().unwrap(), Status::BadRequest);
    assert!(client.scan(Vec::new(), None, 10).is_err());
    assert_eq!(client.commit().unwrap(), Status::Ok); // Empty: nothing to write.
    client.ping().unwrap();
    drop(client);
    server.shutdown();
}

#[test]
fn a_transaction_larger_than_one_group_is_aborted_whole() {
    let (mut server, _fs) = start(54);
    let mut client = Client::connect(server.local_addr()).unwrap();
    assert_eq!(client.begin().unwrap(), Status::Ok);
    for i in 0..64u32 {
        let key = format!("key-{i}").into_bytes();
        assert_eq!(client.set(key, b"v".to_vec()).unwrap(), Status::Queued);
    }
    assert_eq!(
        client.set(b"key-64".to_vec(), b"v".to_vec()).unwrap(),
        Status::ResourceExhausted
    );
    assert_eq!(
        client.set(b"key-65".to_vec(), b"v".to_vec()).unwrap(),
        Status::BadRequest
    );
    assert_eq!(client.commit().unwrap(), Status::BadRequest);
    assert_eq!(client.get(b"key-0".to_vec()).unwrap(), None);
    drop(client);
    server.shutdown();
}

#[test]
fn concurrent_readers_never_see_half_a_transfer() {
    let (mut server, _fs) = start(55);
    let addr = server.local_addr();
    let mut setup = Client::connect(addr).unwrap();
    assert_eq!(
        setup.set(b"a".to_vec(), b"100".to_vec()).unwrap(),
        Status::Ok
    );
    assert_eq!(
        setup.set(b"b".to_vec(), b"100".to_vec()).unwrap(),
        Status::Ok
    );

    let done = Arc::new(AtomicBool::new(false));
    let reader = {
        let done = Arc::clone(&done);
        thread::spawn(move || {
            let mut client = Client::connect(addr).unwrap();
            let mut reads = 0u32;
            while !done.load(Ordering::SeqCst) || reads == 0 {
                // One SCAN page is read under one lock, so it is a
                // consistent view of both keys.
                let (pairs, _) = client.scan(b"a".to_vec(), Some(b"c".to_vec()), 2).unwrap();
                let total: u32 = pairs
                    .iter()
                    .map(|(_, v)| String::from_utf8_lossy(v).parse::<u32>().unwrap())
                    .sum();
                assert_eq!(total, 200, "saw a partial transfer: {pairs:?}");
                reads += 1;
            }
            reads
        })
    };

    let mut writer = Client::connect(addr).unwrap();
    for i in 0..200u32 {
        let a = 100 - (i % 50);
        assert_eq!(writer.begin().unwrap(), Status::Ok);
        writer
            .set(b"a".to_vec(), a.to_string().into_bytes())
            .unwrap();
        writer
            .set(b"b".to_vec(), (200 - a).to_string().into_bytes())
            .unwrap();
        assert_eq!(writer.commit().unwrap(), Status::Ok);
    }
    done.store(true, Ordering::SeqCst);
    assert!(reader.join().unwrap() > 0);
    drop((setup, writer));
    server.shutdown();
}

#[test]
fn committed_transactions_survive_a_crash() {
    let (mut server, fs) = start(56);
    let mut client = Client::connect(server.local_addr()).unwrap();
    assert_eq!(client.begin().unwrap(), Status::Ok);
    for i in 0..10u32 {
        let key = format!("k{i}").into_bytes();
        assert_eq!(client.set(key, b"v".to_vec()).unwrap(), Status::Queued);
    }
    assert_eq!(client.commit().unwrap(), Status::Ok);
    // An open transaction at the crash is lost entirely.
    assert_eq!(client.begin().unwrap(), Status::Ok);
    assert_eq!(
        client.set(b"open".to_vec(), b"v".to_vec()).unwrap(),
        Status::Queued
    );
    drop(client);
    server.shutdown();
    drop(server);

    fs.crash();
    let db = Db::open(fs, Path::new("/db"), DurabilityMode::Fsync).unwrap();
    for i in 0..10u32 {
        let key = format!("k{i}").into_bytes();
        assert_eq!(db.get(&key), GetResult::Found(b"v".to_vec()), "k{i}");
    }
    assert_eq!(db.get(b"open"), GetResult::NotFound);
}

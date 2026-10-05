//! Numeric networking acceptance gates from Technical-Design §13, grounded in
//! the SOW's concurrent-client and partial-message requirements (§§5, 13, 20).

use std::io::{self, Read};
use std::path::Path;
use std::sync::{Arc, Barrier};
use std::thread;

use distributedb::{
    decode_request_body, read_frame, Client, Db, DurabilityMode, Request, Server, ServerConfig,
    SimConfig, SimFs, Status,
};

/// A reader that exposes the first `split` bytes separately from the rest.
/// This exercises every possible boundary within a representative frame.
struct SplitReader<'a> {
    bytes: &'a [u8],
    position: usize,
    split: usize,
}

impl Read for SplitReader<'_> {
    fn read(&mut self, out: &mut [u8]) -> io::Result<usize> {
        if out.is_empty() || self.position == self.bytes.len() {
            return Ok(0);
        }
        let boundary = if self.position < self.split {
            self.split
        } else {
            self.bytes.len()
        };
        let len = out
            .len()
            .min(boundary - self.position)
            .min(self.bytes.len() - self.position);
        out[..len].copy_from_slice(&self.bytes[self.position..self.position + len]);
        self.position += len;
        Ok(len)
    }
}

#[test]
fn every_byte_split_of_representative_frames_is_reassembled() {
    let requests = [
        Request::Set {
            key: vec![0, 255, b'k'],
            value: vec![b'v', 0, 1, 254],
        },
        Request::Get {
            key: b"key".to_vec(),
        },
        Request::Delete {
            key: b"key".to_vec(),
        },
        Request::Exists {
            key: b"key".to_vec(),
        },
        Request::Stats,
        Request::Scan {
            start: b"a".to_vec(),
            end: Some(b"z".to_vec()),
            limit: 10,
        },
    ];
    for request in requests {
        let frame = request.encode();
        for split in 1..frame.len() {
            let mut reader = SplitReader {
                bytes: &frame,
                position: 0,
                split,
            };
            let body = read_frame(&mut reader).unwrap().unwrap();
            assert_eq!(
                decode_request_body(&body).unwrap(),
                request,
                "split={split}"
            );
            assert!(read_frame(&mut reader).unwrap().is_none());
        }
        for cut in 1..frame.len() {
            let mut truncated = io::Cursor::new(&frame[..cut]);
            assert!(read_frame(&mut truncated).is_err(), "cut={cut}");
        }
    }
}

/// Deterministically returns short or coalesced chunks and occasionally an
/// interrupted read. The frame reader must preserve boundaries across a
/// stream of requests without depending on a particular socket read size.
struct SeededReader<'a> {
    bytes: &'a [u8],
    position: usize,
    state: u64,
}

impl SeededReader<'_> {
    fn next(&mut self) -> u64 {
        self.state ^= self.state << 13;
        self.state ^= self.state >> 7;
        self.state ^= self.state << 17;
        self.state
    }
}

impl Read for SeededReader<'_> {
    fn read(&mut self, out: &mut [u8]) -> io::Result<usize> {
        if out.is_empty() || self.position == self.bytes.len() {
            return Ok(0);
        }
        let draw = self.next();
        if draw.is_multiple_of(19) {
            return Err(io::Error::new(io::ErrorKind::Interrupted, "seeded EINTR"));
        }
        let len = out
            .len()
            .min(self.bytes.len() - self.position)
            .min(1 + (draw as usize % 128));
        out[..len].copy_from_slice(&self.bytes[self.position..self.position + len]);
        self.position += len;
        Ok(len)
    }
}

#[test]
fn thousand_seeded_fragmented_and_coalesced_streams_parse_in_order() {
    let requests = [
        Request::Set {
            key: b"alpha".to_vec(),
            value: vec![1; 257],
        },
        Request::Get {
            key: b"alpha".to_vec(),
        },
        Request::Exists {
            key: b"alpha".to_vec(),
        },
        Request::Delete {
            key: b"alpha".to_vec(),
        },
        Request::Stats,
        Request::Scan {
            start: Vec::new(),
            end: None,
            limit: 64,
        },
    ];
    let expected: Vec<_> = requests.iter().cycle().take(18).cloned().collect();
    let stream: Vec<u8> = expected.iter().flat_map(Request::encode).collect();
    for seed in 1..=1000 {
        let mut reader = SeededReader {
            bytes: &stream,
            position: 0,
            state: seed,
        };
        for (index, request) in expected.iter().enumerate() {
            let body = read_frame(&mut reader)
                .unwrap_or_else(|error| panic!("seed={seed} frame={index}: {error}"))
                .unwrap();
            assert_eq!(
                decode_request_body(&body).unwrap(),
                *request,
                "seed={seed} frame={index}"
            );
        }
        assert!(read_frame(&mut reader).unwrap().is_none(), "seed={seed}");
    }
}

#[test]
fn thirty_two_clients_complete_over_ten_thousand_mixed_operations() {
    const CLIENTS: usize = 32;
    const ROUNDS: usize = 80;
    const OPERATIONS: usize = CLIENTS * ROUNDS * 4;
    const { assert!(OPERATIONS >= 10_000) };

    let fs = SimFs::new(SimConfig::new(0xacc3_5500));
    let db = Db::open(fs, Path::new("/network-acceptance"), DurabilityMode::Fsync).unwrap();
    let mut server = Server::start("127.0.0.1:0", db, ServerConfig::default()).unwrap();
    let addr = server.local_addr();
    let barrier = Arc::new(Barrier::new(CLIENTS));
    let workers: Vec<_> = (0..CLIENTS)
        .map(|client_id| {
            let barrier = Arc::clone(&barrier);
            thread::spawn(move || {
                let mut client = Client::connect(addr).unwrap();
                barrier.wait();
                for round in 0..ROUNDS {
                    let key = format!("client:{client_id}:round:{round}").into_bytes();
                    let value = format!("value:{client_id}:{round}").into_bytes();
                    assert_eq!(client.set(key.clone(), value.clone()).unwrap(), Status::Ok);
                    assert_eq!(client.get(key.clone()).unwrap(), Some(value));
                    assert!(client.exists(key.clone()).unwrap());
                    assert_eq!(client.delete(key).unwrap(), Status::Ok);
                }
            })
        })
        .collect();
    for worker in workers {
        worker.join().unwrap();
    }

    let mut verifier = Client::connect(addr).unwrap();
    let stats = verifier.stats().unwrap();
    let stat = |name: &str| -> u64 {
        stats
            .lines()
            .find_map(|line| {
                line.strip_prefix(&format!("{name}="))
                    .and_then(|v| v.parse().ok())
            })
            .unwrap_or_else(|| panic!("missing {name} in {stats}"))
    };
    assert_eq!(stat("current_lsn"), (CLIENTS * ROUNDS * 2) as u64);
    assert_eq!(stat("wal_entries"), (CLIENTS * ROUNDS * 2) as u64);
    assert_eq!(stat("keys"), 0);
    assert!(stat("requests_total") >= OPERATIONS as u64);
    server.shutdown();
}

//! `PING` (SOW §4.1 optional command) and the CLI forms of `PING` and
//! `STATS`.

use std::path::Path;

use distributedb::protocol::KIND_PING;
use distributedb::{
    decode_request_body, parse, Client, Command, Db, DurabilityMode, ParseError, ProtocolError,
    Request, Server, ServerConfig, SimConfig, SimFs, Status,
};

/// The request body as the server sees it: the frame minus its length prefix.
fn body(request: &Request) -> Vec<u8> {
    request.encode()[4..].to_vec()
}

#[test]
fn ping_round_trips_and_rejects_a_payload() {
    let encoded = body(&Request::Ping);
    assert_eq!(encoded, vec![1, KIND_PING]);
    assert_eq!(decode_request_body(&encoded).unwrap(), Request::Ping);
    assert!(matches!(
        decode_request_body(&[1, KIND_PING, 0]),
        Err(ProtocolError::TrailingBytes)
    ));
}

#[test]
fn ping_and_stats_parse_without_arguments() {
    assert_eq!(parse("PING").unwrap(), Command::Ping);
    assert_eq!(parse("  stats ").unwrap(), Command::Stats);
    assert!(matches!(
        parse("PING extra"),
        Err(ParseError::WrongArgCount {
            command: "PING",
            got: 1,
            ..
        })
    ));
}

#[test]
fn server_answers_ping_without_touching_the_database() {
    let fs = SimFs::new(SimConfig::new(41));
    let db = Db::open(fs, Path::new("/db"), DurabilityMode::Fsync).unwrap();
    let mut server = Server::start("127.0.0.1:0", db, ServerConfig::default()).unwrap();
    let mut client = Client::connect(server.local_addr()).unwrap();
    client.ping().unwrap();
    assert_eq!(
        client.set(b"k".to_vec(), b"v".to_vec()).unwrap(),
        Status::Ok
    );
    client.ping().unwrap();
    let stats = client.stats().unwrap();
    assert!(
        stats.lines().any(|line| line == "keys=1"),
        "PING is not a write: {stats}"
    );
    drop(client);
    server.shutdown();
}

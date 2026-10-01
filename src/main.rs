//! DistributeDB binary entry point.
//!
//! Phase 1 provides a minimal stdin REPL over the in-memory
//! [`StorageEngine`](distributedb::StorageEngine). It reads one command per
//! line, parses it with [`distributedb::parse`], executes it against a single
//! in-memory engine instance, and prints a human-readable response consistent
//! with the SOW §4 example behavior:
//!
//! * `SET key value` and `DELETE key` print `OK`.
//! * `GET key` prints the stored value (as UTF-8 text). A missing key prints a
//!   `NOT_FOUND` message; an empty stored value prints an empty line, which is
//!   distinct from `NOT_FOUND`.
//! * `EXISTS key` prints `true` or `false`.
//! * A parse error prints a `BAD_REQUEST` message to stderr and the loop
//!   continues (it never panics).
//!
//! The REPL exits cleanly on end-of-input (EOF).
//!
//! Phase 3 adds two additive subcommands (argv-parsed, std-only, no clap) while
//! leaving the default no-argument behavior exactly as the local in-memory
//! REPL above:
//!
//! * `serve [--addr 127.0.0.1:PORT] [--data DIR]` opens a durable
//!   [`Db`](distributedb::Db) via `RealFs` at `--data` (default under the OS
//!   temp dir, which `.gitignore` keeps out of the repo) and runs the Phase 3
//!   TCP [`Server`](distributedb::Server), bound to loopback, until Ctrl-C /
//!   EOF on stdin.
//! * `client --addr HOST:PORT` runs a REPL that parses the same text syntax
//!   with [`distributedb::parse`], sends each command over the binary protocol
//!   with a [`Client`](distributedb::Client), and prints responses in the same
//!   human-readable format as the local REPL.
//!
//! With no subcommand the original local in-memory stdin REPL runs unchanged.

use std::io::{self, BufRead, Write};
use std::sync::{Arc, RwLock};

use distributedb::{
    parse, Client, Command, GetResult, NodeRole, OpenConfig, PrimaryListener, ReplicaRunner,
    ReplicationStats, Server, ServerConfig, Status, StorageEngine,
};

fn main() {
    let args: Vec<String> = std::env::args().collect();
    match args.get(1).map(String::as_str) {
        Some("serve") => {
            if let Err(err) = serve_command(&args[2..]) {
                eprintln!("serve error: {err}");
                std::process::exit(1);
            }
        }
        Some("client") => {
            if let Err(err) = client_command(&args[2..]) {
                eprintln!("client error: {err}");
                std::process::exit(1);
            }
        }
        Some("replica") => {
            if let Err(err) = replica_command(&args[2..]) {
                eprintln!("replica error: {err}");
                std::process::exit(1);
            }
        }
        // No subcommand (or an unknown first token): preserve the original
        // local in-memory stdin REPL exactly.
        _ => {
            let stdin = io::stdin();
            let stdout = io::stdout();
            let stderr = io::stderr();
            run(&mut stdin.lock(), &mut stdout.lock(), &mut stderr.lock());
        }
    }
}

/// Parse a `--flag value` option out of `args`, returning its value if present.
fn flag_value(args: &[String], flag: &str) -> Option<String> {
    let mut it = args.iter();
    while let Some(arg) = it.next() {
        if arg == flag {
            return it.next().cloned();
        }
    }
    None
}

/// Run the `serve` subcommand: open a durable `Db` and start the TCP server.
fn serve_command(args: &[String]) -> Result<(), Box<dyn std::error::Error>> {
    use distributedb::{Db, DurabilityMode, RealFs};

    let addr = flag_value(args, "--addr").unwrap_or_else(|| "127.0.0.1:5555".to_string());
    let replication_addr =
        flag_value(args, "--replication-addr").unwrap_or_else(|| "127.0.0.1:5556".to_string());
    let durability = match flag_value(args, "--durability").as_deref() {
        None | Some("fsync") => DurabilityMode::Fsync,
        Some("os") => DurabilityMode::Os,
        Some(_) => return Err("--durability must be fsync or os".into()),
    };
    let data_dir = flag_value(args, "--data").unwrap_or_else(|| {
        // Default under the OS temp dir so runtime data never lands in the repo
        // (.gitignore also excludes /data/, /run/, /tmp/, /target/).
        std::env::temp_dir()
            .join("distributedb-data")
            .to_string_lossy()
            .into_owned()
    });

    let db = Db::open(RealFs, std::path::Path::new(&data_dir), durability)?;
    let cluster_id = distributedb::replication::format_id(&db.identity().cluster_id);
    let shared = Arc::new(RwLock::new(db));
    let stats = Arc::new(ReplicationStats::default());
    let mut replication = if durability == DurabilityMode::Fsync {
        Some(PrimaryListener::start(
            replication_addr.as_str(),
            Arc::clone(&shared),
            Arc::clone(&stats),
        )?)
    } else {
        None
    };
    let config = ServerConfig {
        replication_stats: replication.as_ref().map(|_| stats),
        ..ServerConfig::default()
    };
    let server = Server::start_shared(addr.as_str(), shared, config)?;
    println!("DistributeDB listening on {}", server.local_addr());
    if let Some(ref listener) = replication {
        println!("replication listening on {}", listener.local_addr());
    }
    println!("durability: {}", if durability == DurabilityMode::Fsync { "fsync" } else { "os" });
    println!("cluster ID: {cluster_id}");
    println!("data directory: {data_dir}");
    println!("press Ctrl-D (EOF) on stdin to shut down");

    // Block until stdin closes (EOF) or a line is read, then shut down cleanly.
    let handle = server.shutdown_handle();
    let stdin = io::stdin();
    let mut line = String::new();
    loop {
        line.clear();
        match stdin.lock().read_line(&mut line) {
            Ok(0) => break, // EOF: shut down.
            Ok(_) if line.trim() == "shutdown" => break,
            Ok(_) => {}
            Err(_) => break,
        }
    }
    handle.shutdown();
    drop(server); // joins acceptor + sequencer threads.
    if let Some(ref mut listener) = replication {
        listener.shutdown();
    }
    println!("shut down");
    Ok(())
}

fn parse_cluster_id(hex: &str) -> Result<[u8; 16], Box<dyn std::error::Error>> {
    if hex.len() != 32 || !hex.is_ascii() {
        return Err("cluster ID must be exactly 32 hexadecimal characters".into());
    }
    let mut id = [0u8; 16];
    for (index, byte) in id.iter_mut().enumerate() {
        *byte = u8::from_str_radix(&hex[index * 2..index * 2 + 2], 16)?;
    }
    Ok(id)
}

/// Run a statically configured read-only replica. A separate data directory
/// and the primary's printed cluster ID are required on first start.
fn replica_command(args: &[String]) -> Result<(), Box<dyn std::error::Error>> {
    use distributedb::{Db, DurabilityMode, RealFs};

    let primary =
        flag_value(args, "--primary-addr").ok_or("replica requires --primary-addr HOST:PORT")?;
    let primary: std::net::SocketAddr = primary.parse()?;
    let data_dir = flag_value(args, "--data").ok_or("replica requires --data DIRECTORY")?;
    let cluster_hex =
        flag_value(args, "--cluster-id").ok_or("replica requires --cluster-id HEX")?;
    let cluster_id = parse_cluster_id(&cluster_hex)?;
    let provision_rebootstrap = args.iter().any(|arg| arg == "--allow-snapshot-rebootstrap");
    let db = Db::open_configured(
        RealFs,
        std::path::Path::new(&data_dir),
        DurabilityMode::Fsync,
        OpenConfig {
            role: NodeRole::Replica,
            cluster_id: Some(cluster_id),
            allow_snapshot_rebootstrap: provision_rebootstrap,
            ..OpenConfig::default()
        },
    )?;
    if provision_rebootstrap && !db.allows_snapshot_rebootstrap() {
        return Err("existing replica was provisioned without snapshot rebootstrap".into());
    }
    let shared = Arc::new(RwLock::new(db));
    let mut runner = ReplicaRunner::start(primary, shared)?;
    println!("replica connecting to {primary}");
    println!("data directory: {data_dir}");
    println!("press Ctrl-D (EOF) or enter shutdown to stop");
    let (input_tx, input_rx) = std::sync::mpsc::channel();
    std::thread::spawn(move || {
        let stdin = io::stdin();
        let mut line = String::new();
        loop {
            line.clear();
            let stop = match stdin.lock().read_line(&mut line) {
                Ok(0) | Err(_) => true,
                Ok(_) => line.trim() == "shutdown",
            };
            if stop {
                let _ = input_tx.send(());
                break;
            }
        }
    });
    while input_rx
        .recv_timeout(std::time::Duration::from_millis(100))
        .is_err()
    {
        if let Some(error) = runner.fatal_error() {
            runner.shutdown();
            return Err(error.into());
        }
    }
    runner.shutdown();
    if let Some(error) = runner.fatal_error() {
        return Err(error.into());
    }
    Ok(())
}

/// Run the `client` subcommand: a REPL over a TCP [`Client`].
fn client_command(args: &[String]) -> Result<(), Box<dyn std::error::Error>> {
    let addr = flag_value(args, "--addr").ok_or("client requires --addr HOST:PORT")?;
    let mut client = Client::connect(addr.as_str())?;

    let stdin = io::stdin();
    let stdout = io::stdout();
    let mut out = stdout.lock();
    let mut line = String::new();
    loop {
        line.clear();
        match stdin.lock().read_line(&mut line) {
            Ok(0) => break, // EOF.
            Ok(_) => {
                let rendered = match parse(&line) {
                    Ok(command) => client_execute(&mut client, command),
                    Err(err) => Some(format!("BAD_REQUEST: {err}")),
                };
                if let Some(text) = rendered {
                    let _ = writeln!(out, "{text}");
                    let _ = out.flush();
                }
            }
            Err(err) => {
                eprintln!("read error: {err}");
                break;
            }
        }
    }
    Ok(())
}

/// Execute one parsed command over the network client and render the response
/// in the same human-readable form as the local REPL. Returns `None` when
/// nothing should be printed (never, currently) so the caller stays uniform.
fn client_execute(client: &mut Client, command: Command) -> Option<String> {
    let rendered = match command {
        Command::Set { key, value } => match client.set(key, value) {
            Ok(Status::Ok) | Ok(Status::OkVolatile) => OK.to_string(),
            Ok(status) => format!("ERROR: {status:?}"),
            Err(err) => format!("ERROR: {err}"),
        },
        Command::Delete { key } => match client.delete(key) {
            Ok(Status::Ok) | Ok(Status::OkVolatile) => OK.to_string(),
            Ok(status) => format!("ERROR: {status:?}"),
            Err(err) => format!("ERROR: {err}"),
        },
        Command::Get { key } => match client.get(key) {
            Ok(Some(value)) => String::from_utf8_lossy(&value).into_owned(),
            Ok(None) => NOT_FOUND.to_string(),
            Err(err) => format!("ERROR: {err}"),
        },
        Command::Exists { key } => match client.exists(key) {
            Ok(true) => TRUE.to_string(),
            Ok(false) => FALSE.to_string(),
            Err(err) => format!("ERROR: {err}"),
        },
    };
    Some(rendered)
}

/// Run the REPL loop over generic reader/writer handles.
///
/// Reads lines from `input`, parses and executes each against a single
/// in-memory [`StorageEngine`], writing successful responses to `output` and
/// parse-error (`BAD_REQUEST`) messages to `errors`. Returns on EOF. Kept
/// generic over the I/O handles so it stays thin and so `main` can pass the
/// locked standard streams; the per-command logic lives in [`execute`], which
/// is what the unit tests target.
fn run<R, W, E>(input: &mut R, output: &mut W, errors: &mut E)
where
    R: BufRead,
    W: Write,
    E: Write,
{
    let mut engine = StorageEngine::new();
    let mut line = String::new();

    loop {
        line.clear();
        match input.read_line(&mut line) {
            Ok(0) => break, // EOF: exit cleanly.
            Ok(_) => match parse(&line) {
                Ok(command) => {
                    let response = execute(&mut engine, command);
                    // `writeln!` appends the newline; an empty response (an
                    // empty stored value) renders as a blank line.
                    let _ = writeln!(output, "{response}");
                    let _ = output.flush();
                }
                Err(err) => {
                    // Malformed input must not crash the loop (SOW §4 / R1).
                    let _ = writeln!(errors, "BAD_REQUEST: {err}");
                    let _ = errors.flush();
                }
            },
            Err(err) => {
                let _ = writeln!(errors, "BAD_REQUEST: read error: {err}");
                let _ = errors.flush();
                break;
            }
        }
    }
}

/// Execute a single parsed [`Command`] against `engine` and render the
/// human-readable response as a [`String`] (without a trailing newline).
///
/// This is the testable dispatch layer, kept separate from stdin I/O:
///
/// * `Set` / `Delete` return `OK` (a `DELETE` of a missing key still returns
///   `OK`, per Technical-Design §2.1).
/// * `Get` returns the stored value as UTF-8 text, or [`NOT_FOUND`] when the
///   key is absent. An empty stored value renders as an empty string, which is
///   distinct from `NOT_FOUND`.
/// * `Exists` returns `true` or `false`.
///
/// Stored values are byte strings; for display they are decoded as UTF-8 with
/// invalid sequences replaced (the whitespace CLI only produces UTF-8 input).
fn execute(engine: &mut StorageEngine, command: Command) -> String {
    match command {
        Command::Set { key, value } => {
            engine.set(key, value);
            OK.to_string()
        }
        Command::Delete { key } => {
            engine.delete(key);
            OK.to_string()
        }
        Command::Get { key } => match engine.get(&key) {
            GetResult::Found(value) => String::from_utf8_lossy(&value).into_owned(),
            GetResult::NotFound => NOT_FOUND.to_string(),
        },
        Command::Exists { key } => {
            if engine.exists(&key) {
                TRUE.to_string()
            } else {
                FALSE.to_string()
            }
        }
    }
}

/// Response printed for a successful `SET` or `DELETE`.
const OK: &str = "OK";
/// Response printed for a `GET` on a key that does not exist.
///
/// Distinct from an empty stored value, which renders as an empty string.
const NOT_FOUND: &str = "NOT_FOUND";
/// Response printed for `EXISTS` when the key is present.
const TRUE: &str = "true";
/// Response printed for `EXISTS` when the key is absent.
const FALSE: &str = "false";

#[cfg(test)]
mod tests {
    use super::*;

    fn set(engine: &mut StorageEngine, key: &str, value: &str) -> String {
        execute(
            engine,
            Command::Set {
                key: key.as_bytes().to_vec(),
                value: value.as_bytes().to_vec(),
            },
        )
    }

    fn get(engine: &mut StorageEngine, key: &str) -> String {
        execute(
            engine,
            Command::Get {
                key: key.as_bytes().to_vec(),
            },
        )
    }

    fn delete(engine: &mut StorageEngine, key: &str) -> String {
        execute(
            engine,
            Command::Delete {
                key: key.as_bytes().to_vec(),
            },
        )
    }

    fn exists(engine: &mut StorageEngine, key: &str) -> String {
        execute(
            engine,
            Command::Exists {
                key: key.as_bytes().to_vec(),
            },
        )
    }

    #[test]
    fn set_then_get_returns_stored_value() {
        let mut engine = StorageEngine::new();
        assert_eq!(set(&mut engine, "k", "v"), "OK");
        assert_eq!(get(&mut engine, "k"), "v");
    }

    #[test]
    fn sow_worked_example() {
        // SOW §4: `SET user:123 Aaron` then `GET user:123` -> `Aaron`.
        let mut engine = StorageEngine::new();
        assert_eq!(set(&mut engine, "user:123", "Aaron"), "OK");
        assert_eq!(get(&mut engine, "user:123"), "Aaron");
    }

    #[test]
    fn get_missing_key_renders_not_found() {
        let mut engine = StorageEngine::new();
        assert_eq!(get(&mut engine, "absent"), "NOT_FOUND");
    }

    #[test]
    fn empty_value_renders_as_empty_string_not_not_found() {
        let mut engine = StorageEngine::new();
        assert_eq!(set(&mut engine, "k", ""), "OK");
        let rendered = get(&mut engine, "k");
        assert_eq!(rendered, "");
        assert_ne!(rendered, "NOT_FOUND");
    }

    #[test]
    fn delete_present_then_get_renders_not_found() {
        let mut engine = StorageEngine::new();
        set(&mut engine, "k", "v");
        assert_eq!(delete(&mut engine, "k"), "OK");
        assert_eq!(get(&mut engine, "k"), "NOT_FOUND");
    }

    #[test]
    fn delete_missing_key_renders_ok() {
        let mut engine = StorageEngine::new();
        assert_eq!(delete(&mut engine, "absent"), "OK");
    }

    #[test]
    fn exists_true_and_false_rendering() {
        let mut engine = StorageEngine::new();
        assert_eq!(exists(&mut engine, "k"), "false");
        set(&mut engine, "k", "v");
        assert_eq!(exists(&mut engine, "k"), "true");
    }

    #[test]
    fn run_loop_processes_lines_and_exits_on_eof() {
        let mut engine_input = io::Cursor::new(b"SET user:123 Aaron\nGET user:123\n".to_vec());
        let mut output: Vec<u8> = Vec::new();
        let mut errors: Vec<u8> = Vec::new();
        run(&mut engine_input, &mut output, &mut errors);
        assert_eq!(String::from_utf8(output).unwrap(), "OK\nAaron\n");
        assert!(errors.is_empty());
    }

    #[test]
    fn run_loop_reports_parse_error_to_stderr_and_continues() {
        let mut input = io::Cursor::new(b"FROB x\nGET missing\n".to_vec());
        let mut output: Vec<u8> = Vec::new();
        let mut errors: Vec<u8> = Vec::new();
        run(&mut input, &mut output, &mut errors);
        // The bad line goes to stderr; the loop continues to the next command.
        assert_eq!(String::from_utf8(output).unwrap(), "NOT_FOUND\n");
        let err_text = String::from_utf8(errors).unwrap();
        assert!(err_text.starts_with("BAD_REQUEST:"), "got: {err_text}");
    }
}

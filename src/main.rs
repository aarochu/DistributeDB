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
//! The REPL exits cleanly on end-of-input (EOF). This is still local and
//! in-memory only; there is no networking yet (that is Phase 3).

use std::io::{self, BufRead, Write};

use distributedb::{parse, Command, GetResult, StorageEngine};

fn main() {
    let stdin = io::stdin();
    let stdout = io::stdout();
    let stderr = io::stderr();
    run(&mut stdin.lock(), &mut stdout.lock(), &mut stderr.lock());
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

//! Command module.
//!
//! Defines the key-value command types and the text parser for the command
//! syntax from SOW §4.1: `SET key value`, `GET key`, `DELETE key`,
//! `EXISTS key`.
//!
//! The parser enforces the Technical-Design §4.1 limits: the key is
//! `1..=4096` bytes, and a `SET` is accepted only when
//! `33 + key_len + value_len <= 1_000_000`. The command word is
//! case-insensitive. For `SET`, the value is the entire remainder of the line
//! after the key (so values may contain spaces) and is preserved verbatim.
//! CLI text is UTF-8 and is converted to bytes for the byte-string engine.
//!
//! The whitespace CLI syntax is for demonstrations only (Technical-Design
//! §4.1); arbitrary bytes travel over the binary protocol in later phases.

use std::fmt;

/// Maximum key length in bytes (Technical-Design §4.1).
pub const MAX_KEY_LEN: usize = 4096;
/// Minimum key length in bytes (Technical-Design §4.1).
pub const MIN_KEY_LEN: usize = 1;
/// Fixed encoded overhead used by the SET size bound (Technical-Design §4.1).
pub const SET_FIXED_OVERHEAD: usize = 33;
/// Maximum encoded mutation size in bytes (Technical-Design §4.1).
pub const MAX_MUTATION_ENCODED_LEN: usize = 1_000_000;

/// A parsed key-value command (SOW §4.1).
#[derive(Debug, Clone, PartialEq, Eq)]
pub enum Command {
    /// `SET key value` — replace the entire value for `key`.
    Set { key: Vec<u8>, value: Vec<u8> },
    /// `GET key` — look up `key`.
    Get { key: Vec<u8> },
    /// `DELETE key` — remove `key`.
    Delete { key: Vec<u8> },
    /// `EXISTS key` — test whether `key` exists.
    Exists { key: Vec<u8> },
}

/// Error returned when a command line cannot be parsed or violates a limit.
#[derive(Debug, Clone, PartialEq, Eq)]
pub enum ParseError {
    /// The input line was empty or contained only whitespace.
    Empty,
    /// The command word was not one of SET/GET/DELETE/EXISTS.
    UnknownCommand(String),
    /// The command received the wrong number of arguments.
    ///
    /// Reports the command name, how many arguments were expected, and how
    /// many were supplied.
    WrongArgCount {
        command: &'static str,
        expected: &'static str,
        got: usize,
    },
    /// The key length was outside `MIN_KEY_LEN..=MAX_KEY_LEN` bytes.
    InvalidKeyLength(usize),
    /// A `SET` exceeded the encoded mutation size bound.
    MutationTooLarge {
        /// The encoded size that would have been required.
        encoded_len: usize,
        /// The maximum permitted encoded size.
        max: usize,
    },
}

impl fmt::Display for ParseError {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self {
            ParseError::Empty => write!(f, "empty input"),
            ParseError::UnknownCommand(word) => write!(f, "unknown command: {word}"),
            ParseError::WrongArgCount {
                command,
                expected,
                got,
            } => write!(f, "{command} expects {expected} argument(s), got {got}"),
            ParseError::InvalidKeyLength(len) => write!(
                f,
                "invalid key length {len} bytes (must be {MIN_KEY_LEN}..={MAX_KEY_LEN})"
            ),
            ParseError::MutationTooLarge { encoded_len, max } => write!(
                f,
                "mutation too large: encoded {encoded_len} bytes exceeds limit {max}"
            ),
        }
    }
}

impl std::error::Error for ParseError {}

/// Validate that `key` satisfies the Technical-Design §4.1 length bounds.
fn validate_key_len(key: &[u8]) -> Result<(), ParseError> {
    let len = key.len();
    if !(MIN_KEY_LEN..=MAX_KEY_LEN).contains(&len) {
        return Err(ParseError::InvalidKeyLength(len));
    }
    Ok(())
}

/// Parse a single command line into a [`Command`] (SOW §4.1).
///
/// The command word is case-insensitive. For `SET`, the value is the entire
/// remainder of the line after the first space following the key and is
/// preserved verbatim (leading spaces after that separator become part of the
/// value; embedded spaces are kept). `GET`, `DELETE`, and `EXISTS` take
/// exactly one key argument and reject too few or too many.
///
/// Enforces the Technical-Design §4.1 limits: key length `1..=4096` bytes and,
/// for `SET`, `33 + key_len + value_len <= 1_000_000`.
pub fn parse(line: &str) -> Result<Command, ParseError> {
    let trimmed = line.trim();
    if trimmed.is_empty() {
        return Err(ParseError::Empty);
    }

    // Split off the command word; `rest` is everything after the first run of
    // whitespace following the command word.
    let (word, rest) = match trimmed.split_once(char::is_whitespace) {
        Some((word, rest)) => (word, rest.trim_start()),
        None => (trimmed, ""),
    };
    let upper = word.to_ascii_uppercase();

    match upper.as_str() {
        "SET" => parse_set(rest),
        "GET" => parse_single_key("GET", rest).map(|key| Command::Get { key }),
        "DELETE" => parse_single_key("DELETE", rest).map(|key| Command::Delete { key }),
        "EXISTS" => parse_single_key("EXISTS", rest).map(|key| Command::Exists { key }),
        _ => Err(ParseError::UnknownCommand(word.to_string())),
    }
}

/// Parse the arguments of a `SET`: `key` then rest-of-line as the value.
fn parse_set(rest: &str) -> Result<Command, ParseError> {
    if rest.is_empty() {
        // No key and no value at all.
        return Err(ParseError::WrongArgCount {
            command: "SET",
            expected: "2 (key value)",
            got: 0,
        });
    }

    // The value is the entire remainder after the first whitespace run that
    // follows the key, preserved verbatim (spaces allowed inside the value).
    let (key, value) = match rest.split_once(char::is_whitespace) {
        Some((key, value)) => (key, value),
        None => {
            // A key but no value separator.
            return Err(ParseError::WrongArgCount {
                command: "SET",
                expected: "2 (key value)",
                got: 1,
            });
        }
    };

    let key_bytes = key.as_bytes().to_vec();
    validate_key_len(&key_bytes)?;

    let value_bytes = value.as_bytes().to_vec();
    let encoded_len = SET_FIXED_OVERHEAD + key_bytes.len() + value_bytes.len();
    if encoded_len > MAX_MUTATION_ENCODED_LEN {
        return Err(ParseError::MutationTooLarge {
            encoded_len,
            max: MAX_MUTATION_ENCODED_LEN,
        });
    }

    Ok(Command::Set {
        key: key_bytes,
        value: value_bytes,
    })
}

/// Parse the single key argument of GET/DELETE/EXISTS.
fn parse_single_key(command: &'static str, rest: &str) -> Result<Vec<u8>, ParseError> {
    if rest.is_empty() {
        return Err(ParseError::WrongArgCount {
            command,
            expected: "1 (key)",
            got: 0,
        });
    }

    // Reject extra arguments: any whitespace inside `rest` means >1 arg.
    let mut fields = rest.split_whitespace();
    let key = fields.next().expect("rest is non-empty");
    let extra = fields.count();
    if extra > 0 {
        return Err(ParseError::WrongArgCount {
            command,
            expected: "1 (key)",
            got: 1 + extra,
        });
    }

    let key_bytes = key.as_bytes().to_vec();
    validate_key_len(&key_bytes)?;
    Ok(key_bytes)
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn parse_set_happy_path_sow_example() {
        // SOW §4.1 example: `SET user:123 Aaron`.
        assert_eq!(
            parse("SET user:123 Aaron").unwrap(),
            Command::Set {
                key: b"user:123".to_vec(),
                value: b"Aaron".to_vec(),
            }
        );
    }

    #[test]
    fn parse_get_happy_path_sow_example() {
        // SOW §4.1 example: `GET user:123`.
        assert_eq!(
            parse("GET user:123").unwrap(),
            Command::Get {
                key: b"user:123".to_vec()
            }
        );
    }

    #[test]
    fn parse_delete_and_exists_happy_paths() {
        assert_eq!(
            parse("DELETE user:123").unwrap(),
            Command::Delete {
                key: b"user:123".to_vec()
            }
        );
        assert_eq!(
            parse("EXISTS user:123").unwrap(),
            Command::Exists {
                key: b"user:123".to_vec()
            }
        );
    }

    #[test]
    fn command_word_is_case_insensitive() {
        assert_eq!(
            parse("set k v").unwrap(),
            Command::Set {
                key: b"k".to_vec(),
                value: b"v".to_vec()
            }
        );
        assert_eq!(parse("GeT k").unwrap(), Command::Get { key: b"k".to_vec() });
        assert_eq!(
            parse("dElEtE k").unwrap(),
            Command::Delete { key: b"k".to_vec() }
        );
        assert_eq!(
            parse("EXISTS k").unwrap(),
            Command::Exists { key: b"k".to_vec() }
        );
    }

    #[test]
    fn set_value_with_spaces_is_preserved_verbatim() {
        assert_eq!(
            parse("SET greeting hello   world  ").unwrap(),
            Command::Set {
                key: b"greeting".to_vec(),
                // Leading/trailing internal spacing after the key separator is
                // part of the value verbatim; only the outer line is trimmed.
                value: b"hello   world".to_vec(),
            }
        );
    }

    #[test]
    fn empty_line_is_error() {
        assert_eq!(parse(""), Err(ParseError::Empty));
        assert_eq!(parse("    "), Err(ParseError::Empty));
    }

    #[test]
    fn unknown_command_is_error() {
        match parse("FROB k v") {
            Err(ParseError::UnknownCommand(word)) => assert_eq!(word, "FROB"),
            other => panic!("expected UnknownCommand, got {other:?}"),
        }
    }

    #[test]
    fn single_key_commands_reject_missing_key() {
        for cmd in ["GET", "DELETE", "EXISTS"] {
            match parse(cmd) {
                Err(ParseError::WrongArgCount { got, .. }) => assert_eq!(got, 0),
                other => panic!("expected WrongArgCount for {cmd}, got {other:?}"),
            }
        }
    }

    #[test]
    fn single_key_commands_reject_extra_args() {
        for cmd in ["GET", "DELETE", "EXISTS"] {
            let line = format!("{cmd} k extra");
            match parse(&line) {
                Err(ParseError::WrongArgCount { got, .. }) => assert_eq!(got, 2),
                other => panic!("expected WrongArgCount for {line:?}, got {other:?}"),
            }
        }
    }

    #[test]
    fn set_with_no_value_is_error() {
        match parse("SET keyonly") {
            Err(ParseError::WrongArgCount {
                command: "SET",
                got: 1,
                ..
            }) => {}
            other => panic!("expected WrongArgCount(SET, got=1), got {other:?}"),
        }
    }

    #[test]
    fn set_with_no_args_is_error() {
        match parse("SET") {
            Err(ParseError::WrongArgCount {
                command: "SET",
                got: 0,
                ..
            }) => {}
            other => panic!("expected WrongArgCount(SET, got=0), got {other:?}"),
        }
    }

    #[test]
    fn key_of_length_zero_is_error() {
        // A zero-length key cannot occur via the whitespace CLI for GET-style
        // commands, but validate the bound directly through SET is not
        // possible either; assert the validator rejects it.
        assert_eq!(validate_key_len(b""), Err(ParseError::InvalidKeyLength(0)));
    }

    #[test]
    fn key_exceeding_max_len_is_error() {
        let key = "k".repeat(MAX_KEY_LEN + 1);
        let line = format!("GET {key}");
        match parse(&line) {
            Err(ParseError::InvalidKeyLength(len)) => assert_eq!(len, MAX_KEY_LEN + 1),
            other => panic!("expected InvalidKeyLength, got {other:?}"),
        }

        // Also via SET.
        let set_line = format!("SET {key} v");
        match parse(&set_line) {
            Err(ParseError::InvalidKeyLength(len)) => assert_eq!(len, MAX_KEY_LEN + 1),
            other => panic!("expected InvalidKeyLength for SET, got {other:?}"),
        }
    }

    #[test]
    fn max_len_key_is_accepted() {
        let key = "k".repeat(MAX_KEY_LEN);
        let line = format!("GET {key}");
        assert_eq!(
            parse(&line).unwrap(),
            Command::Get {
                key: key.into_bytes()
            }
        );
    }

    #[test]
    fn set_size_bound_accept_just_under_reject_just_over() {
        // Choose a small key so the value dominates the bound.
        let key = "k"; // 1 byte
        let overhead = SET_FIXED_OVERHEAD + key.len();
        // Largest accepted value length.
        let max_value_len = MAX_MUTATION_ENCODED_LEN - overhead;

        // Just under / at the boundary: accepted.
        let value_ok = "v".repeat(max_value_len);
        let line_ok = format!("SET {key} {value_ok}");
        match parse(&line_ok) {
            Ok(Command::Set { key: k, value }) => {
                assert_eq!(k, b"k");
                assert_eq!(value.len(), max_value_len);
                assert_eq!(overhead + value.len(), MAX_MUTATION_ENCODED_LEN);
            }
            other => panic!("expected accepted SET at boundary, got {other:?}"),
        }

        // Just over: rejected.
        let value_over = "v".repeat(max_value_len + 1);
        let line_over = format!("SET {key} {value_over}");
        match parse(&line_over) {
            Err(ParseError::MutationTooLarge { encoded_len, max }) => {
                assert_eq!(max, MAX_MUTATION_ENCODED_LEN);
                assert_eq!(encoded_len, MAX_MUTATION_ENCODED_LEN + 1);
            }
            other => panic!("expected MutationTooLarge just over boundary, got {other:?}"),
        }
    }
}

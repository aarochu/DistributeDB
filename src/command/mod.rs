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
//! The text syntax is for demonstrations. `parse_hex` accepts hex-encoded
//! byte arguments so the CLI can send arbitrary keys and values.

use std::fmt;

/// Maximum key length in bytes (Technical-Design §4.1).
pub const MAX_KEY_LEN: usize = 4096;
/// Minimum key length in bytes (Technical-Design §4.1).
pub const MIN_KEY_LEN: usize = 1;
/// Fixed encoded overhead used by the SET size bound (Technical-Design §4.1).
pub const SET_FIXED_OVERHEAD: usize = 33;
/// Maximum encoded mutation size in bytes (Technical-Design §4.1).
pub const MAX_MUTATION_ENCODED_LEN: usize = 1_000_000;
/// Largest number of pairs one `SCAN` may request.
pub const MAX_SCAN_LIMIT: usize = 10_000;

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
    /// `SCAN start end limit` — up to `limit` pairs with `start <= key < end`
    /// in key order. An empty `start` begins at the first key; `end: None` is
    /// unbounded.
    Scan {
        start: Vec<u8>,
        end: Option<Vec<u8>>,
        limit: usize,
    },
    /// `PING` — liveness check.
    Ping,
    /// `STATS` — operational statistics.
    Stats,
}

/// Error returned when a command line cannot be parsed or violates a limit.
#[derive(Debug, Clone, PartialEq, Eq)]
pub enum ParseError {
    /// The input line was empty or contained only whitespace.
    Empty,
    /// The command word was not a known command.
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
    /// A `SCAN` limit was not an integer in `1..=MAX_SCAN_LIMIT`.
    InvalidLimit(String),
    /// A hex-mode argument had an odd length or a non-hexadecimal digit.
    InvalidHex(&'static str),
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
            ParseError::InvalidLimit(limit) => write!(
                f,
                "invalid SCAN limit {limit} (must be 1..={MAX_SCAN_LIMIT})"
            ),
            ParseError::InvalidHex(field) => write!(f, "invalid hexadecimal {field}"),
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
        "SCAN" => parse_scan(rest),
        "PING" => parse_no_args("PING", rest).map(|()| Command::Ping),
        "STATS" => parse_no_args("STATS", rest).map(|()| Command::Stats),
        _ => Err(ParseError::UnknownCommand(word.to_string())),
    }
}

/// Reject arguments to a command that takes none.
fn parse_no_args(command: &'static str, rest: &str) -> Result<(), ParseError> {
    let got = rest.split_whitespace().count();
    if got != 0 {
        return Err(ParseError::WrongArgCount {
            command,
            expected: "0",
            got,
        });
    }
    Ok(())
}

/// Parse a CLI line whose byte arguments are hexadecimal. In this mode a
/// single `-` denotes an empty SET value; SCAN uses `*` for an open bound.
/// The command word and argument counts match the text parser.
pub fn parse_hex(line: &str) -> Result<Command, ParseError> {
    let mut fields = line.split_whitespace();
    let word = fields.next().ok_or(ParseError::Empty)?;
    let command = word.to_ascii_uppercase();
    let args: Vec<&str> = fields.collect();
    let required = match command.as_str() {
        "SET" => (2, "2 (key value)"),
        "GET" | "DELETE" | "EXISTS" => (1, "1 (key)"),
        "SCAN" => (3, "3 (start end limit)"),
        "PING" | "STATS" => (0, "0"),
        _ => return Err(ParseError::UnknownCommand(word.to_string())),
    };
    if args.len() != required.0 {
        return Err(ParseError::WrongArgCount {
            command: match command.as_str() {
                "SET" => "SET",
                "GET" => "GET",
                "DELETE" => "DELETE",
                "EXISTS" => "EXISTS",
                "PING" => "PING",
                "STATS" => "STATS",
                _ => "SCAN",
            },
            expected: required.1,
            got: args.len(),
        });
    }

    let key = || -> Result<Vec<u8>, ParseError> {
        let key = decode_hex(args[0], "key")?;
        validate_key_len(&key)?;
        Ok(key)
    };
    match command.as_str() {
        "SET" => {
            let key = key()?;
            let value = if args[1] == "-" {
                Vec::new()
            } else {
                decode_hex(args[1], "value")?
            };
            let encoded_len = SET_FIXED_OVERHEAD
                .checked_add(key.len())
                .and_then(|n| n.checked_add(value.len()))
                .unwrap_or(usize::MAX);
            if encoded_len > MAX_MUTATION_ENCODED_LEN {
                return Err(ParseError::MutationTooLarge {
                    encoded_len,
                    max: MAX_MUTATION_ENCODED_LEN,
                });
            }
            Ok(Command::Set { key, value })
        }
        "GET" => Ok(Command::Get { key: key()? }),
        "DELETE" => Ok(Command::Delete { key: key()? }),
        "EXISTS" => Ok(Command::Exists { key: key()? }),
        "SCAN" => {
            let bound = |field: &str| -> Result<Option<Vec<u8>>, ParseError> {
                if field == "*" {
                    return Ok(None);
                }
                let bytes = decode_hex(field, "SCAN bound")?;
                validate_key_len(&bytes)?;
                Ok(Some(bytes))
            };
            let limit = args[2]
                .parse::<usize>()
                .ok()
                .filter(|limit| (1..=MAX_SCAN_LIMIT).contains(limit))
                .ok_or_else(|| ParseError::InvalidLimit(args[2].to_string()))?;
            Ok(Command::Scan {
                start: bound(args[0])?.unwrap_or_default(),
                end: bound(args[1])?,
                limit,
            })
        }
        "PING" => Ok(Command::Ping),
        "STATS" => Ok(Command::Stats),
        _ => unreachable!(),
    }
}

fn decode_hex(text: &str, field: &'static str) -> Result<Vec<u8>, ParseError> {
    let bytes = text.as_bytes();
    if !bytes.len().is_multiple_of(2) {
        return Err(ParseError::InvalidHex(field));
    }
    let digit = |byte: u8| match byte {
        b'0'..=b'9' => Some(byte - b'0'),
        b'a'..=b'f' => Some(byte - b'a' + 10),
        b'A'..=b'F' => Some(byte - b'A' + 10),
        _ => None,
    };
    let (pairs, remainder) = bytes.as_chunks::<2>();
    debug_assert!(remainder.is_empty());
    pairs
        .iter()
        .map(|pair| {
            let high = digit(pair[0]).ok_or(ParseError::InvalidHex(field))?;
            let low = digit(pair[1]).ok_or(ParseError::InvalidHex(field))?;
            Ok(high << 4 | low)
        })
        .collect()
}

/// Parse `SCAN start end limit`. `*` as `start` begins at the first key and
/// `*` as `end` leaves the range unbounded, so `*` itself cannot be a bound.
fn parse_scan(rest: &str) -> Result<Command, ParseError> {
    let args: Vec<&str> = rest.split_whitespace().collect();
    let &[start, end, limit] = args.as_slice() else {
        return Err(ParseError::WrongArgCount {
            command: "SCAN",
            expected: "3 (start end limit)",
            got: args.len(),
        });
    };
    let bound = |text: &str| -> Result<Option<Vec<u8>>, ParseError> {
        if text == "*" {
            return Ok(None);
        }
        let key = text.as_bytes().to_vec();
        validate_key_len(&key)?;
        Ok(Some(key))
    };
    let limit = limit
        .parse::<usize>()
        .ok()
        .filter(|limit| (1..=MAX_SCAN_LIMIT).contains(limit))
        .ok_or_else(|| ParseError::InvalidLimit(limit.to_string()))?;
    Ok(Command::Scan {
        start: bound(start)?.unwrap_or_default(),
        end: bound(end)?,
        limit,
    })
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

    #[test]
    fn hex_mode_preserves_arbitrary_bytes_and_empty_value() {
        assert_eq!(
            parse_hex("SET 0020ff 0a00ff").unwrap(),
            Command::Set {
                key: vec![0, b' ', 255],
                value: vec![10, 0, 255],
            }
        );
        assert_eq!(
            parse_hex("set FF -").unwrap(),
            Command::Set {
                key: vec![255],
                value: Vec::new(),
            }
        );
        assert_eq!(
            parse_hex("GET 00ff").unwrap(),
            Command::Get { key: vec![0, 255] }
        );
        assert_eq!(
            parse_hex("DELETE 00ff").unwrap(),
            Command::Delete { key: vec![0, 255] }
        );
        assert_eq!(
            parse_hex("EXISTS 00ff").unwrap(),
            Command::Exists { key: vec![0, 255] }
        );
        assert_eq!(
            parse_hex("SCAN * ff 10").unwrap(),
            Command::Scan {
                start: Vec::new(),
                end: Some(vec![255]),
                limit: 10,
            }
        );
    }

    #[test]
    fn hex_mode_rejects_bad_arguments_and_enforces_size() {
        assert_eq!(parse_hex("GET f"), Err(ParseError::InvalidHex("key")));
        assert_eq!(parse_hex("SET 00 zz"), Err(ParseError::InvalidHex("value")));
        assert_eq!(parse_hex("GET -"), Err(ParseError::InvalidHex("key")));
        assert_eq!(
            parse_hex("SET 00"),
            Err(ParseError::WrongArgCount {
                command: "SET",
                expected: "2 (key value)",
                got: 1,
            })
        );
        let oversized_key = "ff".repeat(MAX_KEY_LEN + 1);
        assert_eq!(
            parse_hex(&format!("GET {oversized_key}")),
            Err(ParseError::InvalidKeyLength(MAX_KEY_LEN + 1))
        );
        let oversized_value = "00".repeat(MAX_MUTATION_ENCODED_LEN - SET_FIXED_OVERHEAD);
        assert!(matches!(
            parse_hex(&format!("SET 00 {oversized_value}")),
            Err(ParseError::MutationTooLarge { .. })
        ));
    }
}

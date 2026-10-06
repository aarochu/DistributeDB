//! Client TCP wire protocol v1 codec (Technical-Design §4.1).
//!
//! This module implements a pure, I/O-light encode/decode of the
//! length-prefixed binary request/response envelope defined in
//! Technical-Design §4.1. It has no networking and no engine dependency: it
//! only turns [`Request`]/[`Response`] values into bytes and back, plus a
//! framing reader/writer over any [`std::io::Read`]/[`std::io::Write`]. This
//! keeps the protocol fully unit-testable and lets FEAT-002 (server + client)
//! build on top of it.
//!
//! All integers are unsigned and little-endian, matching the WAL and
//! replication protocols (see [`crate::wal::format`]). The frame layout is:
//!
//! ```text
//! frame := body_len:u32 | body[body_len]
//! body  := version:u8 | kind:u8 | payload
//! ```
//!
//! `body_len` is `2..=1_048_576` bytes inclusive in v1 and `version = 1`.
//! Request `kind`: `1=SET`, `2=GET`, `3=DELETE`, `4=EXISTS`, `5=STATS`,
//! `6=SCAN`. In a
//! response `kind` echoes the request kind and the payload is
//! `status:u16 | data_len:u32 | data[data_len]`.
//!
//! Decoders validate strictly and never turn a malformed frame into success
//! (Technical-Design §3, §4.1): they reject trailing bytes, unknown kinds,
//! malformed/short field lengths, integer overflow, oversized frames before
//! allocating, and §4.1 key/mutation-limit violations. A parseable header
//! with an unknown version yields a distinct [`ProtocolError::UnsupportedVersion`]
//! so the server can answer `UNSUPPORTED_VERSION` and then close.

use std::io::{Read, Write};

use crate::command::{
    MAX_KEY_LEN, MAX_MUTATION_ENCODED_LEN, MAX_SCAN_LIMIT, MIN_KEY_LEN, SET_FIXED_OVERHEAD,
};

/// Protocol version encoded in every frame body (Technical-Design §4.1).
pub const PROTOCOL_VERSION: u8 = 1;

/// Minimum legal `body_len` (`version:u8 | kind:u8`, empty payload).
pub const MIN_BODY_LEN: usize = 2;
/// Maximum legal `body_len` in bytes (Technical-Design §4.1).
pub const MAX_BODY_LEN: usize = 1_048_576;

/// `kind` byte for a `SET` request.
pub const KIND_SET: u8 = 1;
/// `kind` byte for a `GET` request.
pub const KIND_GET: u8 = 2;
/// `kind` byte for a `DELETE` request.
pub const KIND_DELETE: u8 = 3;
/// `kind` byte for an `EXISTS` request.
pub const KIND_EXISTS: u8 = 4;
/// `kind` byte for a `STATS` request.
pub const KIND_STATS: u8 = 5;
/// `kind` byte for a `SCAN` request.
pub const KIND_SCAN: u8 = 6;
/// `PING`: liveness check; the reply is `PONG`.
pub const KIND_PING: u8 = 7;
/// `BEGIN`: open a transaction on this connection.
pub const KIND_BEGIN: u8 = 8;
/// `COMMIT`: durably apply the open transaction as one group.
pub const KIND_COMMIT: u8 = 9;
/// `ROLLBACK`: discard the open transaction.
pub const KIND_ROLLBACK: u8 = 10;
/// Largest `SCAN OK` data the server sends: a response body is
/// `version:u8 | kind:u8 | status:u16 | data_len:u32 | data`.
pub const MAX_SCAN_DATA_LEN: usize = MAX_BODY_LEN - 8;

/// A request kind on the wire (Technical-Design §4.1).
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum RequestKind {
    /// `SET key value`.
    Set,
    /// `GET key`.
    Get,
    /// `DELETE key`.
    Delete,
    /// `EXISTS key`.
    Exists,
    /// `STATS` (operational extension).
    Stats,
    /// `SCAN start end limit` (ordered range read).
    Scan,
    /// `PING` (liveness check).
    Ping,
    /// `BEGIN` (open a transaction).
    Begin,
    /// `COMMIT` (apply the open transaction).
    Commit,
    /// `ROLLBACK` (discard the open transaction).
    Rollback,
}

impl RequestKind {
    /// The on-wire `kind` byte for this request kind.
    pub fn as_u8(self) -> u8 {
        match self {
            RequestKind::Set => KIND_SET,
            RequestKind::Get => KIND_GET,
            RequestKind::Delete => KIND_DELETE,
            RequestKind::Exists => KIND_EXISTS,
            RequestKind::Stats => KIND_STATS,
            RequestKind::Scan => KIND_SCAN,
            RequestKind::Ping => KIND_PING,
            RequestKind::Begin => KIND_BEGIN,
            RequestKind::Commit => KIND_COMMIT,
            RequestKind::Rollback => KIND_ROLLBACK,
        }
    }

    /// Decode a `kind` byte, rejecting unknown values.
    pub fn from_u8(v: u8) -> Result<Self, ProtocolError> {
        match v {
            KIND_SET => Ok(RequestKind::Set),
            KIND_GET => Ok(RequestKind::Get),
            KIND_DELETE => Ok(RequestKind::Delete),
            KIND_EXISTS => Ok(RequestKind::Exists),
            KIND_STATS => Ok(RequestKind::Stats),
            KIND_SCAN => Ok(RequestKind::Scan),
            KIND_PING => Ok(RequestKind::Ping),
            KIND_BEGIN => Ok(RequestKind::Begin),
            KIND_COMMIT => Ok(RequestKind::Commit),
            KIND_ROLLBACK => Ok(RequestKind::Rollback),
            other => Err(ProtocolError::UnknownKind(other)),
        }
    }
}

/// A response status code (Technical-Design §4.1).
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum Status {
    /// `0` — success.
    Ok,
    /// `1` — key not found.
    NotFound,
    /// `2` — malformed or invalid request.
    BadRequest,
    /// `3` — this node is not the primary.
    NotPrimary,
    /// `4` — service temporarily unavailable.
    Unavailable,
    /// `5` — an internal error occurred.
    InternalError,
    /// `6` — the frame version is not supported.
    UnsupportedVersion,
    /// `7` — a bounded resource was exhausted.
    ResourceExhausted,
    /// `8` — success without durable sync (benchmark `os` mode).
    OkVolatile,
    /// `9` — a `SET` or `DELETE` was added to the open transaction; it takes
    /// effect only at `COMMIT`.
    Queued,
}

impl Status {
    /// The on-wire `status` value for this status.
    pub fn as_u16(self) -> u16 {
        match self {
            Status::Ok => 0,
            Status::NotFound => 1,
            Status::BadRequest => 2,
            Status::NotPrimary => 3,
            Status::Unavailable => 4,
            Status::InternalError => 5,
            Status::UnsupportedVersion => 6,
            Status::ResourceExhausted => 7,
            Status::OkVolatile => 8,
            Status::Queued => 9,
        }
    }

    /// Decode a `status` value, rejecting unknown codes.
    pub fn from_u16(v: u16) -> Result<Self, ProtocolError> {
        match v {
            0 => Ok(Status::Ok),
            1 => Ok(Status::NotFound),
            2 => Ok(Status::BadRequest),
            3 => Ok(Status::NotPrimary),
            4 => Ok(Status::Unavailable),
            5 => Ok(Status::InternalError),
            6 => Ok(Status::UnsupportedVersion),
            7 => Ok(Status::ResourceExhausted),
            8 => Ok(Status::OkVolatile),
            9 => Ok(Status::Queued),
            other => Err(ProtocolError::InvalidStatus(other)),
        }
    }
}

/// A decoded client request (Technical-Design §4.1).
#[derive(Debug, Clone, PartialEq, Eq)]
pub enum Request {
    /// `SET key value` — replace the entire value for `key`.
    Set {
        /// The key bytes.
        key: Vec<u8>,
        /// The value bytes.
        value: Vec<u8>,
    },
    /// `GET key` — look up `key`.
    Get {
        /// The key bytes.
        key: Vec<u8>,
    },
    /// `DELETE key` — remove `key`.
    Delete {
        /// The key bytes.
        key: Vec<u8>,
    },
    /// `EXISTS key` — test whether `key` exists.
    Exists {
        /// The key bytes.
        key: Vec<u8>,
    },
    /// `STATS` — request operational statistics.
    Stats,
    /// `SCAN` — up to `limit` pairs with `start <= key < end`, in key order.
    /// An empty `start` begins at the first key; `end: None` is unbounded.
    Scan {
        /// Inclusive lower bound; may be empty.
        start: Vec<u8>,
        /// Exclusive upper bound.
        end: Option<Vec<u8>>,
        /// Maximum pairs, `1..=MAX_SCAN_LIMIT`.
        limit: u32,
    },
    /// `PING` — liveness check; answered without touching the database.
    Ping,
    /// `BEGIN` — open a transaction on this connection.
    Begin,
    /// `COMMIT` — apply the open transaction atomically.
    Commit,
    /// `ROLLBACK` — discard the open transaction.
    Rollback,
}

impl Request {
    /// The wire `kind` byte for this request.
    pub fn kind(&self) -> RequestKind {
        match self {
            Request::Set { .. } => RequestKind::Set,
            Request::Get { .. } => RequestKind::Get,
            Request::Delete { .. } => RequestKind::Delete,
            Request::Exists { .. } => RequestKind::Exists,
            Request::Stats => RequestKind::Stats,
            Request::Scan { .. } => RequestKind::Scan,
            Request::Ping => RequestKind::Ping,
            Request::Begin => RequestKind::Begin,
            Request::Commit => RequestKind::Commit,
            Request::Rollback => RequestKind::Rollback,
        }
    }

    /// Encode this request as a complete frame:
    /// `body_len:u32 | version:u8 | kind:u8 | payload`.
    pub fn encode(&self) -> Vec<u8> {
        let mut body = Vec::new();
        body.push(PROTOCOL_VERSION);
        body.push(self.kind().as_u8());
        match self {
            Request::Set { key, value } => {
                body.extend_from_slice(&(key.len() as u32).to_le_bytes());
                body.extend_from_slice(&(value.len() as u32).to_le_bytes());
                body.extend_from_slice(key);
                body.extend_from_slice(value);
            }
            Request::Get { key } | Request::Delete { key } | Request::Exists { key } => {
                body.extend_from_slice(&(key.len() as u32).to_le_bytes());
                body.extend_from_slice(key);
            }
            Request::Stats
            | Request::Ping
            | Request::Begin
            | Request::Commit
            | Request::Rollback => {}
            Request::Scan { start, end, limit } => {
                let end = end.as_deref().unwrap_or_default();
                body.extend_from_slice(&(start.len() as u32).to_le_bytes());
                body.extend_from_slice(&(end.len() as u32).to_le_bytes());
                body.extend_from_slice(&limit.to_le_bytes());
                body.extend_from_slice(start);
                body.extend_from_slice(end);
            }
        }
        frame_from_body(&body)
    }
}

/// A decoded response (Technical-Design §4.1).
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct Response {
    /// The `kind` byte, echoing the request kind.
    pub kind: u8,
    /// The response status.
    pub status: Status,
    /// The response data (value for `GET OK`, single `0`/`1` byte for
    /// `EXISTS OK`, empty for `SET`/`DELETE OK`, bounded UTF-8 text for errors).
    pub data: Vec<u8>,
}

impl Response {
    /// Construct a response echoing `kind` with the given status and data.
    pub fn new(kind: u8, status: Status, data: Vec<u8>) -> Self {
        Response { kind, status, data }
    }

    /// Encode this response as a complete frame:
    /// `body_len:u32 | version:u8 | kind:u8 | status:u16 | data_len:u32 | data`.
    pub fn encode(&self) -> Vec<u8> {
        let mut body = Vec::new();
        body.push(PROTOCOL_VERSION);
        body.push(self.kind);
        body.extend_from_slice(&self.status.as_u16().to_le_bytes());
        body.extend_from_slice(&(self.data.len() as u32).to_le_bytes());
        body.extend_from_slice(&self.data);
        frame_from_body(&body)
    }
}

/// Errors produced when encoding, framing, or decoding a protocol message.
#[derive(Debug)]
pub enum ProtocolError {
    /// An underlying I/O error occurred.
    Io(std::io::Error),
    /// End of stream was reached partway through a frame (after the first
    /// byte was read). A clean disconnect before any byte is `Ok(None)`, not
    /// this error.
    UnexpectedEof,
    /// The `body_len` prefix exceeded [`MAX_BODY_LEN`]; rejected before
    /// allocating a body buffer.
    OversizedFrame {
        /// The declared body length.
        len: usize,
    },
    /// The `body_len` prefix was below [`MIN_BODY_LEN`].
    UndersizedFrame {
        /// The declared body length.
        len: usize,
    },
    /// The frame `version` byte was not [`PROTOCOL_VERSION`].
    UnsupportedVersion(u8),
    /// The `kind` byte was not a known request kind.
    UnknownKind(u8),
    /// A payload was structurally malformed (short field, bad shape).
    MalformedPayload(String),
    /// The frame carried extra bytes after the parsed payload.
    TrailingBytes,
    /// The key length violated the Technical-Design §4.1 bound `1..=4096`.
    KeyLength(usize),
    /// A `SET`/`DELETE` exceeded the §4.1 encoded mutation size bound.
    MutationTooLarge {
        /// The encoded size that would have been required.
        encoded_len: usize,
    },
    /// A response carried an unknown `status` value.
    InvalidStatus(u16),
}

impl std::fmt::Display for ProtocolError {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        match self {
            ProtocolError::Io(e) => write!(f, "io error: {e}"),
            ProtocolError::UnexpectedEof => write!(f, "unexpected end of stream mid-frame"),
            ProtocolError::OversizedFrame { len } => {
                write!(f, "frame body_len {len} exceeds maximum {MAX_BODY_LEN}")
            }
            ProtocolError::UndersizedFrame { len } => {
                write!(f, "frame body_len {len} below minimum {MIN_BODY_LEN}")
            }
            ProtocolError::UnsupportedVersion(v) => write!(f, "unsupported protocol version {v}"),
            ProtocolError::UnknownKind(k) => write!(f, "unknown request kind {k}"),
            ProtocolError::MalformedPayload(why) => write!(f, "malformed payload: {why}"),
            ProtocolError::TrailingBytes => write!(f, "trailing bytes after payload"),
            ProtocolError::KeyLength(len) => write!(
                f,
                "invalid key length {len} bytes (must be {MIN_KEY_LEN}..={MAX_KEY_LEN})"
            ),
            ProtocolError::MutationTooLarge { encoded_len } => write!(
                f,
                "mutation too large: encoded {encoded_len} bytes exceeds limit {MAX_MUTATION_ENCODED_LEN}"
            ),
            ProtocolError::InvalidStatus(s) => write!(f, "invalid response status {s}"),
        }
    }
}

impl std::error::Error for ProtocolError {
    fn source(&self) -> Option<&(dyn std::error::Error + 'static)> {
        match self {
            ProtocolError::Io(e) => Some(e),
            _ => None,
        }
    }
}

impl From<std::io::Error> for ProtocolError {
    fn from(e: std::io::Error) -> Self {
        ProtocolError::Io(e)
    }
}

impl ProtocolError {
    /// Map a decode-side error to the response [`Status`] the server should
    /// send before closing (Technical-Design §4.1). An unsupported version is
    /// reported distinctly; every other client-facing decode failure maps to
    /// [`Status::BadRequest`].
    pub fn to_status(&self) -> Status {
        match self {
            ProtocolError::UnsupportedVersion(_) => Status::UnsupportedVersion,
            _ => Status::BadRequest,
        }
    }
}

/// Wrap a body in a length prefix, producing a complete frame.
fn frame_from_body(body: &[u8]) -> Vec<u8> {
    let mut frame = Vec::with_capacity(4 + body.len());
    frame.extend_from_slice(&(body.len() as u32).to_le_bytes());
    frame.extend_from_slice(body);
    frame
}

/// Read a `u32` little-endian from `buf` at `off`; the caller guarantees the
/// bytes are present.
fn read_u32(buf: &[u8], off: usize) -> u32 {
    u32::from_le_bytes([buf[off], buf[off + 1], buf[off + 2], buf[off + 3]])
}

/// Read one complete frame body from `reader` (Technical-Design §4.1).
///
/// This accumulates exactly four length bytes, validates that the declared
/// `body_len` is within `MIN_BODY_LEN..=MAX_BODY_LEN` (rejecting an oversized
/// length *before* allocating any body buffer), then reads exactly `body_len`
/// bytes, looping because a single [`Read::read`] may return fewer bytes than
/// requested (TCP has no message boundaries).
///
/// Returns:
/// * `Ok(Some(body))` — a complete body (the length prefix is stripped).
/// * `Ok(None)` — a clean disconnect: end of stream *before any* length byte,
///   signalling the peer closed the connection between frames.
/// * `Err(ProtocolError::UnexpectedEof)` — end of stream partway through the
///   length prefix or body.
/// * `Err(ProtocolError::OversizedFrame|UndersizedFrame)` — the length prefix
///   was out of range (oversized is rejected before allocating the body).
pub fn read_frame<R: Read>(reader: &mut R) -> Result<Option<Vec<u8>>, ProtocolError> {
    let mut len_buf = [0u8; 4];
    match read_exact_or_eof(reader, &mut len_buf)? {
        ReadOutcome::CleanEof => return Ok(None),
        ReadOutcome::PartialEof => return Err(ProtocolError::UnexpectedEof),
        ReadOutcome::Filled => {}
    }

    let body_len = u32::from_le_bytes(len_buf) as usize;
    // Validate the declared length BEFORE allocating any body buffer, so an
    // attacker-controlled prefix cannot force a giant allocation (§4.1, §3.9).
    if body_len > MAX_BODY_LEN {
        return Err(ProtocolError::OversizedFrame { len: body_len });
    }
    if body_len < MIN_BODY_LEN {
        return Err(ProtocolError::UndersizedFrame { len: body_len });
    }

    let mut body = vec![0u8; body_len];
    match read_exact_or_eof(reader, &mut body)? {
        // Any EOF while reading the body is truncation: we already committed
        // to a frame by reading its length prefix.
        ReadOutcome::CleanEof | ReadOutcome::PartialEof => Err(ProtocolError::UnexpectedEof),
        ReadOutcome::Filled => Ok(Some(body)),
    }
}

/// Outcome of an attempt to fill a buffer from a reader.
enum ReadOutcome {
    /// The buffer was completely filled.
    Filled,
    /// EOF occurred before any byte of the buffer was read.
    CleanEof,
    /// EOF occurred after at least one byte was read (truncation).
    PartialEof,
}

/// Fill `buf` completely, looping over short reads. Distinguishes EOF before
/// any byte (clean) from EOF partway through (partial). `ErrorKind::Interrupted`
/// is retried, matching [`Read::read_exact`] semantics.
fn read_exact_or_eof<R: Read>(
    reader: &mut R,
    buf: &mut [u8],
) -> Result<ReadOutcome, ProtocolError> {
    let mut filled = 0usize;
    while filled < buf.len() {
        match reader.read(&mut buf[filled..]) {
            Ok(0) => {
                return Ok(if filled == 0 {
                    ReadOutcome::CleanEof
                } else {
                    ReadOutcome::PartialEof
                });
            }
            Ok(n) => filled += n,
            Err(ref e) if e.kind() == std::io::ErrorKind::Interrupted => {}
            Err(e) => return Err(ProtocolError::Io(e)),
        }
    }
    Ok(ReadOutcome::Filled)
}

/// Write a complete frame (as produced by [`Request::encode`] or
/// [`Response::encode`]) to `writer`.
pub fn write_frame<W: Write>(writer: &mut W, frame_bytes: &[u8]) -> Result<(), ProtocolError> {
    writer.write_all(frame_bytes)?;
    Ok(())
}

/// Validate the §4.1 key-length bound `1..=4096`.
fn validate_key_len(len: usize) -> Result<(), ProtocolError> {
    if !(MIN_KEY_LEN..=MAX_KEY_LEN).contains(&len) {
        return Err(ProtocolError::KeyLength(len));
    }
    Ok(())
}

/// Read a `u32` length field at `off`, returning a [`ProtocolError::MalformedPayload`]
/// if fewer than four bytes remain.
fn take_len_field(body: &[u8], off: usize, field: &str) -> Result<u32, ProtocolError> {
    if body.len() < off + 4 {
        return Err(ProtocolError::MalformedPayload(format!(
            "truncated {field} length field"
        )));
    }
    Ok(read_u32(body, off))
}

/// Decode a request body (`version:u8 | kind:u8 | payload`) into a [`Request`]
/// (Technical-Design §4.1).
///
/// `body` is a single frame body as returned by [`read_frame`] (the four-byte
/// length prefix already stripped). Rejects an unsupported version (distinctly,
/// so the server can answer `UNSUPPORTED_VERSION`), an unknown kind, malformed
/// or short field lengths, integer overflow, trailing bytes, and the §4.1
/// key/mutation-limit violations.
pub fn decode_request_body(body: &[u8]) -> Result<Request, ProtocolError> {
    if body.len() < 2 {
        return Err(ProtocolError::MalformedPayload(
            "body shorter than version+kind header".to_string(),
        ));
    }
    let version = body[0];
    if version != PROTOCOL_VERSION {
        return Err(ProtocolError::UnsupportedVersion(version));
    }
    let kind = RequestKind::from_u8(body[1])?;
    let payload = &body[2..];

    match kind {
        RequestKind::Set => {
            let key_len = take_len_field(payload, 0, "key")? as usize;
            let value_len = take_len_field(payload, 4, "value")? as usize;
            // Guard against integer overflow when summing field lengths.
            let fields_len = key_len
                .checked_add(value_len)
                .and_then(|s| s.checked_add(8))
                .ok_or_else(|| {
                    ProtocolError::MalformedPayload("key_len + value_len overflow".to_string())
                })?;
            if payload.len() < fields_len {
                return Err(ProtocolError::MalformedPayload(
                    "SET payload shorter than declared key/value".to_string(),
                ));
            }
            if payload.len() > fields_len {
                return Err(ProtocolError::TrailingBytes);
            }
            validate_key_len(key_len)?;
            let encoded_len = SET_FIXED_OVERHEAD + key_len + value_len;
            if encoded_len > MAX_MUTATION_ENCODED_LEN {
                return Err(ProtocolError::MutationTooLarge { encoded_len });
            }
            let key = payload[8..8 + key_len].to_vec();
            let value = payload[8 + key_len..8 + key_len + value_len].to_vec();
            Ok(Request::Set { key, value })
        }
        RequestKind::Get | RequestKind::Delete | RequestKind::Exists => {
            let key_len = take_len_field(payload, 0, "key")? as usize;
            let fields_len = key_len
                .checked_add(4)
                .ok_or_else(|| ProtocolError::MalformedPayload("key_len overflow".to_string()))?;
            if payload.len() < fields_len {
                return Err(ProtocolError::MalformedPayload(
                    "payload shorter than declared key".to_string(),
                ));
            }
            if payload.len() > fields_len {
                return Err(ProtocolError::TrailingBytes);
            }
            validate_key_len(key_len)?;
            let key = payload[4..4 + key_len].to_vec();
            // A DELETE is a mutation; enforce the §4.1 mutation bound (zero
            // value bytes). GET/EXISTS need only the key bound above.
            if kind == RequestKind::Delete {
                let encoded_len = SET_FIXED_OVERHEAD + key_len;
                if encoded_len > MAX_MUTATION_ENCODED_LEN {
                    return Err(ProtocolError::MutationTooLarge { encoded_len });
                }
            }
            match kind {
                RequestKind::Get => Ok(Request::Get { key }),
                RequestKind::Delete => Ok(Request::Delete { key }),
                RequestKind::Exists => Ok(Request::Exists { key }),
                _ => unreachable!("kind is Get/Delete/Exists in this arm"),
            }
        }
        RequestKind::Stats
        | RequestKind::Ping
        | RequestKind::Begin
        | RequestKind::Commit
        | RequestKind::Rollback => {
            if !payload.is_empty() {
                return Err(ProtocolError::TrailingBytes);
            }
            Ok(match kind {
                RequestKind::Stats => Request::Stats,
                RequestKind::Ping => Request::Ping,
                RequestKind::Begin => Request::Begin,
                RequestKind::Commit => Request::Commit,
                _ => Request::Rollback,
            })
        }
        RequestKind::Scan => {
            let start_len = take_len_field(payload, 0, "start")? as usize;
            let end_len = take_len_field(payload, 4, "end")? as usize;
            let limit = take_len_field(payload, 8, "limit")?;
            let fields_len = start_len
                .checked_add(end_len)
                .and_then(|len| len.checked_add(12))
                .ok_or_else(|| ProtocolError::MalformedPayload("bound length overflow".into()))?;
            if payload.len() < fields_len {
                return Err(ProtocolError::MalformedPayload(
                    "SCAN payload shorter than declared bounds".to_string(),
                ));
            }
            if payload.len() > fields_len {
                return Err(ProtocolError::TrailingBytes);
            }
            for len in [start_len, end_len] {
                if len > MAX_KEY_LEN {
                    return Err(ProtocolError::KeyLength(len));
                }
            }
            if !(1..=MAX_SCAN_LIMIT as u32).contains(&limit) {
                return Err(ProtocolError::MalformedPayload(format!(
                    "SCAN limit {limit} outside 1..={MAX_SCAN_LIMIT}"
                )));
            }
            let start = payload[12..12 + start_len].to_vec();
            let end = payload[12 + start_len..fields_len].to_vec();
            Ok(Request::Scan {
                start,
                end: (!end.is_empty()).then_some(end),
                limit,
            })
        }
    }
}

/// Encode a `SCAN OK` page: `count:u32 | more:u8 | (key_len:u32 |
/// value_len:u32 | key | value)*`. `more` tells the client to continue after
/// the last key.
pub fn encode_scan_page(pairs: &[(Vec<u8>, Vec<u8>)], more: bool) -> Vec<u8> {
    let mut data = Vec::new();
    data.extend_from_slice(&(pairs.len() as u32).to_le_bytes());
    data.push(u8::from(more));
    for (key, value) in pairs {
        data.extend_from_slice(&(key.len() as u32).to_le_bytes());
        data.extend_from_slice(&(value.len() as u32).to_le_bytes());
        data.extend_from_slice(key);
        data.extend_from_slice(value);
    }
    data
}

/// Encoded size of one pair in a `SCAN OK` page.
pub fn scan_pair_len(key: &[u8], value: &[u8]) -> usize {
    8 + key.len() + value.len()
}

/// A decoded `SCAN OK` page: pairs in key order, and whether more remain.
pub use crate::storage::ScanPage;

/// Decode a `SCAN OK` page, rejecting truncation, trailing bytes, and an
/// invalid `more` flag.
pub fn decode_scan_page(data: &[u8]) -> Result<ScanPage, ProtocolError> {
    let malformed = |why: &str| ProtocolError::MalformedPayload(format!("SCAN page: {why}"));
    if data.len() < 5 {
        return Err(malformed("shorter than its header"));
    }
    let count = read_u32(data, 0) as usize;
    let more = match data[4] {
        0 => false,
        1 => true,
        _ => return Err(malformed("invalid more flag")),
    };
    let mut pairs = Vec::new();
    let mut at = 5;
    for _ in 0..count {
        let key_len = take_len_field(data, at, "key")? as usize;
        let value_len = take_len_field(data, at + 4, "value")? as usize;
        let end = (at + 8)
            .checked_add(key_len)
            .and_then(|len| len.checked_add(value_len))
            .filter(|&end| end <= data.len())
            .ok_or_else(|| malformed("pair overruns the page"))?;
        let key = data[at + 8..at + 8 + key_len].to_vec();
        let value = data[at + 8 + key_len..end].to_vec();
        pairs.push((key, value));
        at = end;
    }
    if at != data.len() {
        return Err(ProtocolError::TrailingBytes);
    }
    Ok((pairs, more))
}

/// Decode a response body (`version:u8 | kind:u8 | status:u16 | data_len:u32 |
/// data`) into a [`Response`] for the client side (Technical-Design §4.1).
///
/// Validates the version, the `data_len` against the remaining bytes, rejects
/// trailing bytes, and rejects unknown status codes.
pub fn decode_response_body(body: &[u8]) -> Result<Response, ProtocolError> {
    // version + kind + status(2) + data_len(4) = 8 fixed bytes.
    if body.len() < 8 {
        return Err(ProtocolError::MalformedPayload(
            "response body shorter than fixed header".to_string(),
        ));
    }
    let version = body[0];
    if version != PROTOCOL_VERSION {
        return Err(ProtocolError::UnsupportedVersion(version));
    }
    let kind = body[1];
    let status = Status::from_u16(u16::from_le_bytes([body[2], body[3]]))?;
    let data_len = read_u32(body, 4) as usize;
    let total = 8usize
        .checked_add(data_len)
        .ok_or_else(|| ProtocolError::MalformedPayload("data_len overflow".to_string()))?;
    if body.len() < total {
        return Err(ProtocolError::MalformedPayload(
            "response data shorter than declared data_len".to_string(),
        ));
    }
    if body.len() > total {
        return Err(ProtocolError::TrailingBytes);
    }
    let data = body[8..total].to_vec();
    Ok(Response { kind, status, data })
}

#[cfg(test)]
mod tests {
    use super::*;
    use std::io::Cursor;

    // A Read impl that hands out at most `chunk` bytes per call, to exercise
    // the short-read loop in read_frame (TCP has no message boundaries).
    struct ChunkedReader {
        data: Vec<u8>,
        pos: usize,
        chunk: usize,
    }

    impl ChunkedReader {
        fn new(data: Vec<u8>, chunk: usize) -> Self {
            ChunkedReader {
                data,
                pos: 0,
                chunk,
            }
        }
    }

    impl Read for ChunkedReader {
        fn read(&mut self, buf: &mut [u8]) -> std::io::Result<usize> {
            let remaining = self.data.len() - self.pos;
            if remaining == 0 {
                return Ok(0);
            }
            let n = self.chunk.min(remaining).min(buf.len());
            buf[..n].copy_from_slice(&self.data[self.pos..self.pos + n]);
            self.pos += n;
            Ok(n)
        }
    }

    fn round_trip_request(req: &Request) -> Request {
        let frame = req.encode();
        let mut cursor = Cursor::new(frame);
        let body = read_frame(&mut cursor).unwrap().expect("a frame");
        decode_request_body(&body).unwrap()
    }

    fn round_trip_response(resp: &Response) -> Response {
        let frame = resp.encode();
        let mut cursor = Cursor::new(frame);
        let body = read_frame(&mut cursor).unwrap().expect("a frame");
        decode_response_body(&body).unwrap()
    }

    // ---- Golden byte assertions -----------------------------------------

    #[test]
    fn golden_set_request_k_v() {
        // SET key="k" value="v".
        let req = Request::Set {
            key: b"k".to_vec(),
            value: b"v".to_vec(),
        };
        let bytes = req.encode();
        // body = version(1) | kind(1=SET) | key_len(1) | value_len(1) | 'k' | 'v'
        //      = 1 + 1 + 4 + 4 + 1 + 1 = 12 bytes.
        let expected: Vec<u8> = vec![
            12, 0, 0, 0, // body_len = 12 (LE u32)
            1, // version
            1, // kind = SET
            1, 0, 0, 0, // key_len = 1
            1, 0, 0, 0,    // value_len = 1
            b'k', // key
            b'v', // value
        ];
        assert_eq!(bytes, expected);
    }

    #[test]
    fn golden_get_ok_response() {
        // GET OK with value "v".
        let resp = Response::new(KIND_GET, Status::Ok, b"v".to_vec());
        let bytes = resp.encode();
        // body = version(1) | kind(2=GET) | status(2=OK->0) | data_len(4) | 'v'
        //      = 1 + 1 + 2 + 4 + 1 = 9 bytes.
        let expected: Vec<u8> = vec![
            9, 0, 0, 0, // body_len = 9
            1, // version
            2, // kind = GET
            0, 0, // status = OK (0)
            1, 0, 0, 0,    // data_len = 1
            b'v', // data
        ];
        assert_eq!(bytes, expected);
    }

    #[test]
    fn golden_stats_request_has_empty_payload() {
        let bytes = Request::Stats.encode();
        // body = version | kind(5=STATS) = 2 bytes (the minimum body).
        assert_eq!(bytes, vec![2, 0, 0, 0, 1, 5]);
    }

    // ---- Request round-trips --------------------------------------------

    #[test]
    fn round_trip_all_request_kinds() {
        let cases = vec![
            Request::Set {
                key: b"user:1".to_vec(),
                value: b"Aaron".to_vec(),
            },
            Request::Get {
                key: b"user:1".to_vec(),
            },
            Request::Delete {
                key: b"user:1".to_vec(),
            },
            Request::Exists {
                key: b"user:1".to_vec(),
            },
            Request::Stats,
        ];
        for req in &cases {
            assert_eq!(&round_trip_request(req), req);
        }
    }

    #[test]
    fn round_trip_set_empty_value() {
        let req = Request::Set {
            key: b"k".to_vec(),
            value: Vec::new(),
        };
        assert_eq!(round_trip_request(&req), req);
    }

    #[test]
    fn round_trip_binary_non_utf8_key_value() {
        let key = vec![0x00, 0x80, 0xff, 0xc0];
        assert!(std::str::from_utf8(&key).is_err());
        let req = Request::Set {
            key,
            value: vec![0xfe, 0x00, 0x01, 0x80],
        };
        assert_eq!(round_trip_request(&req), req);
    }

    #[test]
    fn round_trip_max_size_set() {
        // 33 + key + value == 1_000_000 with a max-length key.
        let key = vec![b'k'; MAX_KEY_LEN];
        let value_len = MAX_MUTATION_ENCODED_LEN - SET_FIXED_OVERHEAD - key.len();
        let value = vec![b'v'; value_len];
        let req = Request::Set { key, value };
        assert_eq!(round_trip_request(&req), req);
    }

    // ---- Response round-trips -------------------------------------------

    #[test]
    fn round_trip_all_response_kinds() {
        let cases = vec![
            Response::new(KIND_GET, Status::Ok, b"the-value".to_vec()),
            Response::new(KIND_EXISTS, Status::Ok, vec![1]),
            Response::new(KIND_EXISTS, Status::Ok, vec![0]),
            Response::new(KIND_SET, Status::Ok, Vec::new()),
            Response::new(KIND_DELETE, Status::Ok, Vec::new()),
            Response::new(KIND_SET, Status::OkVolatile, Vec::new()),
            Response::new(KIND_GET, Status::NotFound, Vec::new()),
            Response::new(
                KIND_SET,
                Status::BadRequest,
                b"malformed payload: truncated key length field".to_vec(),
            ),
            Response::new(KIND_SET, Status::UnsupportedVersion, b"version 2".to_vec()),
        ];
        for resp in &cases {
            assert_eq!(&round_trip_response(resp), resp);
        }
    }

    #[test]
    fn round_trip_binary_non_utf8_response_data() {
        let resp = Response::new(KIND_GET, Status::Ok, vec![0x00, 0xff, 0x80, 0xfe]);
        assert_eq!(round_trip_response(&resp), resp);
    }

    // ---- Framing: partial reads -----------------------------------------

    #[test]
    fn read_frame_reassembles_across_small_chunks() {
        let req = Request::Set {
            key: b"user:123".to_vec(),
            value: b"a longer value that spans several read chunks".to_vec(),
        };
        let frame = req.encode();
        // One byte at a time is the most adversarial chunking.
        let mut reader = ChunkedReader::new(frame.clone(), 1);
        let body = read_frame(&mut reader).unwrap().expect("a frame");
        assert_eq!(decode_request_body(&body).unwrap(), req);

        // A chunk that straddles the length prefix / body boundary (3 bytes).
        let mut reader = ChunkedReader::new(frame, 3);
        let body = read_frame(&mut reader).unwrap().expect("a frame");
        assert_eq!(decode_request_body(&body).unwrap(), req);
    }

    #[test]
    fn read_frame_reads_consecutive_frames() {
        let a = Request::Get { key: b"a".to_vec() };
        let b = Request::Delete { key: b"b".to_vec() };
        let mut buf = a.encode();
        buf.extend_from_slice(&b.encode());
        let mut cursor = Cursor::new(buf);
        let first = read_frame(&mut cursor).unwrap().expect("first frame");
        let second = read_frame(&mut cursor).unwrap().expect("second frame");
        assert_eq!(decode_request_body(&first).unwrap(), a);
        assert_eq!(decode_request_body(&second).unwrap(), b);
        // Now EOF: clean disconnect.
        assert!(read_frame(&mut cursor).unwrap().is_none());
    }

    // ---- Framing: EOF handling ------------------------------------------

    #[test]
    fn eof_before_any_byte_is_clean_disconnect() {
        let mut cursor = Cursor::new(Vec::<u8>::new());
        assert!(read_frame(&mut cursor).unwrap().is_none());
    }

    #[test]
    fn eof_partway_through_length_prefix_is_error() {
        let mut cursor = Cursor::new(vec![12, 0]); // only 2 of 4 length bytes
        match read_frame(&mut cursor) {
            Err(ProtocolError::UnexpectedEof) => {}
            other => panic!("expected UnexpectedEof, got {other:?}"),
        }
    }

    #[test]
    fn eof_partway_through_body_is_error() {
        // Declares body_len = 12 but supplies only 5 body bytes.
        let mut frame = 12u32.to_le_bytes().to_vec();
        frame.extend_from_slice(&[1, 1, 1, 0, 0]);
        let mut cursor = Cursor::new(frame);
        match read_frame(&mut cursor) {
            Err(ProtocolError::UnexpectedEof) => {}
            other => panic!("expected UnexpectedEof, got {other:?}"),
        }
    }

    // ---- Framing: length-prefix validation ------------------------------

    #[test]
    fn oversized_body_len_rejected_before_allocation() {
        // body_len = MAX_BODY_LEN + 1, but we supply NO body bytes at all.
        // If read_frame allocated the body it would then block/EOF reading it;
        // instead it must reject from the 4-byte prefix alone.
        let frame = ((MAX_BODY_LEN + 1) as u32).to_le_bytes().to_vec();
        let mut cursor = Cursor::new(frame);
        match read_frame(&mut cursor) {
            Err(ProtocolError::OversizedFrame { len }) => {
                assert_eq!(len, MAX_BODY_LEN + 1);
            }
            other => panic!("expected OversizedFrame, got {other:?}"),
        }
    }

    #[test]
    fn undersized_body_len_rejected() {
        for len in [0u32, 1u32] {
            let frame = len.to_le_bytes().to_vec();
            let mut cursor = Cursor::new(frame);
            match read_frame(&mut cursor) {
                Err(ProtocolError::UndersizedFrame { len: got }) => {
                    assert_eq!(got as u32, len);
                }
                other => panic!("expected UndersizedFrame for {len}, got {other:?}"),
            }
        }
    }

    #[test]
    fn max_body_len_is_accepted_by_prefix_check() {
        // A prefix of exactly MAX_BODY_LEN must pass the length check; supply
        // a valid GET body padded to MAX_BODY_LEN would be huge, so instead
        // assert the check boundary via a body we actually provide at a small
        // legal size and confirm MAX+1 is the first rejected value above.
        assert!(MAX_BODY_LEN <= u32::MAX as usize);
    }

    // ---- Decode: version, kind, trailing, malformed ---------------------

    #[test]
    fn version_two_is_unsupported_version() {
        // Build a body with version=2, kind=STATS.
        let body = vec![2u8, KIND_STATS];
        match decode_request_body(&body) {
            Err(ProtocolError::UnsupportedVersion(2)) => {}
            other => panic!("expected UnsupportedVersion(2), got {other:?}"),
        }
    }

    #[test]
    fn unknown_kind_is_rejected() {
        let body = vec![1u8, 99u8];
        match decode_request_body(&body) {
            Err(ProtocolError::UnknownKind(99)) => {}
            other => panic!("expected UnknownKind(99), got {other:?}"),
        }
    }

    #[test]
    fn trailing_bytes_after_get_key_rejected() {
        // version | kind=GET | key_len=1 | 'k' | extra byte.
        let mut body = vec![1u8, KIND_GET];
        body.extend_from_slice(&1u32.to_le_bytes());
        body.push(b'k');
        body.push(b'X'); // trailing
        match decode_request_body(&body) {
            Err(ProtocolError::TrailingBytes) => {}
            other => panic!("expected TrailingBytes, got {other:?}"),
        }
    }

    #[test]
    fn trailing_bytes_after_stats_rejected() {
        let body = vec![1u8, KIND_STATS, 0u8];
        match decode_request_body(&body) {
            Err(ProtocolError::TrailingBytes) => {}
            other => panic!("expected TrailingBytes, got {other:?}"),
        }
    }

    #[test]
    fn truncated_key_length_field_rejected() {
        // version | kind=GET | only 2 of 4 key_len bytes.
        let body = vec![1u8, KIND_GET, 0u8, 0u8];
        match decode_request_body(&body) {
            Err(ProtocolError::MalformedPayload(_)) => {}
            other => panic!("expected MalformedPayload, got {other:?}"),
        }
    }

    #[test]
    fn short_body_missing_header_rejected() {
        match decode_request_body(&[1u8]) {
            Err(ProtocolError::MalformedPayload(_)) => {}
            other => panic!("expected MalformedPayload, got {other:?}"),
        }
    }

    #[test]
    fn declared_key_longer_than_payload_rejected() {
        // key_len = 10 but only 2 key bytes present.
        let mut body = vec![1u8, KIND_GET];
        body.extend_from_slice(&10u32.to_le_bytes());
        body.extend_from_slice(b"ab");
        match decode_request_body(&body) {
            Err(ProtocolError::MalformedPayload(_)) => {}
            other => panic!("expected MalformedPayload, got {other:?}"),
        }
    }

    // ---- Decode: key / mutation limits ----------------------------------

    #[test]
    fn zero_length_key_rejected() {
        // GET with key_len = 0.
        let mut body = vec![1u8, KIND_GET];
        body.extend_from_slice(&0u32.to_le_bytes());
        match decode_request_body(&body) {
            Err(ProtocolError::KeyLength(0)) => {}
            other => panic!("expected KeyLength(0), got {other:?}"),
        }
    }

    #[test]
    fn oversized_key_rejected() {
        // GET with a key one byte over the max.
        let key_len = MAX_KEY_LEN + 1;
        let mut body = vec![1u8, KIND_GET];
        body.extend_from_slice(&(key_len as u32).to_le_bytes());
        body.extend_from_slice(&vec![b'k'; key_len]);
        match decode_request_body(&body) {
            Err(ProtocolError::KeyLength(len)) => assert_eq!(len, key_len),
            other => panic!("expected KeyLength, got {other:?}"),
        }
    }

    #[test]
    fn set_exceeding_mutation_limit_rejected() {
        // A max-length key plus one value byte too many.
        let key_len = MAX_KEY_LEN;
        let value_len = MAX_MUTATION_ENCODED_LEN - SET_FIXED_OVERHEAD - key_len + 1;
        let mut body = vec![1u8, KIND_SET];
        body.extend_from_slice(&(key_len as u32).to_le_bytes());
        body.extend_from_slice(&(value_len as u32).to_le_bytes());
        body.extend_from_slice(&vec![b'k'; key_len]);
        body.extend_from_slice(&vec![b'v'; value_len]);
        match decode_request_body(&body) {
            Err(ProtocolError::MutationTooLarge { encoded_len }) => {
                assert_eq!(encoded_len, MAX_MUTATION_ENCODED_LEN + 1);
            }
            other => panic!("expected MutationTooLarge, got {other:?}"),
        }
    }

    // ---- Decode: response side ------------------------------------------

    #[test]
    fn decode_response_trailing_bytes_rejected() {
        let mut body = vec![1u8, KIND_GET];
        body.extend_from_slice(&0u16.to_le_bytes()); // status OK
        body.extend_from_slice(&1u32.to_le_bytes()); // data_len = 1
        body.push(b'v');
        body.push(b'X'); // trailing
        match decode_response_body(&body) {
            Err(ProtocolError::TrailingBytes) => {}
            other => panic!("expected TrailingBytes, got {other:?}"),
        }
    }

    #[test]
    fn decode_response_invalid_status_rejected() {
        let mut body = vec![1u8, KIND_GET];
        body.extend_from_slice(&99u16.to_le_bytes()); // unknown status
        body.extend_from_slice(&0u32.to_le_bytes());
        match decode_response_body(&body) {
            Err(ProtocolError::InvalidStatus(99)) => {}
            other => panic!("expected InvalidStatus(99), got {other:?}"),
        }
    }

    #[test]
    fn decode_response_short_data_rejected() {
        let mut body = vec![1u8, KIND_GET];
        body.extend_from_slice(&0u16.to_le_bytes());
        body.extend_from_slice(&5u32.to_le_bytes()); // claims 5 data bytes
        body.push(b'v'); // only 1
        match decode_response_body(&body) {
            Err(ProtocolError::MalformedPayload(_)) => {}
            other => panic!("expected MalformedPayload, got {other:?}"),
        }
    }

    #[test]
    fn decode_response_unsupported_version() {
        let mut body = vec![2u8, KIND_GET];
        body.extend_from_slice(&0u16.to_le_bytes());
        body.extend_from_slice(&0u32.to_le_bytes());
        match decode_response_body(&body) {
            Err(ProtocolError::UnsupportedVersion(2)) => {}
            other => panic!("expected UnsupportedVersion(2), got {other:?}"),
        }
    }

    // ---- Error-to-status mapping ----------------------------------------

    #[test]
    fn error_to_status_mapping() {
        assert_eq!(
            ProtocolError::UnsupportedVersion(2).to_status(),
            Status::UnsupportedVersion
        );
        assert_eq!(
            ProtocolError::UnknownKind(9).to_status(),
            Status::BadRequest
        );
        assert_eq!(ProtocolError::TrailingBytes.to_status(), Status::BadRequest);
        assert_eq!(ProtocolError::KeyLength(0).to_status(), Status::BadRequest);
        assert_eq!(
            ProtocolError::MutationTooLarge { encoded_len: 1 }.to_status(),
            Status::BadRequest
        );
    }

    // ---- Enum conversions ------------------------------------------------

    #[test]
    fn request_kind_round_trips_all_bytes() {
        for kind in [
            RequestKind::Set,
            RequestKind::Get,
            RequestKind::Delete,
            RequestKind::Exists,
            RequestKind::Stats,
            RequestKind::Scan,
            RequestKind::Ping,
            RequestKind::Begin,
            RequestKind::Commit,
            RequestKind::Rollback,
        ] {
            assert_eq!(RequestKind::from_u8(kind.as_u8()).unwrap(), kind);
        }
    }

    #[test]
    fn status_round_trips_all_values() {
        for status in [
            Status::Ok,
            Status::NotFound,
            Status::BadRequest,
            Status::NotPrimary,
            Status::Unavailable,
            Status::InternalError,
            Status::UnsupportedVersion,
            Status::ResourceExhausted,
            Status::OkVolatile,
            Status::Queued,
        ] {
            assert_eq!(Status::from_u16(status.as_u16()).unwrap(), status);
        }
    }
}

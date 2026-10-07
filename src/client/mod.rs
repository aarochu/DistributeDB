//! TCP client for the Phase 3 wire protocol (Technical-Design §4.1; SOW §5).
//!
//! A [`Client`] owns a single [`TcpStream`] to a DistributeDB server and offers
//! typed request methods. Each call encodes a [`Request`] frame with the
//! FEAT-001 codec, writes it, then reads exactly one [`Response`] frame with
//! [`protocol::read_frame`](crate::protocol::read_frame) (which handles partial
//! reads across TCP segment boundaries). There is one outstanding request per
//! connection: the protocol is strictly request/response over a single stream.
//!
//! # Disconnect / retry limitation (Technical-Design §4.2)
//!
//! If the connection drops before a `SET`/`DELETE` response is read, the
//! outcome is UNKNOWN: the mutation may or may not have been committed
//! durably, and there is no server-side de-duplication. A caller that retries
//! may apply the mutation twice. `SET` and `DELETE` are idempotent, so a naive
//! retry is safe for those operations only.

use std::io::Write;
use std::net::{TcpStream, ToSocketAddrs};
use std::time::Duration;

use crate::protocol::{self, ProtocolError, Request, Response, Status};

/// Errors surfaced by [`Client`] operations.
#[derive(Debug)]
pub enum ClientError {
    /// A transport or framing error (connect, write, read, or decode).
    Protocol(ProtocolError),
    /// An I/O error establishing or using the connection.
    Io(std::io::Error),
    /// The server closed the connection without sending a response frame.
    Disconnected,
    /// The server returned an unexpected status for the operation. Carries the
    /// status and any diagnostic bytes the server attached.
    UnexpectedStatus {
        /// The status the server returned.
        status: Status,
        /// Server-attached diagnostic bytes (may be empty).
        data: Vec<u8>,
    },
}

impl std::fmt::Display for ClientError {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        match self {
            ClientError::Protocol(e) => write!(f, "protocol error: {e}"),
            ClientError::Io(e) => write!(f, "io error: {e}"),
            ClientError::Disconnected => write!(f, "server disconnected before responding"),
            ClientError::UnexpectedStatus { status, data } => {
                let detail = String::from_utf8_lossy(data);
                write!(f, "unexpected status {status:?}: {detail}")
            }
        }
    }
}

impl std::error::Error for ClientError {}

impl From<ProtocolError> for ClientError {
    fn from(e: ProtocolError) -> Self {
        ClientError::Protocol(e)
    }
}

impl From<std::io::Error> for ClientError {
    fn from(e: std::io::Error) -> Self {
        ClientError::Io(e)
    }
}

/// A convenience result alias for client operations.
pub type ClientResult<T> = Result<T, ClientError>;

/// A connected TCP client to a DistributeDB server.
pub struct Client {
    stream: TcpStream,
}

impl Client {
    /// Connect to a server at `addr` (e.g. `"127.0.0.1:5555"`).
    pub fn connect<A: ToSocketAddrs>(addr: A) -> ClientResult<Self> {
        let stream = TcpStream::connect(addr)?;
        stream.set_nodelay(true).ok();
        Ok(Client { stream })
    }

    /// Connect to `addr` within `timeout`, and fail any later read or write
    /// that blocks longer than `timeout`. For callers, such as a monitor,
    /// that must not hang on an unresponsive node.
    pub fn connect_timeout(addr: &std::net::SocketAddr, timeout: Duration) -> ClientResult<Self> {
        let stream = TcpStream::connect_timeout(addr, timeout)?;
        stream.set_nodelay(true).ok();
        stream.set_read_timeout(Some(timeout))?;
        stream.set_write_timeout(Some(timeout))?;
        Ok(Client { stream })
    }

    /// The address of the connected server.
    pub fn peer_addr(&self) -> ClientResult<std::net::SocketAddr> {
        Ok(self.stream.peer_addr()?)
    }

    /// Send one request frame and read exactly one response frame.
    fn round_trip(&mut self, request: &Request) -> ClientResult<Response> {
        let frame = request.encode();
        self.stream.write_all(&frame)?;
        self.stream.flush()?;
        match protocol::read_frame(&mut self.stream)? {
            Some(body) => Ok(protocol::decode_response_body(&body)?),
            None => Err(ClientError::Disconnected),
        }
    }

    /// `SET key value`. Returns the response [`Status`] (`Ok` on success).
    pub fn set(&mut self, key: Vec<u8>, value: Vec<u8>) -> ClientResult<Status> {
        let resp = self.round_trip(&Request::Set { key, value })?;
        Ok(resp.status)
    }

    /// `GET key`. Returns `Some(value)` on `Ok`, `None` on `NOT_FOUND`.
    pub fn get(&mut self, key: Vec<u8>) -> ClientResult<Option<Vec<u8>>> {
        let resp = self.round_trip(&Request::Get { key })?;
        match resp.status {
            Status::Ok => Ok(Some(resp.data)),
            Status::NotFound => Ok(None),
            status => Err(ClientError::UnexpectedStatus {
                status,
                data: resp.data,
            }),
        }
    }

    /// `DELETE key`. Returns the response [`Status`] (`Ok` on success).
    pub fn delete(&mut self, key: Vec<u8>) -> ClientResult<Status> {
        let resp = self.round_trip(&Request::Delete { key })?;
        Ok(resp.status)
    }

    /// `EXISTS key`. Returns whether the key exists.
    pub fn exists(&mut self, key: Vec<u8>) -> ClientResult<bool> {
        let resp = self.round_trip(&Request::Exists { key })?;
        match resp.status {
            Status::Ok => Ok(resp.data.first().is_some_and(|&b| b != 0)),
            status => Err(ClientError::UnexpectedStatus {
                status,
                data: resp.data,
            }),
        }
    }

    /// `SCAN`: up to `limit` pairs with `start <= key < end` in key order,
    /// and whether more remain. An empty `start` begins at the first key and
    /// `end: None` is unbounded. A page may hold fewer than `limit` pairs
    /// when the response would exceed one frame; continue from the last key
    /// with a `0` byte appended.
    pub fn scan(
        &mut self,
        start: Vec<u8>,
        end: Option<Vec<u8>>,
        limit: u32,
    ) -> ClientResult<protocol::ScanPage> {
        let resp = self.round_trip(&Request::Scan { start, end, limit })?;
        match resp.status {
            Status::Ok => Ok(protocol::decode_scan_page(&resp.data)?),
            status => Err(ClientError::UnexpectedStatus {
                status,
                data: resp.data,
            }),
        }
    }

    /// `BEGIN`: open a transaction on this connection. Until `COMMIT`,
    /// [`Client::set`] and [`Client::delete`] return [`Status::Queued`].
    pub fn begin(&mut self) -> ClientResult<Status> {
        Ok(self.round_trip(&Request::Begin)?.status)
    }

    /// `COMMIT`: apply the open transaction atomically. `OK` means every
    /// queued write is durable; any other status means none was applied.
    pub fn commit(&mut self) -> ClientResult<Status> {
        Ok(self.round_trip(&Request::Commit)?.status)
    }

    /// `ROLLBACK`: discard the open transaction.
    pub fn rollback(&mut self) -> ClientResult<Status> {
        Ok(self.round_trip(&Request::Rollback)?.status)
    }

    /// `PING`. Succeeds when the server answers `PONG`.
    pub fn ping(&mut self) -> ClientResult<()> {
        let resp = self.round_trip(&Request::Ping)?;
        match resp.status {
            Status::Ok if resp.data == b"PONG" => Ok(()),
            status => Err(ClientError::UnexpectedStatus {
                status,
                data: resp.data,
            }),
        }
    }

    /// `STATS`. Returns the server's operational statistics as text
    /// (`name=value` lines, version first).
    pub fn stats(&mut self) -> ClientResult<String> {
        let resp = self.round_trip(&Request::Stats)?;
        match resp.status {
            Status::Ok => Ok(String::from_utf8_lossy(&resp.data).into_owned()),
            status => Err(ClientError::UnexpectedStatus {
                status,
                data: resp.data,
            }),
        }
    }
}

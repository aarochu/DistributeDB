//! DistributeDB crate root.
//!
//! Phase 1 provides a local, single-threaded, in-memory key-value engine.
//! The module tree mirrors the layout in the Technical Design document:
//! `storage` holds the `StorageEngine` contract (Technical-Design §2.1) and
//! `command` holds the command types and parser for the key-value interface
//! (SOW §4.1: SET, GET, DELETE, EXISTS).
//!
//! FEAT-002 fills in the storage engine and the command types/parser;
//! FEAT-003 expands `main.rs` into a small REPL/CLI over the engine.
//!
//! Phase 2 adds the persistent-WAL foundations: `checksum` provides the
//! in-crate CRC32C and CRC64-ECMA-182 primitives (Technical-Design §6.1) and
//! `fileio` provides the append/sync/rename/truncate/read/lock file-I/O
//! abstraction with a real Linux adapter and a deterministic
//! simulated-power-loss adapter (Technical-Design §6.4, ADR-001).
//!
//! FEAT-002 adds `wal`: the persistent Write-Ahead Log with the exact §6.1
//! byte format, LSN sequencing, group-commit durability, the data-directory
//! layout, crash-recovery replay (§6.3, §9), and a [`Db`] entry point that
//! reconstructs a [`StorageEngine`] from disk.
//!
//! Phase 3 adds TCP networking: `protocol` is the std-only binary wire codec
//! (Technical-Design §4.1); `server` is the threaded TCP server with the §5
//! mutation-sequencer/group-commit architecture wired to [`Db`]; and `client`
//! is the matching typed TCP client. See the `server` module docs for the §5
//! concurrency mapping and the §4.2 disconnect/retry limitation.

pub mod checksum;
pub mod client;
pub mod command;
pub mod fileio;
pub mod protocol;
pub mod server;
pub mod storage;
pub mod wal;

// Re-export the primary types for ergonomic use by the CLI and tests.
pub use checksum::{crc32c, crc64_ecma, Crc32c, Crc64Ecma};
pub use client::{Client, ClientError, ClientResult};
pub use command::{parse, Command, ParseError};
pub use fileio::{
    FileSystem, FsError, FsResult, LockGuard, RealFs, SimConfig, SimFs, SplitMix64, SIM_PAGE_SIZE,
};
pub use protocol::{
    decode_request_body, decode_response_body, read_frame, write_frame, ProtocolError, Request,
    RequestKind, Response, Status, PROTOCOL_VERSION,
};
pub use server::{Server, ServerConfig, ShutdownHandle};
pub use storage::{GetResult, Mutation, StorageEngine};
pub use wal::format::{GroupFooter, MutationRecord, RecordType, SegmentHeader};
pub use wal::{Db, DurabilityMode, Identity, Wal, WalError, WalResult};

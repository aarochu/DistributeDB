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

pub mod checksum;
pub mod command;
pub mod fileio;
pub mod storage;

// Re-export the primary types for ergonomic use by the CLI and tests.
pub use checksum::{crc32c, crc64_ecma, Crc32c, Crc64Ecma};
pub use command::{parse, Command, ParseError};
pub use fileio::{
    FileSystem, FsError, FsResult, LockGuard, RealFs, SimConfig, SimFs, SplitMix64, SIM_PAGE_SIZE,
};
pub use storage::{GetResult, Mutation, StorageEngine};

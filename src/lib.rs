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

pub mod command;
pub mod storage;

// Re-export the primary types for ergonomic use by the CLI and tests.
pub use command::{parse, Command, ParseError};
pub use storage::{GetResult, Mutation, StorageEngine};

//! DistributeDB crate root.
//!
//! Phase 1 provides a local, single-threaded, in-memory key-value engine.
//! The module tree mirrors the layout in the Technical Design document:
//! `storage` holds the `StorageEngine` contract (Technical-Design §2.1) and
//! `command` holds the command types and parser for the key-value interface
//! (SOW §4.1: SET, GET, DELETE, EXISTS).
//!
//! Modules are scaffolded here and filled in by later Phase 1 features
//! (FEAT-002 storage engine, FEAT-003 command parser / CLI).

pub mod command;
pub mod storage;

//! Storage engine module.
//!
//! Implements the in-memory `StorageEngine` contract from Technical-Design
//! §2.1 (SOW R1). Phase 1 is single-threaded and in-memory; no WAL, LSN, or
//! networking yet (those arrive in later phases).
//!
//! The concrete engine implementation is added in FEAT-002. This scaffold
//! keeps the crate compiling with a clean module boundary.

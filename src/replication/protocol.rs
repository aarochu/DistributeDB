//! Bounded little-endian replication frames (Technical-Design §8.1).

use std::io::{self, Read, Write};

use crate::checksum::crc64_ecma;
use crate::wal::format::MutationRecord;

pub const VERSION: u8 = 1;
pub const MAX_BODY: usize = 1_048_576;
pub const MAX_SNAPSHOT_FRAME: usize = 262_144;
pub const MAX_DIAGNOSTIC: usize = 512;
pub const ERROR_CLUSTER_MISMATCH: u16 = 1;
pub const ERROR_DIVERGED: u16 = 2;
pub const ERROR_REBOOTSTRAP_REQUIRED: u16 = 3;
pub const ERROR_BAD_FRAME: u16 = 4;
pub const ERROR_UNAVAILABLE: u16 = 5;

#[derive(Debug)]
pub enum Error {
    Io(io::Error),
    Invalid(&'static str),
}

impl std::fmt::Display for Error {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        match self {
            Self::Io(error) => write!(f, "replication I/O: {error}"),
            Self::Invalid(reason) => write!(f, "invalid replication frame: {reason}"),
        }
    }
}

impl std::error::Error for Error {}

impl From<io::Error> for Error {
    fn from(error: io::Error) -> Self {
        Self::Io(error)
    }
}

pub type Result<T> = std::result::Result<T, Error>;

#[derive(Debug, Clone, PartialEq, Eq)]
pub enum Message {
    Hello {
        cluster_id: [u8; 16],
        replica_id: [u8; 16],
        durable_lsn: u64,
        record_hash: u64,
        wal_version: u16,
        snapshot_version: u16,
    },
    HelloAck {
        cluster_id: [u8; 16],
        primary_id: [u8; 16],
        durable_lsn: u64,
        record_hash: u64,
        earliest_retained_lsn: u64,
        wal_version: u16,
        snapshot_version: u16,
    },
    Record {
        record: MutationRecord,
        record_hash: u64,
    },
    Ack {
        durable_lsn: u64,
        applied_lsn: u64,
        record_hash: u64,
    },
    SnapshotOffer {
        snapshot_lsn: u64,
        record_hash: u64,
        snapshot_bytes: u64,
        snapshot_crc64: u64,
    },
    SnapshotChunk {
        snapshot_lsn: u64,
        offset: u64,
        bytes: Vec<u8>,
    },
    SnapshotDone {
        snapshot_lsn: u64,
        snapshot_crc64: u64,
    },
    Error {
        code: u16,
        diagnostic: String,
    },
    Heartbeat {
        durable_lsn: u64,
        send_monotonic_ns: u64,
    },
}

fn u16_at(bytes: &[u8], offset: usize) -> u16 {
    u16::from_le_bytes(bytes[offset..offset + 2].try_into().unwrap())
}
fn u32_at(bytes: &[u8], offset: usize) -> u32 {
    u32::from_le_bytes(bytes[offset..offset + 4].try_into().unwrap())
}
fn u64_at(bytes: &[u8], offset: usize) -> u64 {
    u64::from_le_bytes(bytes[offset..offset + 8].try_into().unwrap())
}
fn id_at(bytes: &[u8], offset: usize) -> [u8; 16] {
    bytes[offset..offset + 16].try_into().unwrap()
}

impl Message {
    fn kind(&self) -> u8 {
        match self {
            Self::Hello { .. } => 1,
            Self::HelloAck { .. } => 2,
            Self::Record { .. } => 3,
            Self::Ack { .. } => 4,
            Self::SnapshotOffer { .. } => 5,
            Self::SnapshotChunk { .. } => 6,
            Self::SnapshotDone { .. } => 7,
            Self::Error { .. } => 8,
            Self::Heartbeat { .. } => 9,
        }
    }

    pub fn encode(&self) -> Result<Vec<u8>> {
        let mut body = vec![VERSION, self.kind()];
        match self {
            Self::Hello {
                cluster_id,
                replica_id,
                durable_lsn,
                record_hash,
                wal_version,
                snapshot_version,
            } => {
                body.extend_from_slice(cluster_id);
                body.extend_from_slice(replica_id);
                body.extend_from_slice(&durable_lsn.to_le_bytes());
                body.extend_from_slice(&record_hash.to_le_bytes());
                body.extend_from_slice(&wal_version.to_le_bytes());
                body.extend_from_slice(&snapshot_version.to_le_bytes());
            }
            Self::HelloAck {
                cluster_id,
                primary_id,
                durable_lsn,
                record_hash,
                earliest_retained_lsn,
                wal_version,
                snapshot_version,
            } => {
                body.extend_from_slice(cluster_id);
                body.extend_from_slice(primary_id);
                body.extend_from_slice(&durable_lsn.to_le_bytes());
                body.extend_from_slice(&record_hash.to_le_bytes());
                body.extend_from_slice(&earliest_retained_lsn.to_le_bytes());
                body.extend_from_slice(&wal_version.to_le_bytes());
                body.extend_from_slice(&snapshot_version.to_le_bytes());
            }
            Self::Record {
                record,
                record_hash,
            } => {
                let encoded = record.encode();
                if crc64_ecma(&encoded) != *record_hash {
                    return Err(Error::Invalid("RECORD hash mismatch"));
                }
                body.extend_from_slice(&(encoded.len() as u32).to_le_bytes());
                body.extend_from_slice(&encoded);
                body.extend_from_slice(&record_hash.to_le_bytes());
            }
            Self::Ack {
                durable_lsn,
                applied_lsn,
                record_hash,
            } => {
                if applied_lsn > durable_lsn {
                    return Err(Error::Invalid("ACK applied LSN exceeds durable LSN"));
                }
                body.extend_from_slice(&durable_lsn.to_le_bytes());
                body.extend_from_slice(&applied_lsn.to_le_bytes());
                body.extend_from_slice(&record_hash.to_le_bytes());
            }
            Self::SnapshotOffer {
                snapshot_lsn,
                record_hash,
                snapshot_bytes,
                snapshot_crc64,
            } => {
                for value in [snapshot_lsn, record_hash, snapshot_bytes, snapshot_crc64] {
                    body.extend_from_slice(&value.to_le_bytes());
                }
            }
            Self::SnapshotChunk {
                snapshot_lsn,
                offset,
                bytes,
            } => {
                body.extend_from_slice(&snapshot_lsn.to_le_bytes());
                body.extend_from_slice(&offset.to_le_bytes());
                body.extend_from_slice(&(bytes.len() as u32).to_le_bytes());
                body.extend_from_slice(bytes);
            }
            Self::SnapshotDone {
                snapshot_lsn,
                snapshot_crc64,
            } => {
                body.extend_from_slice(&snapshot_lsn.to_le_bytes());
                body.extend_from_slice(&snapshot_crc64.to_le_bytes());
            }
            Self::Error { code, diagnostic } => {
                if diagnostic.len() > MAX_DIAGNOSTIC {
                    return Err(Error::Invalid("ERROR diagnostic too long"));
                }
                body.extend_from_slice(&code.to_le_bytes());
                body.extend_from_slice(&(diagnostic.len() as u16).to_le_bytes());
                body.extend_from_slice(diagnostic.as_bytes());
            }
            Self::Heartbeat {
                durable_lsn,
                send_monotonic_ns,
            } => {
                body.extend_from_slice(&durable_lsn.to_le_bytes());
                body.extend_from_slice(&send_monotonic_ns.to_le_bytes());
            }
        }
        if body.len() > MAX_BODY {
            return Err(Error::Invalid("frame body exceeds cap"));
        }
        if matches!(self, Self::SnapshotChunk { .. }) && body.len() + 4 > MAX_SNAPSHOT_FRAME {
            return Err(Error::Invalid("snapshot chunk exceeds cap"));
        }
        let mut frame = Vec::with_capacity(body.len() + 4);
        frame.extend_from_slice(&(body.len() as u32).to_le_bytes());
        frame.extend_from_slice(&body);
        Ok(frame)
    }
}

pub fn decode(body: &[u8]) -> Result<Message> {
    if body.len() < 2 || body.len() > MAX_BODY {
        return Err(Error::Invalid("frame body length out of range"));
    }
    if body[0] != VERSION {
        return Err(Error::Invalid("unsupported replication version"));
    }
    let payload = &body[2..];
    let exact = |length| {
        if payload.len() == length {
            Ok(())
        } else {
            Err(Error::Invalid("payload length mismatch"))
        }
    };
    match body[1] {
        1 => {
            exact(52)?;
            Ok(Message::Hello {
                cluster_id: id_at(payload, 0),
                replica_id: id_at(payload, 16),
                durable_lsn: u64_at(payload, 32),
                record_hash: u64_at(payload, 40),
                wal_version: u16_at(payload, 48),
                snapshot_version: u16_at(payload, 50),
            })
        }
        2 => {
            exact(60)?;
            Ok(Message::HelloAck {
                cluster_id: id_at(payload, 0),
                primary_id: id_at(payload, 16),
                durable_lsn: u64_at(payload, 32),
                record_hash: u64_at(payload, 40),
                earliest_retained_lsn: u64_at(payload, 48),
                wal_version: u16_at(payload, 56),
                snapshot_version: u16_at(payload, 58),
            })
        }
        3 => {
            if payload.len() < 45 {
                return Err(Error::Invalid("RECORD payload too short"));
            }
            let len = u32_at(payload, 0) as usize;
            if len > 1_000_000 || payload.len() != 4 + len + 8 {
                return Err(Error::Invalid("RECORD length mismatch"));
            }
            let encoded = &payload[4..4 + len];
            let decoded = MutationRecord::decode(encoded)
                .map_err(|_| Error::Invalid("invalid WAL record"))?;
            if decoded.consumed != len {
                return Err(Error::Invalid("RECORD has trailing bytes"));
            }
            let record_hash = u64_at(payload, 4 + len);
            if crc64_ecma(encoded) != record_hash {
                return Err(Error::Invalid("RECORD hash mismatch"));
            }
            Ok(Message::Record {
                record: decoded.record,
                record_hash,
            })
        }
        4 => {
            exact(24)?;
            let durable_lsn = u64_at(payload, 0);
            let applied_lsn = u64_at(payload, 8);
            if applied_lsn > durable_lsn {
                return Err(Error::Invalid("ACK applied LSN exceeds durable LSN"));
            }
            Ok(Message::Ack {
                durable_lsn,
                applied_lsn,
                record_hash: u64_at(payload, 16),
            })
        }
        5 => {
            exact(32)?;
            Ok(Message::SnapshotOffer {
                snapshot_lsn: u64_at(payload, 0),
                record_hash: u64_at(payload, 8),
                snapshot_bytes: u64_at(payload, 16),
                snapshot_crc64: u64_at(payload, 24),
            })
        }
        6 => {
            if body.len() + 4 > MAX_SNAPSHOT_FRAME || payload.len() < 20 {
                return Err(Error::Invalid("SNAPSHOT_CHUNK length out of range"));
            }
            let len = u32_at(payload, 16) as usize;
            if payload.len() != 20 + len {
                return Err(Error::Invalid("SNAPSHOT_CHUNK length mismatch"));
            }
            Ok(Message::SnapshotChunk {
                snapshot_lsn: u64_at(payload, 0),
                offset: u64_at(payload, 8),
                bytes: payload[20..].to_vec(),
            })
        }
        7 => {
            exact(16)?;
            Ok(Message::SnapshotDone {
                snapshot_lsn: u64_at(payload, 0),
                snapshot_crc64: u64_at(payload, 8),
            })
        }
        8 => {
            if payload.len() < 4 {
                return Err(Error::Invalid("ERROR payload too short"));
            }
            let len = u16_at(payload, 2) as usize;
            if len > MAX_DIAGNOSTIC || payload.len() != 4 + len {
                return Err(Error::Invalid("ERROR diagnostic length mismatch"));
            }
            let diagnostic = std::str::from_utf8(&payload[4..])
                .map_err(|_| Error::Invalid("ERROR diagnostic is not UTF-8"))?;
            Ok(Message::Error {
                code: u16_at(payload, 0),
                diagnostic: diagnostic.to_string(),
            })
        }
        9 => {
            exact(16)?;
            Ok(Message::Heartbeat {
                durable_lsn: u64_at(payload, 0),
                send_monotonic_ns: u64_at(payload, 8),
            })
        }
        _ => Err(Error::Invalid("unknown replication kind")),
    }
}

pub fn read_message<R: Read>(reader: &mut R) -> Result<Option<Message>> {
    let mut prefix = [0u8; 4];
    if reader.read(&mut prefix[..1])? == 0 {
        return Ok(None);
    }
    reader.read_exact(&mut prefix[1..])?;
    let length = u32::from_le_bytes(prefix) as usize;
    if !(2..=MAX_BODY).contains(&length) {
        return Err(Error::Invalid("frame length out of range"));
    }
    let mut body = vec![0u8; length];
    reader.read_exact(&mut body)?;
    decode(&body).map(Some)
}

pub fn write_message<W: Write>(writer: &mut W, message: &Message) -> Result<()> {
    writer.write_all(&message.encode()?)?;
    Ok(())
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::wal::format::RecordType;

    fn round_trip(message: Message) {
        let frame = message.encode().unwrap();
        assert_eq!(u32_at(&frame, 0) as usize, frame.len() - 4);
        assert_eq!(read_message(&mut frame.as_slice()).unwrap(), Some(message));
    }

    #[test]
    fn handshake_ack_and_record_round_trip() {
        let hello = Message::Hello {
            cluster_id: [1; 16],
            replica_id: [2; 16],
            durable_lsn: 3,
            record_hash: 4,
            wal_version: 1,
            snapshot_version: 1,
        };
        let frame = hello.encode().unwrap();
        assert_eq!(&frame[..6], &[54, 0, 0, 0, 1, 1]);
        assert_eq!(&frame[6..22], &[1; 16]);
        round_trip(hello);
        round_trip(Message::Ack {
            durable_lsn: 3,
            applied_lsn: 3,
            record_hash: 4,
        });
        let record = MutationRecord {
            lsn: 1,
            rtype: RecordType::Set,
            key: b"k".to_vec(),
            value: b"v".to_vec(),
            prev_hash: 0,
        };
        round_trip(Message::Record {
            record_hash: record.record_hash(),
            record,
        });
    }

    #[test]
    fn snapshot_messages_round_trip_and_size_bound() {
        round_trip(Message::SnapshotOffer {
            snapshot_lsn: 9,
            record_hash: 10,
            snapshot_bytes: 100,
            snapshot_crc64: 11,
        });
        round_trip(Message::SnapshotChunk {
            snapshot_lsn: 9,
            offset: 0,
            bytes: b"chunk".to_vec(),
        });
        round_trip(Message::SnapshotDone {
            snapshot_lsn: 9,
            snapshot_crc64: 11,
        });
        assert!(Message::SnapshotChunk {
            snapshot_lsn: 1,
            offset: 0,
            bytes: vec![0; MAX_SNAPSHOT_FRAME]
        }
        .encode()
        .is_err());
    }

    #[test]
    fn malformed_frames_fail_closed() {
        assert!(read_message(&mut [255, 255, 255, 255].as_slice()).is_err());
        let mut frame = Message::Ack {
            durable_lsn: 1,
            applied_lsn: 1,
            record_hash: 0,
        }
        .encode()
        .unwrap();
        frame[14] = 2;
        assert!(read_message(&mut frame.as_slice()).is_err());
        assert!(Message::Error {
            code: ERROR_BAD_FRAME,
            diagnostic: "x".repeat(MAX_DIAGNOSTIC + 1)
        }
        .encode()
        .is_err());
    }
}

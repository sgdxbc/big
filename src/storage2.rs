use std::fmt::Debug;

use bincode::{Decode, Encode};
use bytes::Bytes;
use sha2::{Digest, Sha256};
use tokio::sync::oneshot;

pub mod plain;
pub mod sched;

#[derive(Clone, Copy, PartialEq, Eq, Hash, PartialOrd, Ord, Encode, Decode)]
pub struct StorageKey([u8; 32]);

impl StorageKey {
    pub fn digest(data: impl AsRef<[u8]>) -> Self {
        Self(Sha256::digest(data).into())
    }

    fn to_hex(self) -> String {
        self.0.iter().map(|b| format!("{b:02x}")).collect()
    }
}

impl Debug for StorageKey {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        write!(f, "StorageKey(0x{}...)", &self.to_hex()[..8])
    }
}

impl AsRef<[u8]> for StorageKey {
    fn as_ref(&self) -> &[u8] {
        &self.0
    }
}

pub enum StorageSchedOp {
    FetchAhead(StorageKey, StateVersion, oneshot::Sender<Option<Bytes>>),
    Bump(BumpUpdates),
}

type StateVersion = u64;

pub type BumpUpdates = Vec<(StorageKey, Option<Bytes>)>;

pub enum StorageOp {
    Fetch(StorageKey, oneshot::Sender<Option<Bytes>>),
    Bump(BumpUpdates),
}

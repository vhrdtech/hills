use crate::record::RecordMeta;
use hills_base::{GenericKey, SimpleVersion};
use rkyv::{Archive, Deserialize, Serialize};
use std::collections::HashMap;
use std::fmt::{Debug, Display, Formatter};
use std::net::SocketAddr;
use std::ops::Range;
use uuid::Uuid;

#[derive(Archive, Debug, Serialize, Deserialize)]
#[archive(check_bytes)]
#[archive_attr(derive(Debug))]
pub struct RecordHotChange {
    pub tree: String,
    pub key: GenericKey,
    pub meta_iteration: u32,
    pub data_iteration: u32,
    pub kind: ChangeKind,
}

#[derive(Archive, Clone, Debug, Serialize, Deserialize)]
#[archive(check_bytes)]
#[archive_attr(derive(Debug))]
pub enum ChangeKind {
    ModifyMeta,
    CreateOrChange,
    Remove,
}

impl From<&ArchivedHotSyncEventKind> for ChangeKind {
    fn from(value: &ArchivedHotSyncEventKind) -> Self {
        match value {
            ArchivedHotSyncEventKind::MetaChanged { .. } => ChangeKind::ModifyMeta,
            ArchivedHotSyncEventKind::CreatedOrChanged { .. } => ChangeKind::CreateOrChange,
            ArchivedHotSyncEventKind::Removed => ChangeKind::Remove,
        }
    }
}

// TODO: Switch to serde with &[u8] support to avoid copying data buffer many times?
#[derive(Archive, Clone, Serialize, Deserialize)]
#[archive(check_bytes)]
// #[archive_attr(derive(Debug))]
pub enum Event {
    PresentSelf {
        uuid: [u8; 16],
        readable_name: String,
    },

    GetTreeOverview {
        tree: String,
    },
    TreeOverview {
        tree: String,
        records: HashMap<GenericKey, RecordIteration>,
    },
    RequestRecords {
        tree: String,
        keys: Vec<GenericKey>,
    },
    HotSyncEvent(HotSyncEvent),

    GetKeySet {
        tree: String,
    },
    KeySet {
        tree: String,
        keys: Range<u32>,
    },

    CheckOut {
        tree: String,
        keys: Vec<GenericKey>,
    },
    Return {
        tree: String,
        keys: Vec<GenericKey>,
    },
    CheckedOut {
        tree: String,
        key: GenericKey,
        queue: Vec<[u8; 16]>,
    },
}

#[derive(Archive, Clone, Serialize, Deserialize)]
#[archive(check_bytes)]
pub struct HotSyncEvent {
    pub tree_name: String,
    pub key: GenericKey,
    pub source_addr: Option<SocketAddr>,
    pub kind: HotSyncEventKind,
}

#[derive(Archive, Clone, Serialize, Deserialize)]
#[archive(check_bytes)]
pub enum HotSyncEventKind {
    MetaChanged {
        meta: RecordMeta,
        meta_iteration: u32,
    },
    CreatedOrChanged {
        meta: RecordMeta,
        meta_iteration: u32,
        data: Vec<u8>,
        data_evolution: SimpleVersion,
        data_iteration: u32,
    },
    Removed,
}

#[derive(Archive, Clone, Serialize, Deserialize)]
#[archive(check_bytes)]
pub struct RecordIteration {
    pub meta_iteration: u32,
    pub data_iteration: u32,
}

#[derive(Default)]
pub(crate) struct RecordBorrows {
    /// tree name -> key -> queue of clients
    pub(crate) borrows: HashMap<String, HashMap<GenericKey, Vec<Uuid>>>,
}

impl Display for RecordIteration {
    fn fmt(&self, f: &mut Formatter<'_>) -> std::fmt::Result {
        write!(
            f,
            "it{{m:{} d:{}}}",
            self.meta_iteration, self.data_iteration
        )
    }
}

impl Debug for RecordIteration {
    fn fmt(&self, f: &mut Formatter<'_>) -> std::fmt::Result {
        write!(f, "{self}")
    }
}

impl Debug for ArchivedRecordIteration {
    fn fmt(&self, f: &mut Formatter<'_>) -> std::fmt::Result {
        write!(
            f,
            "{}",
            RecordIteration {
                meta_iteration: self.meta_iteration,
                data_iteration: self.data_iteration,
            }
        )
    }
}

impl Display for ArchivedHotSyncEventKind {
    fn fmt(&self, f: &mut Formatter<'_>) -> std::fmt::Result {
        match self {
            ArchivedHotSyncEventKind::MetaChanged { meta_iteration, .. } => {
                write!(f, "Meta changed m{meta_iteration}")
            }
            ArchivedHotSyncEventKind::CreatedOrChanged {
                meta_iteration,
                data_iteration,
                ..
            } => write!(f, "CreatedOrChanged m{meta_iteration} d{data_iteration}"),
            ArchivedHotSyncEventKind::Removed => write!(f, "Removed"),
        }
    }
}

use crate::record::RecordMeta;
use hills_base::GenericKey;
use rkyv::{AlignedVec, Archive, Deserialize, Serialize};
use std::collections::HashMap;
use std::ops::Range;

// pub enum NodeKind {
//     /// Node gives out mutable locks, accepts changes and serves data for other nodes.
//     Server,
//     /// Node talks to a server, produces new data and requests existing one when need be.
//     Client,
//     /// Node only receiving data from a server and storing it.
//     Backup,
//     /// Node that fully owns database file, no need for borrowing entries for editing.
//     StandAlone,
// }

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

#[derive(Archive, Debug, Serialize, Deserialize)]
#[archive(check_bytes)]
#[archive_attr(derive(Debug))]
pub enum ChangeKind {
    Create,
    ModifyMeta,
    ModifyBoth,
    Remove,
}

// TODO: Switch to serde with &[u8] support to avoid copying data buffer many times?
#[derive(Archive, Clone, Serialize, Deserialize)]
// #[archive(check_bytes)]
// #[archive_attr(derive(Debug))]
pub enum Event {
    PresentSelf {
        uuid: [u8; 16],
    },
    Subscribe {
        trees: Vec<String>,
    },

    GetTreeOverview {
        tree: String,
    },
    TreeOverview {
        tree: String,
        records: HashMap<GenericKey, RecordIteration>,
    },

    RecordCreated {
        tree: String,
        key: GenericKey,
        meta: RecordMeta,
        data: AlignedVec,
    },
    RecordMetaChanged {
        tree: String,
        key: GenericKey,
        meta: RecordMeta,
        meta_iteration: u32,
    },
    RecordChanged {
        tree: String,
        key: GenericKey,
        meta: RecordMeta,
        meta_iteration: u32,
        data: AlignedVec,
        data_iteration: u32,
    },
    RecordRemoved {
        tree: String,
        key: GenericKey,
    },

    RequestRecords {
        tree: String,
        keys: Vec<GenericKey>,
    },
    RecordAsRequested {
        tree_name: String,
        key: GenericKey,
        record: AlignedVec,
    },

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
        key: Vec<GenericKey>,
    },
    CheckedOut {
        tree: String,
        keys: Vec<GenericKey>,
    },
    AlreadyCheckedOut {
        tree: String,
        keys: Vec<GenericKey>,
        by_node: [u8; 16],
    },
}

#[derive(Archive, Clone, Debug, Serialize, Deserialize)]
#[archive(check_bytes)]
#[archive_attr(derive(Debug))]
pub struct RecordIteration {
    pub meta_iteration: u32,
    pub data_iteration: u32,
}

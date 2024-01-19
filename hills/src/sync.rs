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

#[derive(Archive, Debug, Serialize, Deserialize)]
#[archive(check_bytes)]
#[archive_attr(derive(Debug))]
pub enum ClientEvent {
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
    RecordChanged {
        change: RecordHotChange,
        meta: AlignedVec,
        data: Option<AlignedVec>,
    },

    GetKeySet {
        tree: String,
    },

    CheckOut {
        tree: String,
        keys: Vec<GenericKey>,
    },
    Return {
        tree: String,
        key: Vec<GenericKey>,
    },
}

#[derive(Archive, Debug, Serialize, Deserialize)]
#[archive(check_bytes)]
#[archive_attr(derive(Debug))]
pub enum ServerEvent {
    PresentSelf {
        uuid: [u8; 16],
    },

    GetTreeOverview {
        tree: String,
    },
    TreeOverview {
        tree: String,
        records: HashMap<GenericKey, RecordIteration>,
    },
    RecordChanged {
        change: RecordHotChange,
        meta: AlignedVec,
        data: Option<AlignedVec>,
    },

    KeySet {
        keys: Range<u32>,
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

#[derive(Archive, Debug, Serialize, Deserialize)]
#[archive(check_bytes)]
#[archive_attr(derive(Debug))]
pub struct RecordIteration {
    meta_iteration: u32,
    data_iteration: u32,
}

use hills_base::GenericKey;
use std::collections::HashMap;

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

#[derive(Debug)]
pub struct RecordHotChange {
    pub tree: String,
    pub key: GenericKey,
    pub kind: ChangeKind,
}

#[derive(Debug)]
pub enum ChangeKind {
    Create,
    ModifyMeta,
    ModifyData,
    Remove,
}

#[derive(Debug)]
pub struct TreeOverview {
    records: HashMap<GenericKey, RecordIteration>,
}

#[derive(Debug)]
pub struct RecordIteration {
    meta_iteration: u32,
    data_iteration: u32,
}

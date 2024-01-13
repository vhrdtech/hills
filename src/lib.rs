use rkyv::{Archive, Deserialize, Serialize};

pub mod record;
pub mod tree;
mod sync;
mod journal;

/// Record id inside a tree
#[derive(Archive, Debug, Serialize, Deserialize)]
#[archive(check_bytes)]
#[archive_attr(derive(Debug))]
pub enum RecordId {
    /// When offline, nodes can create new entries with temporary ids, when online global id can
    /// be acquired from the server and AssignGlobalId action will change them.
    Temporary(u32),
    /// Global id across all nodes, so that relations can be created using such ids.
    Global(u32)
}

/// Record state
#[derive(Archive, Debug, Serialize, Deserialize)]
#[archive(check_bytes)]
#[archive_attr(derive(Debug))]
pub enum RecordState {
    /// Record can be edited many times, only latest data is kept and synchronised between nodes
    Draft,
    /// Record is released and it's data cannot be changed.
    /// u32 can be used for other user states (Approved, Obsolete, etc), that can be changed.
    Released(u32),
}

#[derive(Archive, PartialEq, Eq, Debug, Hash, Serialize, Deserialize)]
#[archive(check_bytes)]
#[archive_attr(derive(PartialEq, Eq, Debug, Hash))]
pub struct SimpleVersion {
    /// Backwards compatibility breaking
    pub major: u16,
    /// Backwards and Future compatible changes
    pub minor: u16,
}

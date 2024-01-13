use chrono::{DateTime, Utc};
use rkyv::{Archive, Deserialize, Serialize};

/// Tree key, uniquely identifying a particular record's revision.
/// Stays fixed after creation.
#[derive(Archive, Serialize, Deserialize)]
pub struct Key {
    /// Unique ID of an entry inside a Tree.
    pub id: RecordId,
    /// Each record starts with revision 0 with RecordState::Draft.
    /// Record's data can be modified while in Draft, then it stays fixed.
    pub revision: u32,
}

/// Tree record holding modification dates, who is currently editing an entry or not, and data itself.
#[derive(Archive, Serialize, Deserialize)]
pub struct Record {
    /// Same ID as in key
    id: RecordId,
    /// Same revision as in key
    revision: u32,

    /// Whether some node is holding an entry mutable or not.
    /// If node id is not self, then no modifications should be done to an entry.
    /// Server must reject modified entries from a node if they weren't first checked out by the same node.
    held_mut_by: Option<NodeId>,

    state: RecordState,
    modified: DateTime<Utc>,
    created: DateTime<Utc>,
    /// Additional metadata accessible by user.
    meta: Vec<u8>,

    /// Rust version of the program, that serialized the data.
    rust_version: SimpleVersion,
    /// rkyv version that was used to serialize the data.
    rkyv_version: SimpleVersion,
    /// User program version used to serialize the data.
    evolution: SimpleVersion,

    /// None when only id was created, but no data has been written yet.
    data: Option<Vec<u8>>,
}

#[derive(Archive, Serialize, Deserialize)]
pub struct NodeId(u64);

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

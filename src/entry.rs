use chrono::{DateTime, Utc};
use crate::SimpleVersion;

pub struct Key {
    /// Unique ID of an entry inside a Tree.
    pub id: u64,
    pub revision: Revision,
}

pub struct Entry {
    /// Same ID as in key
    id: u64,
    /// Same revision as in key
    revision: Revision,
    /// Whether some node is holding an entry mutable or not.
    /// If node id is not self, then no modifications should be done to an entry.
    /// Server must reject modified entries from a node if they weren't first checked out by the same node.
    held_mut_by: Option<NodeId>,

    modified: DateTime<Utc>,
    created: DateTime<Utc>,

    /// Rust version of the program, that serialized the data.
    rust_version: SimpleVersion,
    /// rkyv version that was used to serialize the data.
    rkyv_version: SimpleVersion,
    /// User program version used to serialize the data.
    evolution: SimpleVersion,

    data: Vec<u8>,
}

pub enum Revision {
    Staging,
    Released(u32)
}

pub struct NodeId(u64);

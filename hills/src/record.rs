use chrono::{DateTime, Utc};
use hills_base::{GenericKey, SimpleVersion};
use rkyv::{AlignedVec, Archive, Deserialize, Serialize};

// #[derive(Archive, Serialize, Deserialize)]
// pub struct NodeId(u64);

// Whether some node is holding an entry mutable or not.
// If node id is not self, then no modifications should be done to an entry.
// Server must reject modified entries from a node if they weren't first checked out by the same node.
// held_mut_by: Option<NodeId>,

/// Tree record holding meta information, dates and data itself.
#[derive(Archive, Serialize, Deserialize)]
// #[archive(check_bytes)]
// #[archive_attr(derive(Debug))]
pub struct Record {
    /// Same ID as in key
    pub key: GenericKey,

    /// 0 when first created, must be incremented each time any field of a Record is modified.
    /// Used in synchronisation to determine which Record is newer.
    /// DateTime is not used to avoid dealing with incorrect clock on nodes.
    pub iteration: u32,

    /// Versioning information for this record.
    /// All records in a tree are either NonVersioned or in Draft/Released(n) state.
    pub version: Version,

    pub modified_by: String,
    pub modified: DateTime<Utc>,
    pub created: DateTime<Utc>,
    // Additional metadata accessible by user.
    // pub meta: Option<AlignedVec>,
    /// Rust version of the program, that serialized the data.
    pub rust_version: SimpleVersion,
    /// rkyv version that was used to serialize the data.
    pub rkyv_version: SimpleVersion,
    /// User program version used to serialize the data.
    pub evolution: SimpleVersion,

    /// None when only id was created, but no data has been written yet.
    pub data: Option<AlignedVec>,
}

/// Record state
#[derive(Archive, Debug, Serialize, Deserialize)]
#[archive(check_bytes)]
#[archive_attr(derive(Debug))]
pub enum Version {
    NonVersioned,
    /// Record can be edited many times, only latest data is kept and synchronised between nodes
    Draft,
    /// Record is released and it's data cannot be changed.
    /// u32 can be used for other user states (Approved, Obsolete, etc), that can be changed.
    Released(u32),
}

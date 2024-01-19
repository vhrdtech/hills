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
// #[archive(check_bytes)] DateTime prevents deriving this
// #[archive_attr(derive(Debug))]
pub struct Record {
    /// 0 when first created, must be incremented each time any field of a RecordMeta is modified.
    /// Used in synchronisation to determine which Record is newer.
    /// DateTime is not used to avoid dealing with incorrect clock on nodes.
    pub meta_iteration: u32,
    pub meta: RecordMeta,

    /// 0 when first created, must be incremented each time data is changed.
    pub data_iteration: u32,
    /// None when only id was created, but no data has been written yet.
    pub data: AlignedVec,
}

#[derive(Archive, Serialize, Deserialize)]
pub struct RecordMeta {
    /// Same ID as in a Record's key
    pub key: GenericKey,

    /// Versioning information for this record.
    /// All records in a tree are either NonVersioned or in Draft/Released(n) state.
    pub version: Version,

    /// Username
    pub modified_by: String,
    /// Node UUID
    pub modified_on: [u8; 16],
    /// Last time meta or data were changed.
    pub modified: DateTime<Utc>,
    /// Time when Record was created.
    pub created: DateTime<Utc>,
    // Additional metadata accessible by user.
    // pub meta: Option<AlignedVec>,
    /// Rust version of the program, that serialized the data.
    pub rust_version: SimpleVersion,
    /// rkyv version that was used to serialize the data.
    pub rkyv_version: SimpleVersion,
    /// User program version used to serialize the data.
    pub evolution: SimpleVersion,
}

/// Record state
#[derive(Archive, Debug, Serialize, Deserialize)]
#[archive(check_bytes)]
#[archive_attr(derive(Debug))]
pub enum Version {
    /// Record can be edited many times, only latest data is kept and synchronised between nodes
    NonVersioned,
    /// Version controlled Record, not yet released and can be modified multiple times. Next revision cannot be created.
    Draft,
    /// Record is released and it's data cannot be changed.
    /// u32 can be used for other user states (Approved, Obsolete, etc), that can be changed.
    Released(u32),
}

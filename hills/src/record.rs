use hills_base::{GenericKey, SimpleVersion, UtcDateTime};
use rkyv::{AlignedVec, Archive, Deserialize, Serialize};

/// Tree record holding meta information, record iteration and data itself.
#[derive(Archive, Serialize, Deserialize)]
#[archive(check_bytes)]
// #[archive_attr(derive(Debug))]
pub struct Record {
    /// 0 when first created, must be incremented each time any field of a RecordMeta is modified.
    /// Used in synchronisation to determine which Record is newer.
    /// DateTime is not used to avoid dealing with incorrect clock on nodes.
    pub meta_iteration: u32,
    pub meta: RecordMeta,

    /// 0 when first created, must be incremented each time data is changed.
    pub data_iteration: u32,
    /// Data type version used to serialize the data.
    pub data_evolution: SimpleVersion,
    /// Versioned(T) user data in rkyv format.
    pub data: AlignedVec,
}

#[derive(Archive, Clone, Serialize, Deserialize)]
#[archive(check_bytes)]
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
    pub modified: UtcDateTime,
    /// Time when Record was created.
    pub created: UtcDateTime,
    // Additional metadata accessible by user.
    // pub meta: Option<AlignedVec>,
    // Rust version of the program, that serialized the data.
    // pub rust_version: SimpleVersion,
    /// rkyv version that was used to serialize the data.
    pub rkyv_version: SimpleVersion,
}

/// Record state
#[derive(Archive, Clone, Debug, Serialize, Deserialize)]
#[archive(check_bytes)]
#[archive_attr(derive(Debug))]
pub enum Version {
    /// Record can be edited many times, only latest data is kept and synchronised between nodes
    NonVersioned,
    /// Version controlled Record, not yet released and can be modified multiple times. Next revision cannot be created.
    ///  Number can be used for other Draft user states.
    Draft(u32),
    /// Record is released and it's data cannot be changed.
    /// Number can be used for other Released user states (Approved, Obsolete, ..).
    Released(u32),
}

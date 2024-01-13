use std::collections::HashMap;
use rkyv::{Archive, Deserialize, Serialize};
use crate::record::SimpleVersion;

#[derive(Archive, Debug, Serialize, Deserialize)]
#[archive(check_bytes)]
#[archive_attr(derive(Debug))]
pub struct TreeDescriptor {
    pub description: String,
    /// Type definition for each evolution still supported.
    /// Keep for reference.
    pub ts: HashMap<SimpleVersion, String>,
}
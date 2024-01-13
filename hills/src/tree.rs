use std::collections::HashMap;
use rkyv::{Archive, Deserialize, Serialize};
use hills_base::TypeCollection;
use crate::record::SimpleVersion;

#[derive(Archive, Debug, Serialize, Deserialize)]
#[archive(check_bytes)]
#[archive_attr(derive(Debug))]
pub struct TreeDescriptor {
    pub next_temporary_id: u32,
    pub next_global_id: Option<u32>,
    
    pub description: String,
    /// Type definition for each evolution still supported.
    /// Checked when opening a tree.
    pub ts: HashMap<SimpleVersion, TypeCollection>,
}
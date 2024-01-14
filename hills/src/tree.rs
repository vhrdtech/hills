use crate::record::SimpleVersion;
use hills_base::TypeCollection;
use rkyv::{Archive, Deserialize, Serialize};
use std::collections::HashMap;

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

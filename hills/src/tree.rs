use hills_base::SimpleVersion;
use hills_base::TypeCollection;
use rkyv::{Archive, Deserialize, Serialize};
use std::collections::HashMap;

#[derive(Archive, Debug, Serialize, Deserialize)]
#[archive(check_bytes)]
#[archive_attr(derive(Debug))]
pub struct TreeDescriptor {
    /// Type definition for each evolution still supported.
    /// Checked when opening a tree.
    pub evolutions: HashMap<SimpleVersion, TypeCollection>,
    /// Whether Record's created in a tree will be NonVersioned or Draft
    pub versioning: bool,
}

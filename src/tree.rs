use std::collections::HashMap;
use crate::SimpleVersion;

pub struct TreeDescriptor {
    pub description: String,
    /// Type definition for each evolution still supported.
    /// Keep for reference.
    pub ts: HashMap<SimpleVersion, String>,
}
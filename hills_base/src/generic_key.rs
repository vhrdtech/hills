use rkyv::{Archive, Deserialize, Serialize};

pub trait TreeKey {
    fn tree_name() -> &'static str;
    fn new(key: GenericKey) -> Self;
    fn to_generic(&self) -> GenericKey;
}

#[derive(Copy, Clone, Debug, Archive, Serialize, Deserialize)]
#[archive(check_bytes)]
#[archive_attr(derive(Debug))]
pub struct GenericKey {
    pub id: u32,
    pub revision: u32,
}

impl GenericKey {
    pub fn new(id: u32, revision: u32) -> Self {
        GenericKey { id, revision }
    }

    pub fn previous_revision(&self) -> Option<Self> {
        if self.revision > 0 {
            Some(GenericKey {
                id: self.id,
                revision: self.revision - 1,
            })
        } else {
            None
        }
    }
}

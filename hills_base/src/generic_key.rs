use rkyv::{Archive, Deserialize, Serialize};
use std::fmt::{Debug, Display, Formatter};

pub trait TreeKey {
    fn tree_name() -> &'static str;
    fn from_generic(key: GenericKey) -> Self;
    fn to_generic(&self) -> GenericKey;
}

#[derive(Copy, Clone, Hash, PartialEq, Eq, Archive, Serialize, Deserialize)]
#[archive(check_bytes)]
#[archive_attr(derive(Hash, PartialEq, Eq))]
pub struct GenericKey {
    pub id: u32,
    pub revision: u32,
}

impl GenericKey {
    pub fn new(id: u32, revision: u32) -> Self {
        GenericKey { id, revision }
    }

    pub fn from_archived(a: &ArchivedGenericKey) -> Self {
        GenericKey {
            id: a.id,
            revision: a.revision,
        }
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

    pub fn to_bytes(&self) -> [u8; 8] {
        let mut bytes = [0u8; 8];
        bytes[0..=3].copy_from_slice(&self.id.to_be_bytes());
        bytes[4..=7].copy_from_slice(&self.revision.to_be_bytes());
        bytes
    }

    pub fn from_bytes(bytes: &[u8]) -> Option<Self> {
        if bytes.len() != 8 {
            return None;
        }
        let mut word = [0u8; 4];
        word.copy_from_slice(&bytes[0..=3]);
        let id = u32::from_be_bytes(word);
        word.copy_from_slice(&bytes[4..=7]);
        let revision = u32::from_be_bytes(word);
        Some(GenericKey { id, revision })
    }
}

impl Display for GenericKey {
    fn fmt(&self, f: &mut Formatter<'_>) -> std::fmt::Result {
        write!(f, "{}.{}", self.id, self.revision)
    }
}

impl Debug for GenericKey {
    fn fmt(&self, f: &mut Formatter<'_>) -> std::fmt::Result {
        write!(f, "{self}")
    }
}

impl Debug for ArchivedGenericKey {
    fn fmt(&self, f: &mut Formatter<'_>) -> std::fmt::Result {
        write!(f, "{}.{}", self.id, self.revision)
    }
}

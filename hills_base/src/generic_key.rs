use zerocopy::big_endian::U32;
use zerocopy::{AsBytes, FromBytes, FromZeroes};

pub trait TreeKey {
    fn tree_name() -> &'static str;
    fn new(key: GenericKey) -> Self;
    fn to_generic(&self) -> GenericKey;
}

#[derive(Debug, FromBytes, AsBytes, FromZeroes)]
#[repr(C)]
pub struct GenericKey {
    pub id: U32,
    pub revision: U32,
}

impl GenericKey {
    pub fn new(id: u32, revision: u32) -> Self {
        GenericKey {
            id: id.into(),
            revision: revision.into(),
        }
    }

    pub fn previous_revision(&self) -> Option<Self> {
        if self.revision.get() > 0 {
            Some(GenericKey {
                id: self.id,
                revision: (self.revision.get() - 1).into(),
            })
        } else {
            None
        }
    }
}

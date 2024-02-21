use dyn_clone::DynClone;
use hills_base::{GenericKey, SimpleVersion};
use rkyv::{check_archived_root, Deserialize};
use sled::Tree;

use crate::{consts::KEY_POOL, db::Error, record::Record};

mod latest_revisions;
pub mod named;

pub enum Action {
    Insert,
    Update,
    Remove,
}

pub struct TypeErasedTree<'a> {
    pub(crate) tree: &'a Tree,
    pub(crate) evolution: SimpleVersion,
}

pub trait TreeIndex: DynClone {
    fn rebuild(&mut self, tree: TypeErasedTree) -> Result<(), Error>;

    fn update(
        &mut self,
        tree: TypeErasedTree,
        key: GenericKey,
        data: &[u8],
        action: Action,
    ) -> Result<(), Error>;
}

dyn_clone::clone_trait_object!(TreeIndex);

impl<'a> TypeErasedTree<'a> {
    pub fn all_revisions(&self) -> impl Iterator<Item = GenericKey> {
        self.tree.iter().keys().filter_map(|key| {
            if let Ok(key) = key {
                if key == KEY_POOL {
                    return None;
                }
                if key.len() != 8 {
                    return None;
                }
                let key = GenericKey::from_bytes(&key)?;
                Some(key)
            } else {
                None
            }
        })
    }

    pub fn get_with<T, F: FnMut(&[u8]) -> T>(&self, key: GenericKey, mut f: F) -> Result<T, Error> {
        let key_bytes = key.to_bytes();
        let value = self.tree.get(key_bytes)?;
        match value {
            Some(bytes) => {
                let archived_record = check_archived_root::<Record>(&bytes)?;
                let record_evolution: SimpleVersion = archived_record
                    .data_evolution
                    .deserialize(&mut rkyv::Infallible)
                    .expect("");
                if record_evolution != self.evolution {
                    return Err(Error::EvolutionMismatch(format!(
                        "record evolution is {record_evolution} and code is {}",
                        self.evolution
                    )));
                }

                let data = archived_record.data.as_slice();
                Ok(f(data))
            }
            None => Err(Error::RecordNotFound),
        }
    }
}

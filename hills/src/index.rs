use dyn_clone::DynClone;
use hills_base::{GenericKey, SimpleVersion};
use rkyv::{check_archived_root, Deserialize};
use sled::Tree;

use crate::{consts::KEY_POOL, db::Error, record::Record};

mod latest_revisions;
pub mod multi_named;
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

pub trait TreeSearch {
    type Key;

    /// Lookup records with a query and return their keys and textual representation.
    fn search(&self, query: impl AsRef<str>) -> Vec<SearchHit<Self::Key>>;

    /// Return name and description for an already known key.
    fn name_desc(&self, key: Self::Key) -> Result<(String, String), Error>;
}

#[derive(Clone, serde::Serialize, serde::Deserialize)]
pub struct SearchHit<K> {
    /// Key in the target tree
    pub key: K,
    /// Name as is from the tree record
    pub name: String,
    /// Name and optional additional information to show during search process
    pub description: String,
    pub similarity: Similarity,
}

#[derive(Copy, Clone, Eq, PartialEq, serde::Serialize, serde::Deserialize)]
pub enum Similarity {
    Exact,
    Loose,
}

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

#[derive(Clone)]
pub(crate) struct StringPostProcess {
    pub(crate) case_sensitive: bool,
    pub(crate) ignore_chars: Vec<char>,
    pub(crate) trim_whitespace: bool,
}

impl StringPostProcess {
    fn post_process(&self, s: impl AsRef<str>) -> String {
        let s = if self.case_sensitive {
            s.as_ref().to_string()
        } else {
            s.as_ref().to_lowercase()
        };
        let s = if self.trim_whitespace {
            s.trim()
        } else {
            s.as_str()
        };
        s.chars()
            .into_iter()
            .filter(|c| !self.ignore_chars.contains(&c))
            .collect()
    }
}

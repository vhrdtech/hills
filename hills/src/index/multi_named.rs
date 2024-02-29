use std::{
    collections::BTreeMap,
    marker::PhantomData,
    sync::{Arc, RwLock},
};

use hills_base::{index::IndexError, GenericKey, TreeKey};

use crate::db::Error;

use super::{Action, StringPostProcess, TreeIndex, TypeErasedTree};

type ExtractStrFn = fn(data: &[u8]) -> Result<Vec<String>, IndexError>;

/// Index that maps multiple unique names to the same record key.
/// Optionally some characters or case could be ignored and whitespace trimmed.
#[derive(Clone)]
pub struct MultiNamedIndex<K> {
    storage: Arc<RwLock<Storage>>,
    extractor: ExtractStrFn,
    settings: StringPostProcess,
    _phantom: PhantomData<K>,
}

#[derive(Default)]
struct Storage {
    index: BTreeMap<String, GenericKey>,
}

#[derive(Clone)]
struct MultiNamedIndexer {
    storage: Arc<RwLock<Storage>>,
    extractor: ExtractStrFn,
    settings: StringPostProcess,
}

impl TreeIndex for MultiNamedIndexer {
    fn rebuild(&mut self, tree: TypeErasedTree) -> Result<(), Error> {
        let Ok(mut wr) = self.storage.write() else {
            return Err(Error::Index(IndexError::RwLock));
        };
        wr.index.clear();
        for key in tree.all_revisions() {
            let names = tree.get_with(key, |data| (self.extractor)(data))??;
            for name in names {
                let name = self.settings.post_process(name);
                if wr.index.contains_key(&name) {
                    return Err(Error::Index(IndexError::Duplicate(name)));
                }
                wr.index.insert(name, key);
            }
        }
        log::debug!("MultiNamed index rebuilt: {:?}", wr.index);
        Ok(())
    }

    fn update(
        &mut self,
        _tree: TypeErasedTree,
        key: GenericKey,
        data: &[u8],
        action: Action,
    ) -> Result<(), Error> {
        let Ok(mut wr) = self.storage.write() else {
            return Err(Error::Index(IndexError::RwLock));
        };
        match action {
            Action::Insert => {
                let names = (self.extractor)(data)?;
                for name in names {
                    let name = self.settings.post_process(name);
                    if wr.index.contains_key(&name) {
                        return Err(Error::Index(IndexError::Duplicate(name)));
                    }
                    wr.index.insert(name, key);
                }
            }
            Action::Update => {
                let old_names: Vec<String> = wr
                    .index
                    .iter()
                    .filter(|(_, v)| **v == key)
                    .map(|(k, _)| k.to_string())
                    .collect();
                let new_names = (self.extractor)(data)?;
                let new_names: Vec<String> = new_names
                    .into_iter()
                    .map(|s| self.settings.post_process(s))
                    .collect();
                for new_name in &new_names {
                    if let Some(k) = wr.index.get(new_name) {
                        if *k != key {
                            return Err(Error::Index(IndexError::Duplicate(new_name.to_string())));
                        }
                    }
                }
                for old_name in old_names.iter().filter(|s| !new_names.contains(s)) {
                    wr.index.remove(old_name);
                }
                for new_name in new_names {
                    wr.index.insert(new_name, key);
                }
            }
            Action::Remove => {
                let names = (self.extractor)(data)?;
                for name in names {
                    let name = self.settings.post_process(name);
                    wr.index.remove(&name);
                }
            }
        }
        log::debug!("MultiNamed {:?}", wr.index);
        Ok(())
    }
}

impl<K: TreeKey> MultiNamedIndex<K> {
    pub fn new(exctractor: ExtractStrFn) -> Self {
        MultiNamedIndex {
            storage: Arc::new(RwLock::new(Storage::default())),
            extractor: exctractor,
            settings: StringPostProcess {
                case_sensitive: true,
                ignore_chars: vec![],
                trim_whitespace: false,
            },
            _phantom: PhantomData {},
        }
    }

    pub fn case_sensitive(mut self, is_case_sensitive: bool) -> Self {
        self.settings.case_sensitive = is_case_sensitive;
        self
    }

    pub fn ignore_chars(mut self, ignore_chars: impl IntoIterator<Item = char>) -> Self {
        self.settings.ignore_chars = ignore_chars.into_iter().collect();
        self
    }

    pub fn trim_whitespace(mut self, is_trim_whitespace: bool) -> Self {
        self.settings.trim_whitespace = is_trim_whitespace;
        self
    }

    pub fn indexer(&self) -> Box<dyn TreeIndex + Send> {
        Box::new(MultiNamedIndexer {
            storage: self.storage.clone(),
            extractor: self.extractor.clone(),
            settings: self.settings.clone(),
        })
    }

    pub fn get(&self, s: impl AsRef<str>) -> Option<K> {
        let Ok(rd) = self.storage.read() else {
            return None;
        };
        let s = self.settings.post_process(s);
        rd.index
            .get(s.as_str())
            .cloned()
            .map(|k| K::from_generic(k))
    }
}

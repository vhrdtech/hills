use std::{
    collections::BTreeMap,
    sync::{Arc, RwLock},
};

use hills_base::{index::IndexError, GenericKey};

use crate::db::Error;

use super::{Action, TreeIndex, TypeErasedTree};

type ExtractStrFn = fn(data: &[u8]) -> Result<Vec<String>, IndexError>;

/// Index that maps multiple unique name to the same record key.
/// Optionally some characters or case could be ignored and whitespace trimmed.
#[derive(Clone)]
pub struct MultiNamedIndex {
    storage: Arc<RwLock<Storage>>,
    exctractor: ExtractStrFn,
    case_sensitive: bool,
    ignore_chars: Vec<char>,
    trim_whitespace: bool,
}

#[derive(Default)]
struct Storage {
    index: BTreeMap<String, GenericKey>,
}

#[derive(Clone)]
struct MultiNamedIndexer {
    storage: Arc<RwLock<Storage>>,
    extractor: ExtractStrFn,
    case_sensitive: bool,
    ignore_chars: Vec<char>,
    trim_whitespace: bool,
}

impl MultiNamedIndexer {
    fn post_process(&self, s: String) -> String {
        let s = if self.case_sensitive {
            s
        } else {
            s.to_lowercase()
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

impl TreeIndex for MultiNamedIndexer {
    fn rebuild(&mut self, tree: TypeErasedTree) -> Result<(), Error> {
        let Ok(mut wr) = self.storage.write() else {
            return Err(Error::Index(IndexError::RwLock));
        };
        wr.index.clear();
        for key in tree.all_revisions() {
            let names = tree.get_with(key, |data| (self.extractor)(data))??;
            for name in names {
                let name = self.post_process(name);
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
                    let name = self.post_process(name);
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
                    .map(|s| self.post_process(s))
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
                    let name = self.post_process(name);
                    wr.index.remove(&name);
                }
            }
        }
        log::debug!("MultiNamed {:?}", wr.index);
        Ok(())
    }
}

impl MultiNamedIndex {
    pub fn new(exctractor: ExtractStrFn) -> Self {
        MultiNamedIndex {
            storage: Arc::new(RwLock::new(Storage::default())),
            exctractor,
            case_sensitive: true,
            ignore_chars: vec![],
            trim_whitespace: false,
        }
    }

    pub fn case_sensitive(self, is_case_sensitive: bool) -> Self {
        MultiNamedIndex {
            case_sensitive: is_case_sensitive,
            ..self
        }
    }

    pub fn ignore_chars(self, ignore_chars: impl IntoIterator<Item = char>) -> Self {
        MultiNamedIndex {
            ignore_chars: ignore_chars.into_iter().collect(),
            ..self
        }
    }

    pub fn trim_whitespace(self, is_trim_whitespace: bool) -> Self {
        MultiNamedIndex {
            trim_whitespace: is_trim_whitespace,
            ..self
        }
    }

    pub fn indexer(&self) -> Box<dyn TreeIndex + Send> {
        Box::new(MultiNamedIndexer {
            storage: self.storage.clone(),
            extractor: self.exctractor.clone(),
            case_sensitive: self.case_sensitive,
            ignore_chars: self.ignore_chars.clone(),
            trim_whitespace: self.trim_whitespace,
        })
    }

    pub fn get(&self, s: impl AsRef<str>) -> Option<GenericKey> {
        let Ok(rd) = self.storage.read() else {
            return None;
        };
        rd.index.get(s.as_ref()).cloned()
    }
}

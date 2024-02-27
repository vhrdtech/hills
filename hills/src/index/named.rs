use std::{
    collections::BTreeMap,
    sync::{Arc, RwLock},
};

use hills_base::{index::IndexError, GenericKey};

use crate::db::Error;

use super::{Action, TreeIndex, TypeErasedTree};

type ExtractStrFn = fn(data: &[u8]) -> Result<String, IndexError>;

/// Index that maps unique name to a record's key.
/// Optionally some characters or case could be ignored and whitespace trimmed.
#[derive(Clone)]
pub struct NamedIndex {
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
struct NamedIndexer {
    storage: Arc<RwLock<Storage>>,
    extractor: ExtractStrFn,
    case_sensitive: bool,
    ignore_chars: Vec<char>,
    trim_whitespace: bool,
}

impl NamedIndexer {
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

impl TreeIndex for NamedIndexer {
    fn rebuild(&mut self, tree: TypeErasedTree) -> Result<(), Error> {
        let Ok(mut wr) = self.storage.write() else {
            return Err(Error::Index(IndexError::RwLock));
        };
        wr.index.clear();
        for key in tree.all_revisions() {
            let s = tree.get_with(key, |data| (self.extractor)(data))??;
            let s = self.post_process(s);
            if wr.index.contains_key(&s) {
                return Err(Error::Index(IndexError::Duplicate(s)));
            }
            wr.index.insert(s, key);
        }
        log::debug!("Named index rebuilt: {:?}", wr.index);
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
                let s = (self.extractor)(data)?;
                let s = self.post_process(s);
                if wr.index.contains_key(&s) {
                    return Err(Error::Index(IndexError::Duplicate(s)));
                }
                wr.index.insert(s, key);
            }
            Action::Update => {
                let Some(old_name) = wr
                    .index
                    .iter()
                    .find(|(_, v)| **v == key)
                    .map(|(k, _)| k.to_string())
                else {
                    return Err(Error::Index(IndexError::Other(
                        "old name not found".to_string(),
                    )));
                };
                let new_name = (self.extractor)(data)?;
                let new_name = self.post_process(new_name);
                if old_name != new_name {
                    if wr.index.contains_key(&new_name) {
                        return Err(Error::Index(IndexError::Duplicate(new_name)));
                    }
                    wr.index.remove(&old_name);
                    wr.index.insert(new_name, key);
                }
            }
            Action::Remove => {
                let s = (self.extractor)(data)?;
                let s = self.post_process(s);
                wr.index.remove(&s);
            }
        }
        log::debug!("Named {:?}", wr.index);
        Ok(())
    }
}

impl NamedIndex {
    pub fn new(exctractor: ExtractStrFn) -> Self {
        NamedIndex {
            storage: Arc::new(RwLock::new(Storage::default())),
            exctractor,
            case_sensitive: true,
            ignore_chars: vec![],
            trim_whitespace: false,
        }
    }

    pub fn case_sensitive(self, is_case_sensitive: bool) -> Self {
        NamedIndex {
            case_sensitive: is_case_sensitive,
            ..self
        }
    }

    pub fn ignore_chars(self, ignore_chars: impl IntoIterator<Item = char>) -> Self {
        NamedIndex {
            ignore_chars: ignore_chars.into_iter().collect(),
            ..self
        }
    }

    pub fn trim_whitespace(self, is_trim_whitespace: bool) -> Self {
        NamedIndex {
            trim_whitespace: is_trim_whitespace,
            ..self
        }
    }

    pub fn indexer(&self) -> Box<dyn TreeIndex + Send> {
        Box::new(NamedIndexer {
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

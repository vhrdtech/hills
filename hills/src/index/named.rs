use std::{
    collections::BTreeMap,
    sync::{Arc, RwLock},
};

use hills_base::GenericKey;

use crate::db::Error;

use super::{Action, TreeIndex, TypeErasedTree};

type ExtractStrFn = fn(data: &[u8]) -> String;

#[derive(Clone)]
pub struct NamedIndex {
    storage: Arc<RwLock<Storage>>,
    exctractor: ExtractStrFn,
}

#[derive(Default)]
struct Storage {
    index: BTreeMap<String, GenericKey>,
}

#[derive(Clone)]
struct NamedIndexer {
    storage: Arc<RwLock<Storage>>,
    extractor: ExtractStrFn,
}

impl TreeIndex for NamedIndexer {
    fn rebuild(&mut self, tree: TypeErasedTree) -> Result<(), Error> {
        let Ok(mut wr) = self.storage.write() else {
            return Err(Error::Internal("RwLock fail".to_string()));
        };
        wr.index.clear();
        for key in tree.all_revisions() {
            let s = tree.get_with(key, |data| {
                let s = (self.extractor)(data);
                s
            })?;
            if s.is_empty() {
                return Err(Error::IndexReject("Empty name".to_string()));
            }
            if wr.index.contains_key(&s) {
                return Err(Error::IndexReject(format!("Duplicate {s}")));
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
            return Err(Error::Internal("RwLock fail".to_string()));
        };
        match action {
            Action::Insert => {
                let s = (self.extractor)(data);
                if s.is_empty() {
                    return Err(Error::IndexReject("Empty name".to_string()));
                }
                if wr.index.contains_key(&s) {
                    return Err(Error::IndexReject(format!("Duplicate {s}")));
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
                    return Err(Error::IndexReject("old name not found".to_string()));
                };
                let new_name = (self.extractor)(data);
                if new_name.is_empty() {
                    return Err(Error::IndexReject("Empty name".to_string()));
                }
                if old_name != new_name {
                    if wr.index.contains_key(&new_name) {
                        return Err(Error::IndexReject(format!("Duplicate {new_name}")));
                    }
                    wr.index.remove(&old_name);
                    wr.index.insert(new_name, key);
                }
            }
            Action::Remove => {
                let s = (self.extractor)(data);
                wr.index.remove(&s);
            }
        }
        log::debug!("{:?}", wr.index);
        Ok(())
    }
}

impl NamedIndex {
    pub fn new(exctractor: ExtractStrFn) -> Self {
        NamedIndex {
            storage: Arc::new(RwLock::new(Storage::default())),
            exctractor,
        }
    }

    pub fn indexer(&self) -> Box<dyn TreeIndex + Send> {
        Box::new(NamedIndexer {
            storage: self.storage.clone(),
            extractor: self.exctractor.clone(),
        })
    }

    pub fn get(&self, s: impl AsRef<str>) -> Option<GenericKey> {
        let Ok(rd) = self.storage.read() else {
            return None;
        };
        rd.index.get(s.as_ref()).cloned()
    }
}

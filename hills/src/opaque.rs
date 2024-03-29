use crate::consts::KEY_POOL;
use crate::db::{Error, RecordCheckOutState};
use crate::record::RecordMeta;
use crate::TypedTree;
use hills_base::{GenericKey, SimpleVersion, TreeKey, TreeRoot};
use rkyv::ser::serializers::AllocSerializer;
use rkyv::validation::validators::DefaultValidator;
use rkyv::{Archive, CheckBytes, Deserialize, Serialize};
use ron::ser::PrettyConfig;
use std::fmt::Debug;
use std::sync::Arc;

#[derive(Clone, Debug, PartialEq, Eq, Hash)]
pub struct OpaqueKey {
    pub tree_name: Arc<String>,
    pub id: u32,
    pub revision: u32,
}

impl OpaqueKey {
    pub fn new(tree_name: Arc<String>, key: GenericKey) -> Self {
        OpaqueKey {
            tree_name,
            id: key.id,
            revision: key.revision,
        }
    }
}

pub trait OpaqueTree {
    fn keys_in_pool(&self) -> Result<u32, Error>;
    fn record_meta(
        &self,
        key: &OpaqueKey,
    ) -> Result<Option<(u32, RecordMeta, u32, SimpleVersion)>, Error>;

    fn all_revisions(&self) -> Box<dyn Iterator<Item = OpaqueKey> + '_>;
    fn latest_revisions(&self) -> Box<dyn Iterator<Item = OpaqueKey>>;

    fn to_ron_str_pretty(&self, key: &OpaqueKey) -> Result<String, Error>;
    fn insert_from_ron_str(&mut self, value: &str) -> Result<GenericKey, Error>;
    fn update_from_ron_str(&mut self, key: &OpaqueKey, value: &str) -> Result<(), Error>;
    fn remove(&mut self, key: &OpaqueKey) -> Result<(), Error>;

    fn is_checked_out(&self, key: &OpaqueKey) -> Result<bool, Error>;
    fn checked_out_by(&self, key: &OpaqueKey) -> Result<RecordCheckOutState, Error>;
    fn check_out(&mut self, key: &OpaqueKey) -> Result<(), Error>;
    fn release(&mut self, key: &OpaqueKey) -> Result<(), Error>;

    fn versioning(&self) -> bool;
}

impl<K, V> OpaqueTree for TypedTree<K, V>
where
    K: TreeKey + Debug,
    V: TreeRoot
        + Archive
        + Serialize<AllocSerializer<128>>
        + serde::Serialize
        + serde::de::DeserializeOwned,
    <V as Archive>::Archived:
        Deserialize<V, rkyv::Infallible> + for<'a> CheckBytes<DefaultValidator<'a>>,
{
    fn keys_in_pool(&self) -> Result<u32, Error> {
        self.key_pool_stats()
    }

    fn record_meta(
        &self,
        key: &OpaqueKey,
    ) -> Result<Option<(u32, RecordMeta, u32, SimpleVersion)>, Error> {
        let key = check_key(key, self.tree_name.as_str())?;
        self.meta(key)
    }

    fn all_revisions(&self) -> Box<dyn Iterator<Item = OpaqueKey> + '_> {
        Box::new(self.data.iter().keys().filter_map(|key| {
            if let Ok(key) = key {
                if key == KEY_POOL {
                    return None;
                }
                let key = GenericKey::from_bytes(&key)?;
                let key = OpaqueKey {
                    tree_name: self.tree_name.clone(),
                    id: key.id,
                    revision: key.revision,
                };
                Some(key)
            } else {
                log::error!("Err in OpaqueTree::all_revisions");
                None
            }
        }))
    }

    fn latest_revisions(&self) -> Box<dyn Iterator<Item = OpaqueKey>> {
        todo!()
    }

    fn to_ron_str_pretty(&self, key: &OpaqueKey) -> Result<String, Error> {
        let key = check_key(key, self.tree_name.as_str())?;
        let value = self.get(key)?;
        let s = ron::ser::to_string_pretty(&value, PrettyConfig::default().compact_arrays(true))
            .map_err(|e| Error::Internal(format!("{e:?}")))?;
        Ok(s)
    }

    fn insert_from_ron_str(&mut self, value: &str) -> Result<GenericKey, Error> {
        let value: V = ron::de::from_str(value).map_err(|e| Error::Internal(format!("{e:?}")))?;
        self.insert(value).map(|k| k.to_generic())
    }

    fn update_from_ron_str(&mut self, key: &OpaqueKey, value: &str) -> Result<(), Error> {
        let key = check_key(key, self.tree_name.as_str())?;
        let value: V = ron::de::from_str(value).map_err(|e| Error::Internal(format!("{e:?}")))?;
        self.update(key, value)
    }

    fn remove(&mut self, key: &OpaqueKey) -> Result<(), Error> {
        let key = check_key(key, self.tree_name.as_str())?;
        <TypedTree<K, V>>::remove(self, key)?;
        Ok(())
    }

    fn versioning(&self) -> bool {
        V::versioning()
    }

    fn is_checked_out(&self, key: &OpaqueKey) -> Result<bool, Error> {
        let key = check_key(key, self.tree_name.as_str())?;
        Ok(<TypedTree<K, V>>::is_checked_out(&self, key))
    }

    fn check_out(&mut self, key: &OpaqueKey) -> Result<(), Error> {
        let key = check_key(key, self.tree_name.as_str())?;
        <TypedTree<K, V>>::check_out(self, key);
        Ok(())
    }

    fn release(&mut self, key: &OpaqueKey) -> Result<(), Error> {
        let key = check_key(key, self.tree_name.as_str())?;
        <TypedTree<K, V>>::release(self, key);
        Ok(())
    }

    fn checked_out_by(&self, key: &OpaqueKey) -> Result<RecordCheckOutState, Error> {
        let key = check_key(key, self.tree_name.as_str())?;
        Ok(<TypedTree<K, V>>::checked_out_by(&self, key))
    }
}

fn check_key<K: TreeKey>(key: &OpaqueKey, tree_name: &str) -> Result<K, Error> {
    if key.tree_name.as_str() != tree_name {
        return Err(Error::Usage(format!(
            "Tried to use {} key with {} tree",
            key.tree_name, tree_name
        )));
    }
    let key = GenericKey {
        id: key.id,
        revision: key.revision,
    };
    Ok(K::from_generic(key))
}

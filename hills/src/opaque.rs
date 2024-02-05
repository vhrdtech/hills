use crate::consts::KEY_POOL;
use crate::db::Error;
use crate::record::RecordMeta;
use crate::TreeBundle;
use hills_base::{GenericKey, TreeKey, TreeRoot};
use rkyv::ser::serializers::AllocSerializer;
use rkyv::validation::validators::DefaultValidator;
use rkyv::{Archive, CheckBytes, Deserialize, Serialize};
use ron::ser::PrettyConfig;
use std::fmt::Debug;
use std::sync::Arc;

#[derive(Clone)]
pub struct OpaqueKey {
    pub tree_name: Arc<String>,
    pub id: u32,
    pub revision: u32,
}

pub trait OpaqueTree {
    fn keys_in_pool(&self) -> Result<u32, Error>;
    fn record_meta(&self, key: &OpaqueKey) -> Result<Option<(u32, RecordMeta, u32)>, Error>;

    fn all_revisions(&self) -> Box<dyn Iterator<Item = OpaqueKey> + '_>;
    fn latest_revisions(&self) -> Box<dyn Iterator<Item = OpaqueKey>>;

    fn to_ron_str_pretty(&self, key: &OpaqueKey) -> Result<String, Error>;
    fn insert_from_ron_str(&mut self, value: &str) -> Result<GenericKey, Error>;
    fn update_from_ron_str(&mut self, key: &OpaqueKey, value: &str) -> Result<(), Error>;
}

impl<K, V> OpaqueTree for TreeBundle<K, V>
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

    fn record_meta(&self, key: &OpaqueKey) -> Result<Option<(u32, RecordMeta, u32)>, Error> {
        if key.tree_name.as_str() != self.tree_name.as_str() {
            return Err(Error::Usage(format!(
                "Tried to use {} key with {} tree",
                key.tree_name, self.tree_name
            )));
        }
        let key = GenericKey {
            id: key.id,
            revision: key.revision,
        };
        let key: K = K::from_generic(key);
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
        if key.tree_name.as_str() != self.tree_name.as_str() {
            return Err(Error::Usage(format!(
                "Tried to use {} key with {} tree",
                key.tree_name, self.tree_name
            )));
        }
        let key = GenericKey {
            id: key.id,
            revision: key.revision,
        };
        let key: K = K::from_generic(key);
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
        if key.tree_name.as_str() != self.tree_name.as_str() {
            return Err(Error::Usage(format!(
                "Tried to use {} key with {} tree",
                key.tree_name, self.tree_name
            )));
        }
        let key = GenericKey {
            id: key.id,
            revision: key.revision,
        };
        let key: K = K::from_generic(key);
        let value: V = ron::de::from_str(value).map_err(|e| Error::Internal(format!("{e:?}")))?;
        self.update(key, value)
    }
}

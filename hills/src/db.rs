use crate::record::ArchivedRecordState;
use crate::record::{Record, RecordState};
use crate::sync::NodeKind;
use crate::tree::{ArchivedTreeDescriptor, TreeDescriptor};
use chrono::Utc;
use hills_base::{GenericKey, Reflect, SimpleVersion, TreeKey, TreeRoot, TypeCollection};
use log::{info, trace, warn};
use rkyv::option::ArchivedOption;
use rkyv::ser::serializers::{
    AllocScratchError, AllocSerializer, CompositeSerializerError, SharedSerializeMapError,
};
use rkyv::validation::validators::{DefaultValidator, DefaultValidatorError};
use rkyv::validation::CheckArchiveError;
use rkyv::vec::ArchivedVec;
use rkyv::{
    archived_root, check_archived_root, to_bytes, Archive, CheckBytes, Deserialize, Fallible,
    Serialize,
};
use sled::{Db, Tree};
use std::collections::HashMap;
use std::convert::Infallible;
use std::marker::PhantomData;
use std::path::Path;
use thiserror::Error;

pub struct VhrdDb {
    db: Db,
    node_kind: NodeKind,
    descriptors: Tree,
    open_trees: HashMap<String, RawTreeBundle>,
}

#[derive(Clone)]
struct RawTreeBundle {
    /// Key -> Record tree
    data: Tree,
    /// Monotonic serial -> JournalEntry
    journal: Tree,
    /// All latest revisions -> ()
    latest_revision_index: Tree,
}

#[derive(Clone)]
pub struct TreeBundle<K, V> {
    /// Key -> Record tree
    data: Tree,
    /// Monotonic serial -> JournalEntry
    journal: Tree,
    /// Monotonic index -> Key for all the latest revisions
    latest_revision_index: Tree,

    username: String,

    _phantom_k: PhantomData<K>,
    _phantom_v: PhantomData<V>,
}

#[derive(Debug, Error)]
pub enum Error {
    #[error(transparent)]
    Sled(#[from] sled::Error),

    #[error("{}", .0)]
    Internal(String),

    #[error("Tree {} not found", .0)]
    TreeNotFound(String),

    #[error("check_archived_root failed")]
    RkyvCheckArchivedRoot,

    #[error("rkyv serialize: {}", .0)]
    RkyvSerializeError(String),

    #[error("rkyv check_archived_root failed: {}", .0)]
    RkyvDeserializeError(String),

    #[error("Evolution mismatch: {}", .0)]
    EvolutionMismatch(String),

    #[error("Key from tree {} used with another tree {}", .0, .1)]
    WrongKey(String, String),

    #[error("Value from tree {} used with another tree {}", .0, .1)]
    WrongValue(String, String),

    #[error("{}", .0)]
    VersioningMismatch(String),
}

impl From<CompositeSerializerError<Infallible, AllocScratchError, SharedSerializeMapError>>
    for Error
{
    fn from(
        value: CompositeSerializerError<Infallible, AllocScratchError, SharedSerializeMapError>,
    ) -> Self {
        Error::RkyvSerializeError(format!("{value:?}"))
    }
}

impl<T: core::fmt::Debug> From<CheckArchiveError<T, DefaultValidatorError>> for Error {
    fn from(value: CheckArchiveError<T, DefaultValidatorError>) -> Self {
        Error::RkyvDeserializeError(format!("{value:?}"))
    }
}

impl From<Infallible> for Error {
    fn from(_value: Infallible) -> Self {
        Error::RkyvDeserializeError("Infallible".into())
    }
}

impl VhrdDb {
    pub fn open<P: AsRef<Path>>(path: P, node_kind: NodeKind) -> Result<Self, Error> {
        let db = sled::open(path)?;
        let descriptors = db.open_tree("descriptors")?;
        Ok(VhrdDb {
            db,
            node_kind,
            descriptors,
            open_trees: HashMap::default(),
        })
    }

    // pub fn create_record(&mut self, tree_name: impl AsRef<str>) -> Result<RecordId, Error> {
    //     let descriptor = self.descriptors.get(tree_name.as_ref().as_bytes())?;
    //     let Some(descriptor) = descriptor else {
    //         return Err(Error::TreeNotFound(tree_name.as_ref().to_string()));
    //     };
    //     let descriptor = check_archived_root::<TreeDescriptor>(&descriptor)
    //         .map_err(|_| Error::RkyvCheckArchivedRoot)?;
    //     trace!("{descriptor:?}");
    //
    //     // match self.node_kind {
    //     //     NodeKind::Server | NodeKind::StandAlone => {}
    //     //     NodeKind::Client => {}
    //     //     NodeKind::Backup => {}
    //     // }
    //     todo!()
    // }

    pub fn open_tree<K, V>(
        &mut self,
        tree_name: impl AsRef<str>,
        username: impl AsRef<str>,
    ) -> Result<TreeBundle<K, V>, Error>
    where
        K: TreeKey,
        V: TreeRoot + Reflect,
    {
        let tree_name = tree_name.as_ref();
        let key_tree_name = <K as TreeKey>::tree_name();
        let evolution = <V as TreeRoot>::evolution();
        if key_tree_name != tree_name {
            return Err(Error::WrongKey(
                key_tree_name.to_string(),
                tree_name.to_string(),
            ));
        }
        let value_tree_name = <V as TreeRoot>::tree_name();
        if value_tree_name != tree_name {
            return Err(Error::WrongKey(
                key_tree_name.to_string(),
                tree_name.to_string(),
            ));
        }

        match self.open_trees.get(tree_name) {
            Some(tree) => Ok(TreeBundle {
                data: tree.data.clone(),
                journal: tree.journal.clone(),
                latest_revision_index: tree.latest_revision_index.clone(),
                username: username.as_ref().to_string(),
                _phantom_k: Default::default(),
                _phantom_v: Default::default(),
            }),
            None => {
                let mut current_tc = TypeCollection::new();
                V::reflect(&mut current_tc);
                match self.descriptors.get(tree_name.as_bytes())? {
                    Some(descriptor_bytes) => {
                        let descriptor: &ArchivedTreeDescriptor =
                            check_archived_root::<TreeDescriptor>(&descriptor_bytes)?;
                        let max_evolution = descriptor
                            .evolutions
                            .keys()
                            .map(|k| k.as_original())
                            .max()
                            .unwrap_or(SimpleVersion::new(0, 0));
                        trace!(
                            "Checking existing tree '{tree_name}' with latest evolution: {}",
                            max_evolution
                        );
                        // if evolution < current_evolution {
                        //     return Err(Error::EvolutionMismatch("Code evolution is older than database already have".into()));
                        // }
                        // TODO: register new evolution
                        if evolution > max_evolution {
                            info!("Will need to evolve {} to {}", max_evolution, evolution);
                            let mut descriptor: TreeDescriptor =
                                descriptor.deserialize(&mut rkyv::Infallible)?;
                            descriptor.evolutions.insert(evolution, current_tc);
                        } else if evolution < max_evolution {
                            trace!(
                                "Opening in backwards compatible mode, code is {}",
                                evolution
                            );
                        } else {
                            match descriptor.evolutions.get(&evolution.as_archived()) {
                                Some(known_evolution) => {
                                    let known_tc: TypeCollection =
                                        known_evolution.deserialize(&mut rkyv::Infallible)?;
                                    if current_tc != known_tc {
                                        return Err(Error::EvolutionMismatch("Type definitions changed compared to what's in the database".into()));
                                    }
                                    trace!("Type definitions matches exactly");
                                }
                                None => {
                                    warn!(
                                        "Didn't found {evolution} in the database tree descriptor"
                                    );
                                }
                            }
                        }
                    }
                    None => {
                        trace!("Create new tree {tree_name}");
                        if evolution != SimpleVersion::new(0, 0) {
                            return Err(Error::EvolutionMismatch(
                                "First evolution must be 0.0".into(),
                            ));
                        }
                        let descriptor = TreeDescriptor {
                            next_temporary_id: 0,
                            next_global_id: None,
                            description: "".to_string(),
                            evolutions: [(evolution, current_tc)].into(),
                        };
                        let descriptor_bytes = rkyv::to_bytes::<_, 1024>(&descriptor)?;
                        self.descriptors
                            .insert(tree_name.as_bytes(), descriptor_bytes.as_slice())?;
                    }
                }
                let data = self.db.open_tree(tree_name.as_bytes())?;
                let journal = self
                    .db
                    .open_tree(format!("{tree_name}_journal").as_bytes())?;
                let latest_revision_index = self
                    .db
                    .open_tree(format!("{tree_name}_latest_revision_index").as_bytes())?;
                let bundle = RawTreeBundle {
                    data,
                    journal,
                    latest_revision_index,
                };
                self.open_trees
                    .insert(tree_name.to_string(), bundle.clone());
                Ok(TreeBundle {
                    data: bundle.data,
                    journal: bundle.journal,
                    latest_revision_index: bundle.latest_revision_index,
                    username: username.as_ref().to_string(),
                    _phantom_k: Default::default(),
                    _phantom_v: Default::default(),
                })
            }
        }
    }
}

impl<K, V> TreeBundle<K, V>
where
    K: TreeKey,
    V: TreeRoot + Archive + Serialize<AllocSerializer<128>>,
    <V as Archive>::Archived:
        Deserialize<V, rkyv::Infallible> + for<'a> CheckBytes<DefaultValidator<'a>>,
{
    pub fn insert(&mut self, key: K, value: V) -> Result<(), Error> {
        let key = key.to_generic();
        let key_bytes = to_bytes::<_, 0>(&key)?;
        if let Some(replacing) = self.data.get(&key_bytes)? {
            let replacing = unsafe { archived_root::<Record>(&replacing) };
            if matches!(replacing.state, ArchivedRecordState::Released(_)) {
                return Err(Error::VersioningMismatch(format!(
                    "Cannot replace Released record {key:?}"
                )));
            }
        }
        if let Some(previous) = key.previous_revision() {
            let previous = &to_bytes::<_, 0>(&previous)?;
            if !self.latest_revision_index.contains_key(previous)? {
                return Err(Error::VersioningMismatch(format!(
                    "Cannot insert next revision without previous {key:?}"
                )));
            }
            let Some(previous_record) = self.data.get(&previous)? else {
                return Err(Error::Internal(format!(
                    "Previous version of {key:?} is not in the data tree"
                )));
            };
            let previous_record = unsafe { archived_root::<Record>(&previous_record) };
            if !matches!(previous_record.state, ArchivedRecordState::Released(_)) {
                return Err(Error::VersioningMismatch(format!("Cannot release a new revision if previous one is not in Released state {key:?}")));
            }
            self.latest_revision_index.remove(previous)?;
        }
        let value = to_bytes::<_, 128>(&value)?;
        let record = Record {
            key,
            state: RecordState::NonVersioned,
            last_edited_by: self.username.clone(),
            modified: Utc::now(),
            created: Utc::now(),
            meta: None,
            rust_version: SimpleVersion::rust_version(),
            rkyv_version: SimpleVersion::rkyv_version(),
            evolution: <V as TreeRoot>::evolution(),
            data: Some(value),
        };
        let record = to_bytes::<_, 128>(&record)?;
        self.data.insert(&key_bytes, &*record)?;
        self.latest_revision_index.insert(&key_bytes, &[])?;
        Ok(())
    }

    pub fn get(&self, key: K) -> Result<Option<V>, Error> {
        let value = self.data.get(to_bytes::<_, 0>(&key.to_generic())?)?;
        match value {
            Some(bytes) => {
                let archived_record = unsafe { archived_root::<Record>(&bytes) };

                match &archived_record.data {
                    ArchivedOption::Some(data) => {
                        let archived_data = check_archived_root::<V>(data)?;
                        let deserialized: V = archived_data.deserialize(&mut rkyv::Infallible)?;
                        Ok(Some(deserialized))
                    }
                    ArchivedOption::None => Ok(None),
                }
            }
            None => Ok(None),
        }
    }

    pub fn get_archived<F: FnMut(Option<&V::Archived>) -> R, R>(
        &self,
        key: K,
        mut f: F,
    ) -> Result<R, Error> {
        let value = self.data.get(to_bytes::<_, 0>(&key.to_generic())?)?;
        match value {
            Some(bytes) => {
                let archived_record = unsafe { archived_root::<Record>(&bytes) };
                match &archived_record.data {
                    ArchivedOption::Some(data) => {
                        let archived_data = check_archived_root::<V>(data)?;
                        Ok(f(Some(archived_data)))
                    }
                    ArchivedOption::None => Ok(f(None)),
                }
            }
            None => Ok(f(None)),
        }
    }

    pub fn latest_revisions(&self) -> impl Iterator<Item = GenericKey> {
        self.latest_revision_index.iter().keys().filter_map(|key| {
            if let Ok(key) = key {
                if key.len() < 8 {
                    warn!("Wrong key in latest_revisions");
                    return None;
                }
                // Alignment 2 is returned here (:
                // let key = unsafe { rkyv::archived_root::<GenericKey>(&key) };
                // let key: GenericKey = key.deserialize(&mut rkyv::Infallible)?;
                let mut word = [0u8; 4];
                word.copy_from_slice(&key[0..=3]);
                let id = u32::from_le_bytes(word);
                word.copy_from_slice(&key[4..=7]);
                let revision = u32::from_le_bytes(word);
                let key = GenericKey { id, revision };
                Some(key)
            } else {
                warn!("Err in latest_revisions");
                None
            }
        })
    }
}

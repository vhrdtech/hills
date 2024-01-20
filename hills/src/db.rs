use crate::key_pool::{ArchivedKeyPool, KeyPool};
use crate::record::{ArchivedVersion, RecordMeta};
use crate::record::{Record, Version};
use crate::sync::{ChangeKind, RecordHotChange};
use crate::sync_client::{SyncClientCommand, SyncClientTelemetry, VhrdDbSyncHandle};
use crate::sync_common::SELF_UUID;
use crate::tree::{ArchivedTreeDescriptor, TreeDescriptor};
use crate::{VhrdDbCmdTx, VhrdDbTelem};
use chrono::Utc;
use hills_base::{GenericKey, Reflect, SimpleVersion, TreeKey, TreeRoot, TypeCollection};
use log::{info, trace, warn};
use postage::prelude::Sink;
use rkyv::ser::serializers::{
    AllocScratchError, AllocSerializer, CompositeSerializerError, SharedSerializeMapError,
};
use rkyv::validation::validators::{DefaultValidator, DefaultValidatorError};
use rkyv::validation::CheckArchiveError;
use rkyv::{
    archived_root, check_archived_root, to_bytes, Archive, CheckBytes, Deserialize, Serialize,
};
use sled::transaction::{ConflictableTransactionError, TransactionError};
use sled::{Db, Tree};
use std::cmp::Ordering;
use std::collections::HashMap;
use std::convert::Infallible;
use std::fmt::Debug;
use std::marker::PhantomData;
use std::net::IpAddr;
use std::ops::Range;
use std::path::Path;
use thiserror::Error;
use tokio::runtime::Runtime;
use tokio::task::JoinHandle;
use uuid::Uuid;

pub struct VhrdDbClient {
    db: Db,
    self_uuid: Uuid,
    descriptors: Tree,
    open_trees: HashMap<String, RawTreeBundle>,
    event_tx: postage::mpsc::Sender<RecordHotChange>,
    cmd_tx: VhrdDbCmdTx,
    pub telem: VhrdDbTelem,
}

#[derive(Clone)]
struct RawTreeBundle {
    /// Key -> Record tree
    data: Tree,
    /// All latest revisions -> ()
    latest_revision_index: Tree,
    versioning: bool,
}

#[derive(Clone)]
pub struct TreeBundle<K, V> {
    /// Key -> Record tree
    data: Tree,
    /// Monotonic index -> Key for all the latest revisions
    latest_revision_index: Tree,

    tree_name: String,
    uuid: Uuid,
    username: String,
    versioning: bool,

    event_tx: postage::mpsc::Sender<RecordHotChange>,

    _phantom_k: PhantomData<K>,
    _phantom_v: PhantomData<V>,
}

#[derive(Debug, Error)]
pub enum Error {
    #[error(transparent)]
    Sled(#[from] sled::Error),

    #[error("{}", .0)]
    Usage(String),

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

    #[error("Pool of available keys depleted")]
    OutOfKeys,

    #[error("Mpsc send failed")]
    Mpsc,
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

impl<T: Debug> From<CheckArchiveError<T, DefaultValidatorError>> for Error {
    fn from(value: CheckArchiveError<T, DefaultValidatorError>) -> Self {
        Error::RkyvDeserializeError(format!("{value:?}"))
    }
}

impl From<Infallible> for Error {
    fn from(_value: Infallible) -> Self {
        Error::RkyvDeserializeError("Infallible".into())
    }
}

impl<T: Debug> From<TransactionError<T>> for Error {
    fn from(value: TransactionError<T>) -> Self {
        match value {
            TransactionError::Abort(e) => Error::Internal(format!("{e:?}")),
            TransactionError::Storage(e) => Error::Sled(e),
        }
    }
}

impl VhrdDbClient {
    pub fn open<P: AsRef<Path>>(
        path: P,
        rt: &Runtime,
    ) -> Result<(VhrdDbClient, JoinHandle<()>), Error> {
        #[cfg(not(test))]
        let db = sled::open(path)?;
        #[cfg(test)]
        let db = sled::Config::new().temporary(true).path(path).open()?;
        let descriptors = db.open_tree("descriptors")?;

        let self_uuid = match db.get(SELF_UUID)? {
            Some(uuid_bytes) => {
                if uuid_bytes.len() != 16 {
                    return Err(Error::Internal("Invalid self_uuid".into()));
                }
                let mut uuid = [0u8; 16];
                uuid[..].copy_from_slice(&uuid_bytes);
                Uuid::from_bytes(uuid)
            }
            None => {
                let uuid = Uuid::new_v4();
                trace!("Created new db, uuid={uuid}");
                let uuid_bytes = uuid.into_bytes();
                db.insert(SELF_UUID, &uuid_bytes)?;
                uuid
            }
        };

        let (tx, rx) = postage::mpsc::channel(64);
        let sync_handle = VhrdDbSyncHandle::new(db.clone(), rx);
        let (cmd_tx, telem, syncer_join) = sync_handle.start(rt);
        Ok((
            VhrdDbClient {
                db,
                self_uuid,
                descriptors,
                open_trees: HashMap::default(),
                event_tx: tx,
                cmd_tx,
                telem,
            },
            syncer_join,
        ))
    }

    pub fn open_tree<K, V>(
        &mut self,
        tree_name: impl AsRef<str>,
        username: impl AsRef<str>,
        versioning: bool,
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
            Some(raw_tree) => Ok(TreeBundle {
                data: raw_tree.data.clone(),
                latest_revision_index: raw_tree.latest_revision_index.clone(),
                username: username.as_ref().to_string(),
                versioning: raw_tree.versioning,
                tree_name: tree_name.to_string(),
                event_tx: self.event_tx.clone(),
                uuid: self.self_uuid,

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
                        if versioning != descriptor.versioning {
                            return Err(Error::VersioningMismatch(
                                "Cannot change versioning of a tree after creation".to_owned(),
                            ));
                        }
                        // if evolution < current_evolution {
                        //     return Err(Error::EvolutionMismatch("Code evolution is older than database already have".into()));
                        // }
                        // TODO: register new evolution
                        match evolution.cmp(&max_evolution) {
                            Ordering::Less => {
                                trace!(
                                    "Opening in backwards compatible mode, code is {}",
                                    evolution
                                );
                            }
                            Ordering::Equal => {
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
                            Ordering::Greater => {
                                info!("Will need to evolve {} to {}", max_evolution, evolution);
                                let mut descriptor: TreeDescriptor =
                                    descriptor.deserialize(&mut rkyv::Infallible)?;
                                descriptor.evolutions.insert(evolution, current_tc);
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
                            evolutions: [(evolution, current_tc)].into(),
                            versioning,
                        };
                        let descriptor_bytes = rkyv::to_bytes::<_, 1024>(&descriptor)?;
                        self.descriptors
                            .insert(tree_name.as_bytes(), descriptor_bytes.as_slice())?;
                    }
                }
                let data = self.db.open_tree(tree_name.as_bytes())?;
                let latest_revision_index = self
                    .db
                    .open_tree(format!("{tree_name}_latest_revision_index").as_bytes())?;

                let bundle = RawTreeBundle {
                    data,
                    latest_revision_index,
                    versioning,
                };
                self.open_trees
                    .insert(tree_name.to_string(), bundle.clone());
                Ok(TreeBundle {
                    data: bundle.data,
                    latest_revision_index: bundle.latest_revision_index,
                    username: username.as_ref().to_string(),
                    versioning,
                    tree_name: tree_name.to_string(),
                    event_tx: self.event_tx.clone(),
                    uuid: self.self_uuid,

                    _phantom_k: Default::default(),
                    _phantom_v: Default::default(),
                })
            }
        }
    }

    pub fn connect(&mut self, ip_addr: IpAddr, port: u16) {
        let r = self
            .cmd_tx
            .blocking_send(SyncClientCommand::Connect(ip_addr, port));
        if r.is_err() {
            warn!("db: connect: send failed");
        }
    }

    pub fn disconnect(&mut self) {
        let r = self.cmd_tx.blocking_send(SyncClientCommand::Disconnect);
        if r.is_err() {
            warn!("db: disconnect: send failed");
        }
    }

    pub fn telemetry<F: FnMut(&SyncClientTelemetry)>(&self, mut f: F) {
        if let Ok(telem) = self.telem.read() {
            f(&telem);
        }
    }
}

impl<K, V> TreeBundle<K, V>
where
    K: TreeKey + Debug,
    V: TreeRoot + Archive + Serialize<AllocSerializer<128>>,
    <V as Archive>::Archived:
        Deserialize<V, rkyv::Infallible> + for<'a> CheckBytes<DefaultValidator<'a>>,
{
    // TODO: should not be pub
    pub fn feed_key_pool(&mut self, additional_range: Range<u32>) -> Result<(), Error> {
        self.data
            .transaction(|tx_db| match tx_db.get(b"_key_pool")? {
                Some(key_pool) => {
                    // let key_pool: &ArchivedKeyPool = unsafe { archived_root::<KeyPool>(&key_pool) };
                    let key_pool: &ArchivedKeyPool = check_archived_root::<KeyPool>(&key_pool)
                        .map_err(|e| {
                            ConflictableTransactionError::Abort(format!(
                                "check_archived_root: {e:?}"
                            ))
                        })?;
                    let mut key_pool: KeyPool =
                        key_pool.deserialize(&mut rkyv::Infallible).map_err(|_| {
                            ConflictableTransactionError::Abort(
                                "get_next_key: deserialize".to_string(),
                            )
                        })?;
                    key_pool.push(additional_range.clone());
                    let key_pool = to_bytes::<_, 8>(&key_pool)
                        .map_err(|_| ConflictableTransactionError::Abort("to_bytes".to_string()))?;
                    tx_db.insert(b"_key_pool", &*key_pool)?;
                    Ok(Ok(()))
                }
                None => {
                    let key_pool = KeyPool::new(vec![additional_range.clone()]);
                    let key_pool = to_bytes::<_, 8>(&key_pool)
                        .map_err(|_| ConflictableTransactionError::Abort("to_bytes".to_string()))?;
                    tx_db.insert(b"_key_pool", &*key_pool)?;
                    Ok(Ok(()))
                }
            })?
    }

    pub fn key_pool_stats(&self) -> Result<u32, Error> {
        if let Some(key_pool) = self.data.get(b"_key_pool")? {
            // let key_pool: &ArchivedKeyPool = unsafe { archived_root::<KeyPool>(&key_pool) };
            let key_pool: &ArchivedKeyPool = check_archived_root::<KeyPool>(&key_pool)?;
            let key_pool: KeyPool = key_pool.deserialize(&mut rkyv::Infallible).unwrap();
            Ok(key_pool.total_keys_available())
        } else {
            Err(Error::Usage("No key pool".to_string()))
        }
    }

    fn get_next_key(&mut self) -> Result<u32, Error> {
        self.data
            .transaction(|tx_db| match tx_db.get(b"_key_pool")? {
                Some(key_pool) => {
                    // let key_pool: &ArchivedKeyPool = unsafe { archived_root::<KeyPool>(&key_pool) };
                    let key_pool: &ArchivedKeyPool = check_archived_root::<KeyPool>(&key_pool)
                        .map_err(|_| {
                            ConflictableTransactionError::Abort("checked_archived_root")
                        })?;
                    let mut key_pool: KeyPool =
                        key_pool.deserialize(&mut rkyv::Infallible).map_err(|_| {
                            ConflictableTransactionError::Abort("get_next_key: deserialize")
                        })?;
                    let next_key = match key_pool.get() {
                        Some(next_key) => {
                            let key_pool = to_bytes::<_, 8>(&key_pool)
                                .map_err(|_| ConflictableTransactionError::Abort("to_bytes"))?;
                            tx_db.insert(b"_key_pool", &*key_pool)?;
                            next_key
                        }
                        None => {
                            return Ok(Err(Error::OutOfKeys));
                        }
                    };
                    Ok(Ok(next_key))
                }
                None => Ok(Err(Error::OutOfKeys)),
            })?
    }

    pub fn insert(&mut self, value: V) -> Result<K, Error> {
        let key = self.get_next_key()?;
        let key = GenericKey::new(key, 0);
        let key_bytes = to_bytes::<_, 0>(&key)?;

        if self.data.contains_key(&key_bytes)? {
            return Err(Error::Internal("Duplicate key from KeyPool".to_string()));
        }
        let data = to_bytes::<_, 128>(&value)?;
        let versioning = if self.versioning {
            Version::Draft
        } else {
            Version::NonVersioned
        };
        let meta = RecordMeta {
            key,
            version: versioning,
            modified_on: self.uuid.clone().into_bytes(),
            modified_by: self.username.clone(),
            modified: Utc::now(),
            created: Utc::now(),
            rust_version: SimpleVersion::rust_version(),
            rkyv_version: SimpleVersion::rkyv_version(),
            evolution: <V as TreeRoot>::evolution(),
        };
        let record = Record {
            meta_iteration: 0,
            meta,
            data_iteration: 0,
            data,
        };
        let record = to_bytes::<_, 128>(&record)?;
        self.data.insert(&key_bytes, &*record)?;
        self.latest_revision_index.insert(&key_bytes, &[])?;

        let change = RecordHotChange {
            tree: self.tree_name.clone(),
            key,
            kind: ChangeKind::Create,
        };
        self.event_tx
            .blocking_send(change)
            .map_err(|_| Error::Mpsc)?;

        Ok(K::from_generic(key))
    }

    pub fn update(&mut self, key: K, value: V) -> Result<K, Error> {
        let generic_key = key.to_generic();
        let key_bytes = to_bytes::<_, 0>(&generic_key)?;

        if !self.versioning && generic_key.revision != 0 {
            return Err(Error::Usage(format!(
                "update {key:?}, on un-versioned tree"
            )));
        }
        if let Some(previous) = generic_key.previous_revision() {
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
            if matches!(previous_record.meta.version, ArchivedVersion::Draft) {
                return Err(Error::VersioningMismatch(format!("Cannot release a new revision if previous one is not in Released state {key:?}")));
            }
            self.latest_revision_index.remove(previous)?;
        }

        if let Some(replacing) = self.data.get(&key_bytes)? {
            let replacing = unsafe { archived_root::<Record>(&replacing) };
            if self.versioning {
                if matches!(replacing.meta.version, ArchivedVersion::Released(_)) {
                    return Err(Error::VersioningMismatch(format!(
                        "Cannot replace Released record {key:?}"
                    )));
                }
            }
            let data = to_bytes::<_, 128>(&value)?;
            let versioning = if self.versioning {
                Version::Draft
            } else {
                Version::NonVersioned
            };
            let meta = RecordMeta {
                key: generic_key,
                version: versioning,
                modified_on: self.uuid.clone().into_bytes(),
                modified_by: self.username.clone(),
                modified: Utc::now(),
                created: replacing
                    .meta
                    .created
                    .deserialize(&mut rkyv::Infallible)
                    .unwrap(),
                rust_version: SimpleVersion::rust_version(),
                rkyv_version: SimpleVersion::rkyv_version(),
                evolution: <V as TreeRoot>::evolution(),
            };
            let record = Record {
                meta_iteration: replacing.meta_iteration,
                meta,
                data_iteration: replacing.data_iteration + 1,
                data,
            };
            let record = to_bytes::<_, 128>(&record)?;
            self.data.insert(&key_bytes, &*record)?;
            self.latest_revision_index.insert(&key_bytes, &[])?;

            let change = RecordHotChange {
                tree: self.tree_name.clone(),
                key: generic_key,
                kind: ChangeKind::ModifyBoth,
            };
            self.event_tx
                .blocking_send(change)
                .map_err(|_| Error::Mpsc)?;

            Ok(K::from_generic(generic_key))
        } else {
            Err(Error::Usage(format!(
                "update {key:?}, not found, create record first"
            )))
        }
    }

    pub fn get(&self, key: K) -> Result<Option<V>, Error> {
        let value = self.data.get(to_bytes::<_, 0>(&key.to_generic())?)?;
        match value {
            Some(bytes) => {
                let archived_record = unsafe { archived_root::<Record>(&bytes) };

                let archived_data = check_archived_root::<V>(&archived_record.data)?;
                let deserialized: V = archived_data.deserialize(&mut rkyv::Infallible)?;
                Ok(Some(deserialized))
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
                let archived_data = check_archived_root::<V>(&archived_record.data)?;
                Ok(f(Some(archived_data)))
            }
            None => Ok(f(None)),
        }
    }

    pub fn latest_revisions(&self) -> impl Iterator<Item = K> {
        self.latest_revision_index.iter().keys().filter_map(|key| {
            if let Ok(key) = key {
                let key = GenericKey::from_bytes(&key)?;
                let key = K::from_generic(key);
                Some(key)
            } else {
                warn!("Err in latest_revisions");
                None
            }
        })
    }
}

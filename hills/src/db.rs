use crate::common::ManagedTrees;
use crate::consts::{DESCRIPTORS_TREE, KEY_POOL, READABLE_NAME, SELF_UUID};
use crate::index::{TreeIndex, TypeErasedTree};
use crate::key_pool::{ArchivedKeyPool, KeyPool};
use crate::opaque::OpaqueKey;
use crate::record::{ArchivedVersion, RecordMeta};
use crate::record::{Record, Version};
use crate::sync::{ChangeKind, RecordBorrows, RecordHotChange};
use crate::sync_client::{
    ChangeNotification, SyncClientCommand, SyncClientTelemetry, SyncHandle, VhrdDbCmdTx,
};
use crate::tree::{ArchivedTreeDescriptor, TreeDescriptor};
use crate::VhrdDbTelem;
use chrono::Utc;
use hills_base::{Evolving, GenericKey, Reflect, SimpleVersion, TreeKey, TreeRoot, TypeCollection};
use log::{error, info, trace, warn};
use postage::prelude::Sink;
use rkyv::ser::serializers::{
    AllocScratchError, AllocSerializer, CompositeSerializerError, SharedSerializeMapError,
};
use rkyv::validation::validators::{DefaultValidator, DefaultValidatorError};
use rkyv::validation::CheckArchiveError;
use rkyv::{check_archived_root, to_bytes, Archive, CheckBytes, Deserialize, Serialize};
use sled::transaction::{ConflictableTransactionError, TransactionError};
use sled::{Db, Tree};
use std::cmp::Ordering;
use std::collections::HashMap;
use std::convert::Infallible;
use std::fmt::Debug;
use std::marker::PhantomData;
use std::net::IpAddr;
use std::path::Path;
use std::sync::Arc;
use thiserror::Error;
use tokio::runtime::Runtime;
use tokio::sync::RwLock;
use tokio::task::JoinHandle;
use uuid::Uuid;

pub struct HillsClient {
    db: Db,
    self_uuid: Uuid,
    descriptors: Tree,
    open_trees: HashMap<String, RawTreeBundle>,
    cmd_tx: VhrdDbCmdTx,
    updates_tx: postage::broadcast::Sender<ChangeNotification>,
    borrows: Arc<RwLock<RecordBorrows>>,
    pub telem: VhrdDbTelem,
}

#[derive(Clone)]
struct RawTreeBundle {
    /// Key -> Record tree
    data: Tree,
    versioning: bool,
    indexers: Vec<Box<dyn TreeIndex>>,
}

#[derive(Clone)]
pub struct TypedTree<K, V> {
    /// Key -> Record tree
    pub(crate) data: Tree,

    pub(crate) tree_name: Arc<String>,
    uuid: Uuid,
    username: String,
    versioning: bool,

    /// Notifications to client (internal)
    cmd_tx: VhrdDbCmdTx,
    /// Notifications to user (pub)
    updates_tx: postage::broadcast::Sender<ChangeNotification>,

    indexers: Vec<Box<dyn TreeIndex>>,
    borrows: Arc<RwLock<RecordBorrows>>,

    _phantom_k: PhantomData<K>,
    _phantom_v: PhantomData<V>,
}

// TODO: Make one common error?
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

    #[error("check_archived_root failed: {}", .0)]
    RkyvCheckArchivedRoot(String),

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

    #[error("Provided key is not in the tree")]
    RecordNotFound,

    #[error(transparent)]
    Index(#[from] hills_base::index::IndexError),
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
        Error::RkyvCheckArchivedRoot(format!("{value:?}"))
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

use crate::common::Error as CommonError;
impl From<CommonError> for Error {
    fn from(value: CommonError) -> Self {
        match value {
            CommonError::Sled(e) => Error::Sled(e),
            CommonError::Internal(e) => Error::Internal(e),
            CommonError::Ws => Error::Internal("common::Error::Ws".to_string()),
            CommonError::RkyvSerializeError(e) => Error::RkyvSerializeError(e),
            CommonError::RkyvDeserializeError(e) => Error::RkyvDeserializeError(e),
            CommonError::PostageBroadcast => Error::Internal("postage broadcasr".into()),
        }
    }
}

/// Holds information about record's borrow state.
pub enum RecordCheckOutState {
    /// Record is currently not checked out by any client
    Empty,
    /// Record is checked out by .0 and maybe others, this client is waiting as well
    WaitingFor(Uuid),
    /// Record is checked out by .0 and maybe others, this client is not waiting
    CheckedOutBy(Uuid),
    /// Record is borrowed by this client and could be modified
    CheckedOut,
}

impl HillsClient {
    pub fn open<P: AsRef<Path>>(
        path: P,
        rt: &Runtime,
    ) -> Result<
        (
            HillsClient,
            postage::broadcast::Receiver<ChangeNotification>,
            JoinHandle<()>,
        ),
        Error,
    > {
        #[cfg(not(test))]
        let db = sled::open(path)?;
        #[cfg(test)]
        let db = sled::Config::new().temporary(true).path(path).open()?;
        let descriptors = db.open_tree(DESCRIPTORS_TREE)?;

        let self_uuid = match db.get(SELF_UUID)? {
            Some(uuid_bytes) => {
                if uuid_bytes.len() != 16 {
                    return Err(Error::Internal("Invalid self_uuid".into()));
                }
                let mut uuid = [0u8; 16];
                uuid[..].copy_from_slice(&uuid_bytes);
                let uuid = Uuid::from_bytes(uuid);
                trace!("Self uuid is {uuid}");
                uuid
            }
            None => {
                let uuid = Uuid::new_v4();
                trace!("Created new db, uuid={uuid}");
                let uuid_bytes = uuid.into_bytes();
                db.insert(SELF_UUID, &uuid_bytes)?;
                uuid
            }
        };

        let sync_handle = SyncHandle::new(db.clone());
        let (updates_tx, updates_rx) = postage::broadcast::channel(64);
        let borrows = Arc::new(RwLock::new(RecordBorrows::default()));
        let (cmd_tx, telem, syncer_join) =
            sync_handle.start(rt, updates_tx.clone(), borrows.clone());
        Ok((
            HillsClient {
                db,
                self_uuid,
                descriptors,
                open_trees: HashMap::default(),
                cmd_tx,
                updates_tx,
                borrows,
                telem,
            },
            updates_rx,
            syncer_join,
        ))
    }

    pub fn set_readable_name(&mut self, name: impl AsRef<str>) -> Result<(), Error> {
        if let Some(existing) = self.db.get(READABLE_NAME)? {
            let existing = std::str::from_utf8(&existing).unwrap_or("");
            if existing != name.as_ref() {
                self.db.insert(READABLE_NAME, name.as_ref())?;
            }
        } else {
            self.db.insert(READABLE_NAME, name.as_ref())?;
        }
        trace!("Self readable name is {}", name.as_ref());
        Ok(())
    }

    pub fn open_tree<K, V>(&mut self, username: impl AsRef<str>) -> Result<TypedTree<K, V>, Error>
    where
        K: TreeKey,
        V: TreeRoot + Reflect,
    {
        let tree_name = <V as TreeRoot>::tree_name();
        if tree_name.starts_with("_") {
            return Err(Error::Usage("Tree names cannot start with '_'".to_string()));
        }
        let versioning = <V as TreeRoot>::versioning();

        ManagedTrees::add_to_managed(&self.db, tree_name)?;

        match self.open_trees.get(tree_name) {
            Some(raw_tree) => Ok(TypedTree {
                data: raw_tree.data.clone(),
                username: username.as_ref().to_string(),
                versioning: raw_tree.versioning,
                tree_name: Arc::new(tree_name.to_string()),
                // event_tx: self.event_tx.clone(),
                updates_tx: self.updates_tx.clone(),
                uuid: self.self_uuid,
                indexers: raw_tree.indexers.clone(),
                borrows: self.borrows.clone(),
                cmd_tx: self.cmd_tx.clone(),

                _phantom_k: Default::default(),
                _phantom_v: Default::default(),
            }),
            None => {
                self.open_cold_tree::<K, V>()?;
                let Some(bundle) = self.open_trees.get(tree_name) else {
                    return Err(Error::Internal("open_cold_tree failed".to_string()));
                };
                Ok(TypedTree {
                    data: bundle.data.clone(),
                    username: username.as_ref().to_string(),
                    versioning,
                    tree_name: Arc::new(tree_name.to_string()),
                    // event_tx: self.event_tx.clone(),
                    updates_tx: self.updates_tx.clone(),
                    uuid: self.self_uuid,
                    indexers: bundle.indexers.clone(),
                    borrows: self.borrows.clone(),
                    cmd_tx: self.cmd_tx.clone(),

                    _phantom_k: Default::default(),
                    _phantom_v: Default::default(),
                })
            }
        }
    }

    pub fn add_indexer<K, V>(&mut self, mut indexer: Box<dyn TreeIndex + Send>) -> Result<(), Error>
    where
        K: TreeKey,
        V: TreeRoot + Reflect,
    {
        self.open_cold_tree::<K, V>()?;
        let tree_name = <V as TreeRoot>::tree_name();
        let evolution = <V as TreeRoot>::evolution();
        let Some(bundle) = self.open_trees.get_mut(tree_name) else {
            return Err(Error::Internal("open_cold_tree failed".to_string()));
        };
        indexer.rebuild(TypeErasedTree {
            tree: &mut bundle.data,
            evolution,
        })?;
        bundle.indexers.push(indexer.clone());
        let r = self.cmd_tx.blocking_send(SyncClientCommand::RegisterIndex {
            tree_name: tree_name.to_string(),
            indexer,
        });
        if r.is_err() {
            warn!("db: add_indexer: send failed");
        }
        Ok(())
    }

    fn open_cold_tree<K, V>(&mut self) -> Result<(), Error>
    where
        K: TreeKey,
        V: TreeRoot + Reflect,
    {
        let key_tree_name = <K as TreeKey>::tree_name();
        let tree_name = <V as TreeRoot>::tree_name();
        if key_tree_name != tree_name {
            return Err(Error::WrongKey(
                key_tree_name.to_string(),
                tree_name.to_string(),
            ));
        }
        if tree_name.starts_with("_") {
            return Err(Error::Usage("Tree names cannot start with '_'".to_string()));
        }
        ManagedTrees::add_to_managed(&self.db, tree_name)?;

        let versioning = <V as TreeRoot>::versioning();
        let mut current_tc = TypeCollection::new();
        let evolution = <V as TreeRoot>::evolution();
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
                                warn!("Didn't found {evolution} in the database tree descriptor");
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
                // if evolution != SimpleVersion::new(0, 0) {
                //     return Err(Error::EvolutionMismatch(
                //         "First evolution must be 0.0".into(),
                //     ));
                // }
                let descriptor = TreeDescriptor {
                    evolutions: [(evolution, current_tc)].into(),
                    versioning,
                };
                let descriptor_bytes = to_bytes::<_, 1024>(&descriptor)?;
                self.descriptors
                    .insert(tree_name.as_bytes(), descriptor_bytes.as_slice())?;
                let r = self
                    .cmd_tx
                    .blocking_send(SyncClientCommand::TreeCreated(tree_name.to_string()));
                if r.is_err() {
                    warn!("db: TreeCreated send failed");
                }
            }
        }
        let data = self.db.open_tree(tree_name.as_bytes())?;

        let bundle = RawTreeBundle {
            data,
            versioning,
            indexers: Vec::new(),
        };
        self.open_trees
            .insert(tree_name.to_string(), bundle.clone());
        Ok(())
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
        if let Ok(telem) = self.telem.try_read() {
            f(&telem);
        }
    }
}

impl<K, V> TypedTree<K, V>
where
    K: TreeKey + Debug,
    V: TreeRoot + Archive + Serialize<AllocSerializer<128>>,
    <V as Archive>::Archived:
        Deserialize<V, rkyv::Infallible> + for<'a> CheckBytes<DefaultValidator<'a>>,
{
    // pub fn feed_key_pool(&mut self, additional_range: Range<u32>) -> Result<(), Error> {
    //     KeyPool::feed_for(&self.data, additional_range).map_err(Error::Internal)
    // }

    pub fn key_pool_stats(&self) -> Result<u32, Error> {
        Ok(KeyPool::stats_for(&self.data)?)
    }

    fn pool_get_key(&mut self) -> Result<GenericKey, Error> {
        self.data.transaction(|tx_db| match tx_db.get(KEY_POOL)? {
            Some(key_pool) => {
                // let key_pool: &ArchivedKeyPool = unsafe { archived_root::<KeyPool>(&key_pool) };
                let key_pool: &ArchivedKeyPool = check_archived_root::<KeyPool>(&key_pool)
                    .map_err(|_| ConflictableTransactionError::Abort("checked_archived_root"))?;
                let mut key_pool: KeyPool =
                    key_pool.deserialize(&mut rkyv::Infallible).map_err(|_| {
                        ConflictableTransactionError::Abort("get_next_key: deserialize")
                    })?;
                let next_key = match key_pool.get() {
                    Some(next_key) => {
                        let key_pool = to_bytes::<_, 8>(&key_pool)
                            .map_err(|_| ConflictableTransactionError::Abort("to_bytes"))?;
                        tx_db.insert(KEY_POOL, &*key_pool)?;
                        let next_key = GenericKey::new(next_key, 0);
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
        let generic_key = self.pool_get_key()?;
        let key_bytes = generic_key.to_bytes();
        let evolution = <V as TreeRoot>::evolution();

        if self.data.contains_key(key_bytes)? {
            return Err(Error::Internal("Duplicate key from KeyPool".to_string()));
        }
        let data = to_bytes::<_, 128>(&Evolving(value))?;
        for indexer in &mut self.indexers {
            indexer.update(
                TypeErasedTree {
                    tree: &mut self.data,
                    evolution,
                },
                generic_key,
                &data,
                crate::index::Action::Insert,
            )?;
        }

        let versioning = if self.versioning {
            Version::Draft(0)
        } else {
            Version::NonVersioned
        };
        let meta = RecordMeta {
            key: generic_key,
            version: versioning,
            modified_on: self.uuid.into_bytes(),
            modified_by: self.username.clone(),
            modified: Utc::now().into(),
            created: Utc::now().into(),
            rkyv_version: SimpleVersion::rkyv_version(),
        };
        let record = Record {
            meta_iteration: 0,
            meta,
            data_iteration: 0,
            data,
            data_evolution: evolution,
        };
        let record = to_bytes::<_, 128>(&record)?;
        self.data.insert(key_bytes, &*record)?;

        let change = RecordHotChange {
            tree: String::from(self.tree_name.as_str()),
            key: generic_key,
            kind: ChangeKind::CreateOrChange,
            data_iteration: 0,
            meta_iteration: 0,
        };
        self.cmd_tx
            .blocking_send(SyncClientCommand::Change(change))
            .map_err(|_| Error::Mpsc)?;

        let notification = ChangeNotification::Tree {
            key: OpaqueKey::new(self.tree_name.clone(), generic_key),
            kind: ChangeKind::CreateOrChange,
        };
        if let Err(_) = self.updates_tx.try_send(notification) {
            warn!("Notification send: mpsc fail");
        }

        Ok(K::from_generic(generic_key))
    }

    pub fn update(&mut self, key: K, value: V) -> Result<(), Error> {
        let generic_key = key.to_generic();
        if !self.is_checked_out(key) {
            return Err(Error::Usage(format!(
                "Cannot update: {}/{generic_key} - not checked out",
                self.tree_name
            )));
        }

        let key_bytes = generic_key.to_bytes();
        let evolution = <V as TreeRoot>::evolution();

        if !self.versioning && generic_key.revision != 0 {
            return Err(Error::Usage(format!(
                "update {generic_key}, on un-versioned tree"
            )));
        }
        if let Some(previous) = generic_key.previous_revision() {
            let previous = previous.to_bytes();
            // if !self.latest_revision_index.contains_key(previous)? {
            //     return Err(Error::VersioningMismatch(format!(
            //         "Cannot insert next revision without previous {key:?}"
            //     )));
            // }
            let Some(previous_record) = self.data.get(previous)? else {
                return Err(Error::Internal(format!(
                    "Previous version of {}/{generic_key} is not in the data tree",
                    self.tree_name
                )));
            };
            let previous_record = check_archived_root::<Record>(&previous_record)?;
            if matches!(previous_record.meta.version, ArchivedVersion::Draft(0)) {
                return Err(Error::VersioningMismatch(format!("Cannot release a new revision if previous one is not in Released state {}/{generic_key}", self.tree_name)));
            }
            // self.latest_revision_index.remove(previous)?;
        }

        if let Some(replacing) = self.data.get(key_bytes)? {
            let replacing = check_archived_root::<Record>(&replacing)?;
            if self.versioning && matches!(replacing.meta.version, ArchivedVersion::Released(_)) {
                return Err(Error::VersioningMismatch(format!(
                    "Cannot replace Released record {}/{generic_key}",
                    self.tree_name
                )));
            }
            let data = to_bytes::<_, 128>(&Evolving(value))?;
            for indexer in &mut self.indexers {
                indexer.update(
                    TypeErasedTree {
                        tree: &mut self.data,
                        evolution,
                    },
                    generic_key,
                    &data,
                    crate::index::Action::Update,
                )?;
            }

            let versioning = if self.versioning {
                Version::Draft(0)
            } else {
                Version::NonVersioned
            };
            let meta = RecordMeta {
                key: generic_key,
                version: versioning,
                modified_on: self.uuid.into_bytes(),
                modified_by: self.username.clone(),
                modified: Utc::now().into(),
                created: replacing
                    .meta
                    .created
                    .deserialize(&mut rkyv::Infallible)
                    .expect(""),
                rkyv_version: SimpleVersion::rkyv_version(),
            };
            let record = Record {
                meta_iteration: replacing.meta_iteration + 1,
                meta,
                data_iteration: replacing.data_iteration + 1,
                data,
                data_evolution: evolution,
            };
            let record_bytes = to_bytes::<_, 128>(&record)?;
            self.data.insert(key_bytes, &*record_bytes)?;
            // self.latest_revision_index.insert(key_bytes, &[])?;

            let change = RecordHotChange {
                tree: String::from(self.tree_name.as_str()),
                key: generic_key,
                meta_iteration: record.meta_iteration,
                data_iteration: record.data_iteration,
                kind: ChangeKind::CreateOrChange,
            };
            self.cmd_tx
                .blocking_send(SyncClientCommand::Change(change))
                .map_err(|_| Error::Mpsc)?;

            let notification = ChangeNotification::Tree {
                key: OpaqueKey::new(self.tree_name.clone(), generic_key),
                kind: ChangeKind::CreateOrChange,
            };
            if let Err(_) = self.updates_tx.try_send(notification) {
                warn!("Notification send: mpsc fail");
            }

            Ok(())
        } else {
            Err(Error::Usage(format!(
                "update {}/{generic_key}, not found, create record first",
                self.tree_name
            )))
        }
    }

    pub fn get(&self, key: K) -> Result<V, Error> {
        let key_bytes = key.to_generic().to_bytes();
        let value = self.data.get(key_bytes)?;
        match value {
            Some(bytes) => {
                let archived_record = check_archived_root::<Record>(&bytes)?;
                let record_evolution: SimpleVersion = archived_record
                    .data_evolution
                    .deserialize(&mut rkyv::Infallible)
                    .expect("");
                if record_evolution != V::evolution() {
                    return Err(Error::EvolutionMismatch(format!(
                        "record evolution is {record_evolution} and code is {}",
                        V::evolution()
                    )));
                }

                let archived_data = check_archived_root::<Evolving<V>>(&archived_record.data)?;
                let deserialized: Evolving<V> = archived_data.deserialize(&mut rkyv::Infallible)?;
                Ok(deserialized.0)
            }
            None => Err(Error::RecordNotFound),
        }
    }

    pub fn get_archived<F: FnMut(Option<&V::Archived>) -> R, R>(
        &self,
        key: K,
        mut f: F,
    ) -> Result<R, Error> {
        let key_bytes = key.to_generic().to_bytes();
        let value = self.data.get(key_bytes)?;
        match value {
            Some(bytes) => {
                let archived_record = check_archived_root::<Record>(&bytes)?;

                let record_evolution: SimpleVersion = archived_record
                    .data_evolution
                    .deserialize(&mut rkyv::Infallible)
                    .expect("");
                if record_evolution != V::evolution() {
                    return Err(Error::EvolutionMismatch(format!(
                        "record evolution is {record_evolution} and code is {}",
                        V::evolution()
                    )));
                }

                let archived_data = check_archived_root::<Evolving<V>>(&archived_record.data)?;
                Ok(f(Some(archived_data.0.get())))
            }
            None => Ok(f(None)),
        }
    }

    pub fn remove(&mut self, key: K) -> Result<Option<()>, Error> {
        let generic_key = key.to_generic();
        if !self.is_checked_out(key) {
            return Err(Error::Usage(format!(
                "Cannot remove: {}/{generic_key} - not checked out",
                self.tree_name
            )));
        }

        let key_bytes = generic_key.to_bytes();
        let value = self.data.get(key_bytes)?;
        match value {
            Some(bytes) => {
                let archived_record = check_archived_root::<Record>(&bytes)?;
                if matches!(archived_record.meta.version, ArchivedVersion::Released(_)) {
                    return Err(Error::Usage(format!(
                        "Cannot remove released record {}/{generic_key}",
                        self.tree_name,
                    )));
                }
                for indexer in &mut self.indexers {
                    let r = indexer.update(
                        TypeErasedTree {
                            tree: &mut self.data,
                            evolution: <V as TreeRoot>::evolution(),
                        },
                        generic_key,
                        &archived_record.data,
                        crate::index::Action::Remove,
                    );
                    if r.is_err() {
                        log::error!(
                            "Indexer for {} failed at deleting with key {generic_key}",
                            self.tree_name
                        );
                    }
                }
                self.data.remove(key_bytes)?;

                let change = RecordHotChange {
                    tree: String::from(self.tree_name.as_str()),
                    key: generic_key,
                    meta_iteration: archived_record.meta_iteration,
                    data_iteration: archived_record.data_iteration,
                    kind: ChangeKind::Remove,
                };
                self.cmd_tx
                    .blocking_send(SyncClientCommand::Change(change))
                    .map_err(|_| Error::Mpsc)?;

                let notification = ChangeNotification::Tree {
                    key: OpaqueKey::new(self.tree_name.clone(), generic_key),
                    kind: ChangeKind::Remove,
                };
                if let Err(_) = self.updates_tx.try_send(notification) {
                    warn!("Notification send: mpsc fail");
                }

                Ok(Some(()))
            }
            None => Ok(None),
        }
    }

    pub fn check_out(&mut self, key: K) {
        if let Err(_) = self.cmd_tx.blocking_send(SyncClientCommand::CheckOut(
            self.tree_name.as_str().to_string(),
            key.to_generic(),
        )) {
            error!("check_out: mpsc error");
        }
    }

    pub fn release(&mut self, key: K) {
        if let Err(_) = self.cmd_tx.blocking_send(SyncClientCommand::Release(
            self.tree_name.as_str().to_string(),
            key.to_generic(),
        )) {
            error!("check_out: mpsc error");
        }
    }

    pub fn is_checked_out(&self, key: K) -> bool {
        let rd = self.borrows.blocking_read();
        if let Some(borrowed_keys) = rd.borrows.get(self.tree_name.as_str()) {
            let key = key.to_generic();
            match borrowed_keys.get(&key) {
                Some(queue) => queue.get(0) == Some(&self.uuid),
                None => false,
            }
        } else {
            false
        }
    }

    pub fn checked_out_by(&self, key: K) -> RecordCheckOutState {
        let rd = self.borrows.blocking_read();
        if let Some(borrowed_keys) = rd.borrows.get(self.tree_name.as_str()) {
            let key = key.to_generic();
            match borrowed_keys.get(&key) {
                Some(queue) => {
                    let Some(checked_out_by) = queue.get(0) else {
                        return RecordCheckOutState::Empty;
                    };
                    if checked_out_by == &self.uuid {
                        RecordCheckOutState::CheckedOut
                    } else if queue.contains(&self.uuid) {
                        RecordCheckOutState::WaitingFor(*checked_out_by)
                    } else {
                        RecordCheckOutState::CheckedOutBy(*checked_out_by)
                    }
                }
                None => RecordCheckOutState::Empty,
            }
        } else {
            RecordCheckOutState::Empty
        }
    }

    pub fn meta(&self, key: K) -> Result<Option<(u32, RecordMeta, u32, SimpleVersion)>, Error> {
        let key_bytes = key.to_generic().to_bytes();
        let value = self.data.get(key_bytes)?;
        match value {
            Some(bytes) => {
                let archived_record = check_archived_root::<Record>(&bytes)?;
                let meta: RecordMeta = archived_record.meta.deserialize(&mut rkyv::Infallible)?;
                let evolution = archived_record
                    .data_evolution
                    .deserialize(&mut rkyv::Infallible)
                    .expect("");

                Ok(Some((
                    archived_record.meta_iteration,
                    meta,
                    archived_record.data_iteration,
                    evolution,
                )))
            }
            None => Ok(None),
        }
    }

    // pub fn latest_revisions(&self) -> impl Iterator<Item = K> {
    //     todo!()
    // }

    pub fn all_revisions(&self) -> impl Iterator<Item = K> {
        self.data.iter().keys().filter_map(|key| {
            if let Ok(key) = key {
                if key == KEY_POOL {
                    return None;
                }
                let key = GenericKey::from_bytes(&key)?;
                let key = K::from_generic(key);
                Some(key)
            } else {
                warn!("Err in all_revisions");
                None
            }
        })
    }
}

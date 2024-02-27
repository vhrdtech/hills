use crate::common::{Error, ManagedTrees};
use crate::consts::SERVER_UUID;
use crate::handle_result;
use crate::index::TreeIndex;
use crate::key_pool::KeyPool;
use crate::opaque::OpaqueKey;
use crate::sync::{ArchivedEvent, ChangeKind, Event, RecordBorrows, RecordHotChange};
use crate::sync_common::{
    compare_and_request_missing_records, handle_incoming_record, present_self, send_hot_change,
    send_records, send_tree_overviews,
};
use futures_util::Sink;
use futures_util::{
    stream::{SplitSink, SplitStream},
    SinkExt, StreamExt, TryStreamExt,
};
use hills_base::GenericKey;
use log::{error, info, trace, warn};
use postage::mpsc::{channel, Receiver, Sender};
use postage::prelude::Stream;
use rkyv::{check_archived_root, to_bytes};
use sled::Db;
use std::collections::HashMap;
use std::net::IpAddr;
use std::sync::Arc;
use tokio::runtime::Runtime;
use tokio::sync::RwLock;
use tokio::task::JoinHandle;
use tokio_tungstenite::tungstenite::Message;
use uuid::Uuid;

pub(crate) struct SyncHandle {
    db: Db,
}

#[derive(Clone, Debug)]
pub enum ChangeNotification {
    Tree { key: OpaqueKey, kind: ChangeKind },
    BorrowsChanged,
}

impl SyncHandle {
    pub(crate) fn new(db: Db) -> Self {
        Self { db }
    }

    pub(crate) fn start(
        self,
        rt: &Runtime,
        updates_tx: postage::broadcast::Sender<ChangeNotification>,
        borrows: Arc<RwLock<RecordBorrows>>,
    ) -> (Sender<SyncClientCommand>, VhrdDbTelem, JoinHandle<()>) {
        let (cmd_tx, cmd_rx) = channel(64);
        let telem = SyncClientTelemetry::default();
        let telem = Arc::new(RwLock::new(telem));
        let telem_2 = telem.clone();
        let join_handle = rt
            .spawn(async move { event_loop(self.db, cmd_rx, updates_tx, telem_2, borrows).await });

        (cmd_tx, telem, join_handle)
    }
}

pub(crate) enum SyncClientCommand {
    Connect(IpAddr, u16),
    Disconnect,
    TreeCreated(String),
    RegisterIndex {
        tree_name: String,
        indexer: Box<dyn TreeIndex + Send>,
    },
    Change(RecordHotChange),
    CheckOut(String, GenericKey),
    Release(String, GenericKey),
    // FullReSync,
}

pub(crate) type VhrdDbCmdTx = Sender<SyncClientCommand>;

#[derive(Default)]
pub struct SyncClientTelemetry {
    pub connected: bool,
    pub error_message: String,
    pub bytes_sent: usize,
    pub tx_bps: usize,
    pub bytes_received: usize,
    pub rx_bps: usize,
    pub backlog: usize,
}

pub type VhrdDbTelem = Arc<RwLock<SyncClientTelemetry>>;

async fn event_loop(
    mut db: Db,
    mut cmd_rx: Receiver<SyncClientCommand>,
    mut updates_tx: postage::broadcast::Sender<ChangeNotification>,
    telem: VhrdDbTelem,
    borrows: Arc<RwLock<RecordBorrows>>,
) {
    let mut ws_txrx: Option<(SplitSink<_, _>, SplitStream<_>)> = None;
    // let mut to_replay = Vec::new();
    let mut indexers: HashMap<String, Vec<Box<dyn TreeIndex + Send>>> = HashMap::new();

    let mut server_uuid = match db.get(SERVER_UUID) {
        Ok(Some(uuid_bytes)) => {
            if uuid_bytes.len() != 16 {
                None
            } else {
                let mut uuid = [0u8; 16];
                uuid[..].copy_from_slice(&uuid_bytes);
                let uuid = Uuid::from_bytes(uuid);
                trace!("Server uuid must be {uuid}");
                Some(uuid)
            }
        }
        _ => None,
    };

    loop {
        let mut should_disconnect = false;
        if let Some((ws_tx, ws_rx)) = &mut ws_txrx {
            tokio::select! {
                message = ws_rx.try_next() => {
                    if let Ok(Some(Message::Close(_))) = &message {
                        should_disconnect = true;
                    }
                    if let Ok(Some(Message::Binary(bytes))) = message {
                        let Ok(ev) = check_archived_root::<Event>(&bytes) else {
                            error!("message unarchive failed");
                            continue
                        };
                        match ev {
                            ArchivedEvent::PresentSelf { uuid, .. } => {
                                let uuid = Uuid::from_bytes(*uuid);
                                trace!("Server uuid is: {uuid}");
                                match server_uuid {
                                    Some(server_uuid) => {
                                        if server_uuid == uuid {
                                            let r = present_self(&db, ws_tx).await;
                                            handle_result!(r);
                                            let r = send_tree_overviews(&db, ws_tx).await;
                                            handle_result!(r);
                                            let r = request_keys(&db, ws_tx).await;
                                            handle_result!(r);
                                        } else {
                                            let mut telem = telem.write().await;
                                            telem.error_message = "Server UUID does not match with the current database".to_string();
                                            warn!("{}", telem.error_message);
                                            should_disconnect = true;
                                        }
                                    }
                                    None => {
                                        server_uuid = Some(uuid);
                                        let uuid_bytes = uuid.into_bytes();
                                        let r = db.insert(SERVER_UUID, &uuid_bytes);
                                        info!("Linking this database with connected server: {}", r.is_ok());
                                        let r = present_self(&db, ws_tx).await;
                                        handle_result!(r);
                                        let r = send_tree_overviews(&db, ws_tx).await;
                                        handle_result!(r);
                                        let r = request_keys(&db, ws_tx).await;
                                        handle_result!(r);
                                    }
                                }
                            }
                            ArchivedEvent::GetTreeOverview { .. } => {}
                            ArchivedEvent::TreeOverview { tree, records } => {
                                trace!("Got {tree} overview {records:?}");
                                if let Err(e) = compare_and_request_missing_records(&db, tree, records, ws_tx, None).await {
                                    error!("tree overview: {e:?}");
                                }
                            }
                            ArchivedEvent::KeySet { tree, keys } => {
                                trace!("Got more keys for {tree} {keys:?}");
                                let Ok(tree) = db.open_tree(tree.as_str()) else {
                                    error!("key set open_tree failed");
                                    continue
                                };
                                let keys = keys.start..keys.end;
                                if let Err(e) = KeyPool::feed_for(&tree, keys).map_err(Error::Internal) {
                                    error!("key set: {e:?}");
                                }
                            }
                            ArchivedEvent::CheckedOut { tree, key, queue } => {
                                let borrows = &mut borrows.write().await.borrows;
                                let borrowed_keys = borrows.entry(tree.as_str().to_string()).or_default();
                                let queue = queue.iter().map(|uuid| Uuid::from_bytes(*uuid)).collect();
                                let key = GenericKey::from_archived(key);
                                trace!("Now checked out for {}/{}: {:?}", tree.as_str(), key, queue);
                                borrowed_keys.insert(key, queue);
                                if let Err(_) = postage::sink::Sink::send(&mut updates_tx, ChangeNotification::BorrowsChanged).await {
                                    warn!("Notification send: mpsc fail");
                                }
                            }
                            ArchivedEvent::HotSyncEvent(hot_sync_event) => {
                                let tree_name = hot_sync_event.tree_name.as_str();
                                let key = GenericKey::from_archived(&hot_sync_event.key);
                                trace!(
                                    "Got hot sync {tree_name}/{key}: {}",
                                    hot_sync_event.kind
                                );
                                if let Err(e) = handle_incoming_record(&mut db, hot_sync_event, "server", Some(&mut indexers)) {
                                    error!("hot sync event, handle_incoming_record: {e:?}");
                                }
                                let notification = ChangeNotification::Tree {
                                    key: OpaqueKey::new(Arc::new(tree_name.to_string()), key),
                                    kind: (&hot_sync_event.kind).into(),
                                };
                                if let Err(_) = postage::sink::Sink::send(&mut updates_tx, notification).await {
                                    warn!("Notification send: mpsc fail");
                                }
                            }
                            ArchivedEvent::CheckOut { .. }
                            | ArchivedEvent::Return { .. }
                            | ArchivedEvent::GetKeySet { .. } => {
                                warn!("Unsupported event from server");
                            }
                            ArchivedEvent::RequestRecords { tree, keys } => {
                                if let Err(e) = send_records(&db, tree.as_str(), keys, ws_tx, None).await {
                                    error!("send_records: {e:?}");
                                }
                            }
                        }
                    } else {
                        warn!("Unsupported ws message or None from channel, ignoring");
                        // should_disconnect = true;
                    }
                }
                cmd = cmd_rx.recv() => {
                    let Some(cmd) = cmd else {
                        info!("Sync client: tx end no longer exist, exiting");
                        break;
                    };
                    match cmd {
                        SyncClientCommand::Disconnect => {
                            should_disconnect = true;
                        }
                        SyncClientCommand::Connect(..) => {}
                        SyncClientCommand::TreeCreated(_tree_name) => {
                            let r = request_keys(&db, ws_tx).await;
                            handle_result!(r);
                        }
                        SyncClientCommand::RegisterIndex { tree_name, indexer } => {
                            indexers.entry(tree_name).or_default().push(indexer);
                        }
                        SyncClientCommand::Change(event) => {
                            trace!("{event:?}");
                            let r = send_hot_change(&db, event, ws_tx, None).await;
                            handle_result!(r);
                        }
                        SyncClientCommand::CheckOut(tree, key) => {
                            let r = check_out(tree, key, ws_tx).await;
                            handle_result!(r);
                        },
                        SyncClientCommand::Release(tree, key) => {
                            let r = release(tree, key, ws_tx).await;
                            handle_result!(r);
                        },
                    }
                }
            }
        } else {
            tokio::select! {
                cmd = cmd_rx.recv() => {
                    let Some(cmd) = cmd else {
                        info!("Sync client: tx end no longer exist, exiting");
                        break;
                    };
                    match cmd {
                        SyncClientCommand::Connect(ip_addr, port) => {
                            let url = format!("ws://{ip_addr}:{port}");
                            info!("ws: Connecting to remote {url}");
                            let ws_stream = match tokio_tungstenite::connect_async(url).await {
                                Ok((ws_stream, _)) => {
                                    let mut telem = telem.write().await;
                                    telem.connected = true;
                                    telem.error_message.clear();
                                    ws_stream
                                },
                                Err(e) => {
                                    warn!("{e:?}");
                                    let mut telem = telem.write().await;
                                    telem.connected = false;
                                    telem.error_message = format!("{e:?}");
                                    continue;
                                }
                            };
                            ws_txrx = Some(ws_stream.split());
                        }
                        SyncClientCommand::Disconnect => {}
                        SyncClientCommand::TreeCreated(_tree_name) => {
                        }
                        SyncClientCommand::RegisterIndex { tree_name, indexer } => {
                            indexers.entry(tree_name).or_default().push(indexer);
                        }
                        SyncClientCommand::Change(_event) => {
                            // to_replay.push(event);
                            telem.write().await.backlog += 1;
                        }
                        SyncClientCommand::CheckOut(tree, key) => {
                            warn!("Ignoring CheckOut {tree}/{key} because of disconnected state");
                        },
                        SyncClientCommand::Release(tree, key) => {
                            warn!("Ignoring Release {tree}/{key} because of disconnected state");
                        },
                    }
                }
            }
        }

        if should_disconnect {
            if let Some((ws_tx, ws_rx)) = ws_txrx.take() {
                if let Ok(mut ws) = ws_rx.reunite(ws_tx) {
                    let _ = ws.close(None).await;
                }
            }

            telem.write().await.connected = false;
        }
    }
}

// Cannot extract a method because of Box<dyn TreeIndex> being not Send, yet can inline just fine
// async fn process_message(
//     ws_message: Message,
//     db: &mut Db,
//     mut ws_tx: impl Sink<Message> + Unpin,
//     indexers: &HashMap<String, Vec<Box<dyn TreeIndex + Send>>>,
// ) -> Result<(), Error> {
//     Ok(())
// }

pub async fn request_keys(
    db: &Db,
    ws_tx: &mut (impl futures_util::Sink<Message> + Unpin),
) -> Result<(), Error> {
    let trees = ManagedTrees::managed(db)?;
    for tree_name in &trees {
        let tree = db.open_tree(tree_name.as_str())?;
        let available_keys = KeyPool::stats_for(&tree)?;
        trace!("request_keys: {} available: {}", tree_name, available_keys);
        if available_keys < 3 {
            let ev = Event::GetKeySet {
                tree: tree_name.to_string(),
            };
            let ev_bytes = to_bytes::<_, 128>(&ev)?;
            ws_tx
                .feed(Message::Binary(ev_bytes.to_vec()))
                .await
                .map_err(|_| Error::Ws)?;
        }
    }
    ws_tx.flush().await.map_err(|_| Error::Ws)?;
    Ok(())
}

async fn check_out(
    tree: String,
    key: GenericKey,
    tx: &mut (impl Sink<Message> + Unpin),
) -> Result<(), Error> {
    let event = Event::CheckOut {
        tree,
        keys: vec![key],
    };
    let id_event = to_bytes::<_, 8>(&event)?;
    tx.feed(Message::Binary(id_event.to_vec()))
        .await
        .map_err(|_| Error::Ws)?;

    tx.flush().await.map_err(|_| Error::Ws)?;
    Ok(())
}

async fn release(
    tree: String,
    key: GenericKey,
    tx: &mut (impl Sink<Message> + Unpin),
) -> Result<(), Error> {
    let event = Event::Return {
        tree,
        keys: vec![key],
    };
    let id_event = to_bytes::<_, 8>(&event)?;
    tx.feed(Message::Binary(id_event.to_vec()))
        .await
        .map_err(|_| Error::Ws)?;

    tx.flush().await.map_err(|_| Error::Ws)?;
    Ok(())
}

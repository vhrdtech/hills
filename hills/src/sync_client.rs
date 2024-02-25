use crate::common::{Error, ManagedTrees};
use crate::consts::SERVER_UUID;
use crate::handle_result;
use crate::index::TreeIndex;
use crate::key_pool::KeyPool;
use crate::opaque::OpaqueKey;
use crate::sync::{ArchivedEvent, ChangeKind, Event, RecordHotChange};
use crate::sync_common::{
    compare_and_request_missing_records, handle_incoming_record, present_self, send_hot_change,
    send_records, send_tree_overviews,
};
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
use std::sync::{Arc, RwLock};
use tokio::runtime::Runtime;
use tokio::task::JoinHandle;
use tokio_tungstenite::tungstenite::Message;
use uuid::Uuid;

pub struct VhrdDbSyncHandle {
    db: Db,
    event_rx: Receiver<RecordHotChange>,
}

#[derive(Clone, Debug)]
pub enum ChangeNotification {
    Tree { key: OpaqueKey, kind: ChangeKind },
    Borrowed,
}

impl VhrdDbSyncHandle {
    pub(crate) fn new(db: Db, rx: Receiver<RecordHotChange>) -> Self {
        Self { db, event_rx: rx }
    }

    pub(crate) fn start(
        self,
        rt: &Runtime,
        updates_tx: postage::broadcast::Sender<ChangeNotification>,
    ) -> (Sender<SyncClientCommand>, VhrdDbTelem, JoinHandle<()>) {
        let (cmd_tx, cmd_rx) = channel(64);
        let telem = SyncClientTelemetry::default();
        let telem = Arc::new(RwLock::new(telem));
        let telem_2 = telem.clone();
        let join_handle = rt.spawn(async move {
            event_loop(self.db, self.event_rx, cmd_rx, updates_tx, telem_2).await
        });

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
    mut event_rx: Receiver<RecordHotChange>,
    mut cmd_rx: Receiver<SyncClientCommand>,
    mut updates_tx: postage::broadcast::Sender<ChangeNotification>,
    telem: VhrdDbTelem,
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
                trace!("Server uuid must be {uuid:?}");
                Some(Uuid::from_bytes(uuid))
            }
        }
        _ => None,
    };

    loop {
        let mut should_disconnect = false;
        if let Some((ws_tx, ws_rx)) = &mut ws_txrx {
            tokio::select! {
                message = ws_rx.try_next() => {
                    match message {
                        Ok(Some(message)) => {
                            if let Message::Close(_) = &message {
                                should_disconnect = true;
                            }
                            // let r = process_message(message, &mut db, ws_tx, &indexers).await;
                            match message {
                                Message::Binary(bytes) => {
                                    let Ok(ev) = check_archived_root::<Event>(&bytes) else {
                                        error!("message unarchive failed");
                                        continue
                                    };
                                    match ev {
                                        ArchivedEvent::PresentSelf { uuid, .. } => {
                                            trace!("Server uuid is: {uuid:x?}");
                                            let uuid = Uuid::from_bytes(*uuid);
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
                                                        if let Ok(mut wr) = telem.write() {
                                                            wr.error_message = "Server UUID does not match with the current database".to_string();
                                                            warn!("{}", wr.error_message);
                                                        }
                                                        should_disconnect = true;
                                                    }
                                                }
                                                None => {
                                                    server_uuid = Some(uuid);
                                                    let uuid_bytes = uuid.into_bytes();
                                                    let r = db.insert(SERVER_UUID, &uuid_bytes);
                                                    info!("Linking this database with connected server: {r:?}");
                                                }
                                            }
                                        }
                                        ArchivedEvent::GetTreeOverview { .. } => {}
                                        ArchivedEvent::TreeOverview { tree, records } => {
                                            trace!("Got {tree} overview {records:?}");
                                            if let Err(e) = compare_and_request_missing_records(&db, tree, records, ws_tx, &[]).await {
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
                                        ArchivedEvent::CheckedOut { .. } => {}
                                        ArchivedEvent::AlreadyCheckedOut { .. } => {}
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
                                }
                                u => {
                                    warn!("Unsupported ws message: {u:?}");
                                }
                            }
                        }
                        Ok(None) => {
                            warn!("ws_tx: None");
                            should_disconnect = true;
                        }
                        Err(e) => {
                            warn!("ws_rx: {e}");
                            should_disconnect = true;
                        }
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
                    }
                }
                event = event_rx.recv() => {
                    if let Some(event) = event {
                        trace!("{event:?}");
                        let r = send_hot_change(&db, event, ws_tx, None).await;
                        handle_result!(r);
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
                                    if let Ok(mut telem) = telem.write() {
                                        telem.connected = true;
                                        telem.error_message.clear();
                                    }
                                    ws_stream
                                },
                                Err(e) => {
                                    warn!("{e:?}");
                                    if let Ok(mut telem) = telem.write() {
                                        telem.connected = false;
                                        telem.error_message = format!("{e:?}");
                                    }
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
                    }
                }
                event = event_rx.recv() => {
                    trace!("ev: {event:?}");
                    if let Some(_event) = event {
                        // to_replay.push(event);
                        if let Ok(mut telem) = telem.write() {
                            telem.backlog += 1;
                        }
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

            if let Ok(mut telem) = telem.write() {
                telem.connected = false;
            }
        }
    }
}

// async fn process_message(
//     ws_message: Message,
//     db: &mut Db,
//     mut ws_tx: impl Sink<Message> + Unpin,
//     indexers: &HashMap<String, Vec<Box<dyn TreeIndex + Send>>>,
// ) -> Result<(), Error> {
//     match ws_message {
//         Message::Binary(bytes) => {
//             let ev = check_archived_root::<Event>(&bytes)?;
//             match ev {
//                 ArchivedEvent::PresentSelf { uuid } => {
//                     trace!("Server uuid is: {uuid:x?}");
//                 }
//                 ArchivedEvent::Subscribe { .. } => {}
//                 ArchivedEvent::GetTreeOverview { .. } => {}
//                 ArchivedEvent::TreeOverview { tree, records } => {
//                     trace!("Got {tree} overview {records:?}");
//                     compare_and_request_missing_records(db, tree, records, &mut ws_tx).await?;
//                 }
//                 ArchivedEvent::KeySet { tree, keys } => {
//                     trace!("Got more keys for {tree} {keys:?}");
//                     let tree = db.open_tree(tree.as_str())?;
//                     let keys = keys.start..keys.end;
//                     KeyPool::feed_for(&tree, keys).map_err(Error::Internal)?;
//                 }
//                 ArchivedEvent::CheckedOut { .. } => {}
//                 ArchivedEvent::AlreadyCheckedOut { .. } => {}
//                 ArchivedEvent::HotSyncEvent(hot_sync_event) => {
//                     handle_incoming_record(db, hot_sync_event, "server")?;
//                 }
//                 ArchivedEvent::CheckOut { .. }
//                 | ArchivedEvent::Return { .. }
//                 | ArchivedEvent::GetKeySet { .. } => {
//                     warn!("Unsupported event from server");
//                 }
//                 ArchivedEvent::RequestRecords { tree, keys } => {
//                     send_records(db, tree.as_str(), keys, &mut ws_tx).await?;
//                 }
//                 ArchivedEvent::RecordAsRequested {
//                     tree_name,
//                     key,
//                     record,
//                 } => {
//                     let tree = db.open_tree(tree_name.as_str())?;
//                     let key = GenericKey::from_archived(key);
//                     let key_bytes = key.to_bytes();
//                     if tree.contains_key(key_bytes)? {
//                         warn!("{tree_name}/{key} already exist");
//                     } else {
//                         tree.insert(key_bytes, record.as_slice())?;
//                         trace!("{tree_name}/{key} inserted");
//                     }
//                 }
//             }
//         }
//         u => {
//             warn!("Unsupported ws message: {u:?}");
//         }
//     }

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

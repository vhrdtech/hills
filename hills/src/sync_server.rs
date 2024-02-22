use crate::common::{Error, ManagedTrees};
use crate::consts::SELF_UUID;
use crate::record::{Record, RecordMeta};
use crate::sync::{
    ArchivedEvent, ArchivedHotSyncEventKind, Event, HotSyncEvent, HotSyncEventKind, RecordHotChange,
};
use crate::sync_common::{
    compare_and_request_missing_records, present_self, send_records, send_tree_overviews,
};
use crate::{handle_result, sync_common};
use futures_util::{Sink, SinkExt, Stream, StreamExt, TryStreamExt};
use hills_base::GenericKey;
use log::{error, info, trace, warn};
use rkyv::{check_archived_root, to_bytes, Archive, Deserialize, Serialize};
use sled::Db;
use std::collections::{HashMap, HashSet};
use std::net::SocketAddr;
use std::ops::Range;
use std::path::Path;
use std::sync::Arc;
use tokio::net::{TcpListener, ToSocketAddrs};
use tokio::runtime::Runtime;
use tokio::sync::RwLock;
use tokio::task::JoinHandle;
use tokio_tungstenite::tungstenite::Message;
use uuid::Uuid;

pub struct HillsServer {
    pub join: JoinHandle<()>,
}

#[derive(Archive, Default, Debug, Serialize, Deserialize)]
#[archive(check_bytes)]
#[archive_attr(derive(Debug))]
struct ClientInfo {
    uuid: [u8; 16],
    /// Issued key ranges for each tree
    key_ranges: HashMap<String, Vec<Range<u32>>>,
    /// Which trees a client is subscribed to
    subscribed_to: HashSet<String>,
    readable_name: String,
    to_replay: Vec<RecordHotChange>,
}

impl ClientInfo {
    fn owns_key(&self, tree: impl AsRef<str>, key: GenericKey) -> bool {
        if let Some(ranges) = self.key_ranges.get(tree.as_ref()) {
            for r in ranges {
                if r.contains(&key.id) {
                    return true;
                }
            }
        }
        false
    }
}

#[derive(Archive, Default, Serialize, Deserialize)]
#[archive(check_bytes)]
#[archive_attr(derive(Debug))]
struct TreeInfo {
    next_key: u32,
    // checked_out
    _dummy22: [u8; 18],
}

struct State {
    remote_addr: SocketAddr,
    info: Option<ClientInfo>,
}

impl HillsServer {
    pub fn start<P: AsRef<Path>, A: ToSocketAddrs + Send + 'static>(
        path: P,
        addr: A,
        rt: &Runtime,
    ) -> Result<Self, Error> {
        #[cfg(not(test))]
        let db = sled::open(path)?;
        #[cfg(test)]
        let db = sled::Config::new().temporary(true).path(path).open()?;

        if !db.contains_key(SELF_UUID)? {
            let uuid = Uuid::new_v4();
            trace!("Created new server db, uuid={uuid}");
            let uuid_bytes = uuid.into_bytes();
            db.insert(SELF_UUID, &uuid_bytes)?;
        }

        let join = rt.spawn(async move {
            let listener = TcpListener::bind(addr).await.unwrap();
            ws_server_acceptor(listener, db).await;
        });

        Ok(HillsServer { join })
    }
}

async fn ws_server_acceptor(listener: TcpListener, db: Db) {
    info!("Server event loop started");
    let (broadcast_tx, broadcast_rx) = postage::broadcast::channel(256);
    drop(broadcast_rx);
    let removed = Arc::new(RwLock::new(HashMap::new()));
    loop {
        match listener.accept().await {
            Ok((tcp_stream, remote_addr)) => {
                info!("Got new connection from: {remote_addr}");
                let ws_stream = match tokio_tungstenite::accept_async(tcp_stream).await {
                    Ok(ws_stream) => ws_stream,
                    Err(e) => {
                        warn!("Error during the websocket handshake occurred {e:?}");
                        continue;
                    }
                };

                let (ws_sink, ws_source) = StreamExt::split(ws_stream);

                let db_clone = db.clone();
                let state = State {
                    remote_addr,
                    info: None,
                };
                let rx = broadcast_tx.subscribe();
                let tx = broadcast_tx.clone();
                let removed = removed.clone();
                tokio::spawn(async move {
                    ws_event_loop(ws_sink, ws_source, db_clone, state, rx, tx, removed).await
                });
            }
            Err(e) => {
                warn!("{e:?}");
            }
        }
    }
}

async fn ws_event_loop(
    mut ws_tx: impl Sink<Message> + Unpin,
    mut ws_rx: impl Stream<Item = Result<Message, tokio_tungstenite::tungstenite::Error>> + Unpin,
    mut db: Db,
    mut state: State,
    mut broadcast_rx: postage::broadcast::Receiver<HotSyncEvent>,
    mut broadcast_tx: postage::broadcast::Sender<HotSyncEvent>,
    removed: Arc<RwLock<HashMap<String, Vec<GenericKey>>>>,
) {
    info!("Event loop for {}: started", state.remote_addr);
    let r = present_self(&db, &mut ws_tx).await;
    handle_result!(r);
    let r = send_tree_overviews(&db, &mut ws_tx).await;
    handle_result!(r);

    use postage::prelude::Stream;

    loop {
        tokio::select! {
            message = ws_rx.try_next() => {
                match message {
                    Ok(Some(message)) => {
                        if let Message::Close(_) = &message {
                            break;
                        }
                        let r = process_message(message, &mut ws_tx, &mut db, &mut state, &mut broadcast_tx, &removed).await;
                        handle_result!(r);
                    }
                    Ok(None) => {
                        break;
                    }
                    Err(e) => {
                        warn!("{e}");
                        break;
                    }
                }
            }
            event = broadcast_rx.recv() => {
                let Some(event) = event else {
                    warn!("broadcast_rx returned None {}", state.remote_addr);
                    continue
                };
                // trace!("hot change from {:?} in {} event loop", event.source_addr, state.remote_addr);
                if event.source_addr != Some(state.remote_addr) {
                    trace!("relaying event to {}", state.remote_addr);
                    let ev_bytes = to_bytes::<_, 128>(&Event::HotSyncEvent(event)).unwrap();
                    let r = ws_tx.send(Message::Binary(ev_bytes.to_vec())).await;
                    if r.is_err() {
                        warn!("relay error");
                    }
                }
            }
        }
    }

    info!("Event loop {}: exiting", state.remote_addr);
}

async fn process_message(
    ws_message: Message,
    mut ws_tx: impl Sink<Message> + Unpin,
    db: &mut Db,
    state: &mut State,
    broadcast_tx: &mut postage::broadcast::Sender<HotSyncEvent>,
    removed: &Arc<RwLock<HashMap<String, Vec<GenericKey>>>>,
) -> Result<(), Error> {
    use postage::prelude::Sink;
    match ws_message {
        Message::Binary(bytes) => {
            let client_event = check_archived_root::<Event>(&bytes)?;
            match client_event {
                ArchivedEvent::PresentSelf { uuid } => {
                    trace!("Client presenting uuid: {uuid:x?}");
                    let clients = db.open_tree("clients")?;
                    let client_info = if let Some(client_info_bytes) = clients.get(uuid)? {
                        let client_info = check_archived_root::<ClientInfo>(&client_info_bytes)?;
                        let client_info: ClientInfo =
                            client_info.deserialize(&mut rkyv::Infallible).expect("");
                        trace!("Known client {client_info:?}");
                        client_info
                    } else {
                        trace!("New client {} {uuid:x?}", state.remote_addr);
                        let client_info = ClientInfo {
                            uuid: *uuid,
                            readable_name: state.remote_addr.to_string(),
                            ..Default::default()
                        };
                        let client_info_bytes = to_bytes::<_, 128>(&client_info)?;
                        clients.insert(uuid, client_info_bytes.as_slice())?;
                        client_info
                    };
                    state.info = Some(client_info);
                }
                ArchivedEvent::Subscribe { trees } => {
                    trace!("{} subscribes to {trees:?}", state.remote_addr);
                    if let Some(info) = &mut state.info {
                        for tree in trees.iter() {
                            info.subscribed_to.insert(tree.to_string());
                        }
                    }
                }
                ArchivedEvent::GetTreeOverview { .. } => {}
                ArchivedEvent::TreeOverview { tree, records } => {
                    trace!("Got {}/{tree} overview {records:?}", state.remote_addr);
                    let removed = removed
                        .read()
                        .await
                        .get(tree.as_str())
                        .cloned()
                        .unwrap_or_default();
                    let found_in_removed = compare_and_request_missing_records(
                        db, tree, records, &mut ws_tx, &removed,
                    )
                    .await?;
                    if !found_in_removed.is_empty() {
                        trace!("To be removed on client: {found_in_removed:?}");
                    }
                    for key in found_in_removed {
                        let ev = Event::HotSyncEvent(HotSyncEvent {
                            tree_name: tree.to_string(),
                            key,
                            source_addr: None,
                            kind: HotSyncEventKind::Removed,
                        });
                        let ev_bytes = to_bytes::<_, 128>(&ev)?;
                        ws_tx
                            .send(Message::Binary(ev_bytes.to_vec()))
                            .await
                            .map_err(|_| Error::Ws)?;
                    }
                }
                ArchivedEvent::GetKeySet { tree } => {
                    let key_count = 1000;
                    trace!("{}: GetKeySet for {tree}", state.remote_addr);
                    let Some(client_info) = &mut state.info else {
                        return Ok(());
                    };
                    // TODO: use transaction here, but only access through tx_db in the closure
                    // let next_key = db.transaction::<_, _, Error>(|db_tx| {
                    let next_key = {
                        let key = format!("{tree}_info");
                        if let Some(tree_info_bytes) = db.get(key.as_bytes())? {
                            let tree_info = check_archived_root::<TreeInfo>(&tree_info_bytes)?;
                            let next_key: u32 = tree_info.next_key;
                            trace!("next_key is {next_key}");
                            let tree_info = TreeInfo {
                                next_key: next_key + key_count,
                                ..Default::default()
                            };
                            let tree_info_bytes = to_bytes::<_, 0>(&tree_info)?;
                            db.insert(key.as_bytes(), tree_info_bytes.as_slice())?;
                            next_key
                        } else {
                            trace!("New tree {tree}");
                            ManagedTrees::add_to_managed(db, tree)?;
                            let tree_info = TreeInfo {
                                next_key: key_count,
                                ..Default::default()
                            };
                            let tree_info_bytes = to_bytes::<_, 0>(&tree_info)?;
                            db.insert(key.as_bytes(), tree_info_bytes.as_slice())?;
                            0u32
                        }
                        // }).unwrap();
                    };
                    let new_range = next_key..next_key + key_count;
                    let ev = Event::KeySet {
                        tree: tree.to_string(),
                        keys: new_range.clone(),
                    };

                    client_info
                        .key_ranges
                        .entry(tree.to_string())
                        .and_modify(|ranges| ranges.push(new_range.clone()))
                        .or_insert(vec![new_range]);

                    let client_info_bytes = to_bytes::<_, 128>(client_info)?;
                    let clients = db.open_tree("clients")?;
                    clients.insert(client_info.uuid, client_info_bytes.as_slice())?;

                    trace!(
                        "issued: {}/{}..{} to {}",
                        tree,
                        next_key,
                        next_key + key_count,
                        state.remote_addr
                    );
                    let ev_bytes = to_bytes::<_, 128>(&ev)?;
                    ws_tx
                        .send(Message::Binary(ev_bytes.to_vec()))
                        .await
                        .map_err(|_| Error::Ws)?;
                }
                ArchivedEvent::CheckOut { .. } => {}
                ArchivedEvent::Return { .. } => {}
                ArchivedEvent::KeySet { .. }
                | ArchivedEvent::CheckedOut { .. }
                | ArchivedEvent::AlreadyCheckedOut { .. } => {
                    warn!("{}: wrong message", state.remote_addr);
                }
                ArchivedEvent::HotSyncEvent(hot_sync_event) => {
                    let Some(client_info) = &state.info else {
                        error!("Record hot update ignored, because client is not registered yet");
                        return Ok(());
                    };
                    let tree_name = hot_sync_event.tree_name.as_str();
                    let key = GenericKey::from_archived(&hot_sync_event.key);
                    let remote_name = format!("{}", state.remote_addr);
                    trace!(
                        "Got hot sync from {remote_name}/{tree_name}/{key}: {}",
                        hot_sync_event.kind
                    );
                    match hot_sync_event.kind {
                        ArchivedHotSyncEventKind::Created { .. } => {
                            if !client_info.owns_key(tree_name, key) {
                                warn!(
                                    "{} tried to create a record with id {} it doesn't own",
                                    remote_name, key
                                );
                                return Ok(());
                            }
                        }
                        ArchivedHotSyncEventKind::Removed => {
                            removed
                                .write()
                                .await
                                .entry(tree_name.to_string())
                                .or_default()
                                .push(key);
                        }
                        _ => {}
                    }
                    sync_common::handle_incoming_record(db, hot_sync_event, &remote_name, None)?;
                    let mut hot_sync_event_owned: HotSyncEvent =
                        hot_sync_event.deserialize(&mut rkyv::Infallible).expect("");
                    hot_sync_event_owned.source_addr = Some(state.remote_addr);
                    broadcast_tx
                        .send(hot_sync_event_owned)
                        .await
                        .map_err(|_| Error::PostageBroadcast)?;
                }
                ArchivedEvent::RequestRecords { tree, keys } => {
                    send_records(db, tree.as_str(), keys, &mut ws_tx).await?;
                }
                ArchivedEvent::RecordAsRequested {
                    tree_name,
                    key,
                    record,
                } => {
                    let tree = db.open_tree(tree_name.as_str())?;
                    let key = GenericKey::from_archived(key);
                    let key_bytes = key.to_bytes();
                    if tree.contains_key(key_bytes)? {
                        warn!("{tree_name}/{key} already exist");
                    } else {
                        tree.insert(key_bytes, record.as_slice())?;
                        trace!("{tree_name}/{key} inserted");

                        let record = check_archived_root::<Record>(record)?;
                        let meta: RecordMeta =
                            record.meta.deserialize(&mut rkyv::Infallible).expect("");
                        let evolution = record
                            .data_evolution
                            .deserialize(&mut rkyv::Infallible)
                            .expect("");
                        let data = record.data.to_vec();
                        let hot_change_ev = HotSyncEvent {
                            tree_name: tree_name.to_string(),
                            key,
                            source_addr: Some(state.remote_addr),
                            kind: HotSyncEventKind::Created {
                                meta,
                                meta_iteration: record.meta_iteration,
                                data,
                                data_iteration: record.data_iteration,
                                data_evolution: evolution,
                            },
                        };
                        broadcast_tx
                            .send(hot_change_ev)
                            .await
                            .map_err(|_| Error::PostageBroadcast)?;
                        trace!("Relayed cold change to other connected clients");
                    }
                }
            }
        }
        u => {
            warn!("Unsupported ws message: {u:?}");
        }
    }

    Ok(())
}

// pub(crate) async fn serialize_and_send(ev: AddressableEvent, ws_sink: impl Sink<Message>) -> bool {
//     let mut buf = Vec::new();
//     match serde::Serialize::serialize(&ev.event, &mut rmp_serde::Serializer::new(&mut buf)) {
//         Ok(()) => match ws_sink.send(Message::Binary(buf)).await {
//             Ok(_) => {}
//             Err(_) => {
//                 error!("ws send error");
//                 return true;
//             }
//         },
//         Err(e) => {
//             error!("rmp serialize error {e:?}");
//         }
//     }
//     false
// }

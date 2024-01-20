use crate::handle_result;
use crate::record::{Record, RecordMeta};
use crate::sync::{ArchivedEvent, Event, RecordHotChange};
use crate::sync_common::{present_self, Error, SELF_UUID};
use futures_util::{Sink, SinkExt, Stream, StreamExt, TryStreamExt};
use hills_base::GenericKey;
use log::{error, info, trace, warn};
use rkyv::{
    archived_root, check_archived_root, to_bytes, AlignedVec, Archive, Deserialize, Serialize,
};
use sled::transaction::ConflictableTransactionError;
use sled::Db;
use std::collections::{HashMap, HashSet};
use std::net::SocketAddr;
use std::ops::Range;
use std::path::Path;
use tokio::net::TcpListener;
use tokio::runtime::Runtime;
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
    pub fn start<P: AsRef<Path>>(path: P, rt: &Runtime) -> Result<Self, Error> {
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
            let listener = TcpListener::bind("127.0.0.1:8080").await.unwrap();
            ws_server_acceptor(listener, db).await;
        });

        Ok(HillsServer { join })
    }
}

async fn ws_server_acceptor(listener: TcpListener, db: Db) {
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
                tokio::spawn(
                    async move { ws_event_loop(ws_sink, ws_source, db_clone, state).await },
                );
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
) {
    info!("Event loop started");
    let r = present_self(&db, &mut ws_tx).await;
    handle_result!(r);
    loop {
        tokio::select! {
            message = ws_rx.try_next() => {
                match message {
                    Ok(Some(message)) => {
                        if let Message::Close(_) = &message {
                            break;
                        }
                        let r = process_message(message, &mut ws_tx, &mut db, &mut state).await;
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
        }
    }

    info!("Event loop: exiting");
}

async fn process_message(
    ws_message: Message,
    mut ws_tx: impl Sink<Message> + Unpin,
    db: &mut Db,
    state: &mut State,
) -> Result<(), Error> {
    match ws_message {
        Message::Binary(bytes) => {
            let client_event = unsafe { archived_root::<Event>(&bytes) };
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
                ArchivedEvent::TreeOverview { .. } => {}
                ArchivedEvent::GetKeySet { tree } => {
                    let key_count = 10;
                    trace!("{}: GetKeySet for {tree}", state.remote_addr);
                    let Some(client_info) = &mut state.info else {
                        return Ok(());
                    };
                    let next_key = db
                        .transaction(|db_tx| {
                            let key = format!("{tree}_info");
                            if let Some(tree_info_bytes) = db_tx.get(key.as_bytes())? {
                                let tree_info = check_archived_root::<TreeInfo>(&tree_info_bytes)
                                    .map_err(ConflictableTransactionError::Abort)?;
                                let next_key: u32 = tree_info.next_key;
                                let tree_info = TreeInfo {
                                    next_key: next_key + key_count,
                                    ..Default::default()
                                };
                                let tree_info_bytes = to_bytes::<_, 0>(&tree_info).unwrap();
                                db_tx.insert(key.as_bytes(), tree_info_bytes.as_slice())?;
                                Ok(next_key)
                            } else {
                                trace!("New tree {tree}");
                                let tree_info = TreeInfo::default();
                                let tree_info_bytes = to_bytes::<_, 0>(&tree_info).unwrap();
                                db_tx.insert(key.as_bytes(), tree_info_bytes.as_slice())?;
                                Ok(0u32)
                            }
                        })
                        .map_err(|_| Error::Internal("sled transaction failed".into()))?;
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
                ArchivedEvent::RecordCreated { tree, key, .. }
                | ArchivedEvent::RecordMetaChanged { tree, key, .. }
                | ArchivedEvent::RecordChanged { tree, key, .. }
                | ArchivedEvent::RecordRemoved { tree, key, .. } => {
                    let Some(client_info) = &state.info else {
                        error!("Record hot update ignored, because client is not registered yet");
                        return Ok(());
                    };
                    let key = GenericKey::new(key.id, key.revision);
                    if let ArchivedEvent::RecordCreated { .. } = client_event {
                        if !client_info.owns_key(tree, key) {
                            warn!(
                                "{} tried to create a record with invalid id {}",
                                state.remote_addr, key
                            );
                            return Ok(());
                        }
                    }
                    let key_bytes = key.to_bytes();
                    let db_tree = db.open_tree(tree.as_str())?;
                    match client_event {
                        ArchivedEvent::RecordCreated { meta, data, .. } => {
                            let mut new_data = AlignedVec::new();
                            let meta: RecordMeta =
                                meta.deserialize(&mut rkyv::Infallible).expect("");
                            new_data.extend_from_slice(data.as_slice());
                            let record = Record {
                                meta_iteration: 0,
                                meta,
                                data_iteration: 0,
                                data: new_data,
                            };
                            let record_bytes = to_bytes::<_, 128>(&record)?;
                            db.insert(&key_bytes, record_bytes.as_slice())?;
                            trace!("created record {}/{}", tree, key);
                        }
                        ArchivedEvent::RecordMetaChanged {
                            meta,
                            meta_iteration,
                            ..
                        }
                        | ArchivedEvent::RecordChanged {
                            meta,
                            meta_iteration,
                            ..
                        } => {
                            let Some(record) = db_tree.get(key_bytes)? else {
                                error!(
                                    "{} tried to modify non-existing record: {}/{}",
                                    state.remote_addr, tree, key
                                );
                                return Ok(());
                            };
                            let old_record = unsafe { archived_root::<Record>(&record) };
                            let meta: RecordMeta =
                                meta.deserialize(&mut rkyv::Infallible).expect("");

                            match client_event {
                                ArchivedEvent::RecordMetaChanged { .. } => {
                                    let mut old_data = AlignedVec::new();
                                    old_data.extend_from_slice(old_record.data.as_slice());
                                    let record = Record {
                                        meta_iteration: *meta_iteration,
                                        meta,
                                        data_iteration: old_record.data_iteration,
                                        data: old_data,
                                    };
                                    let record_bytes = to_bytes::<_, 128>(&record)?;
                                    db.insert(&key_bytes, record_bytes.as_slice())?;
                                    trace!(
                                        "updated meta {}/{} m.it{}->{}",
                                        tree,
                                        key,
                                        old_record.meta_iteration,
                                        meta_iteration
                                    );
                                }
                                ArchivedEvent::RecordChanged {
                                    data,
                                    data_iteration,
                                    ..
                                } => {
                                    let mut new_data = AlignedVec::new();
                                    new_data.extend_from_slice(data.as_slice());
                                    let record = Record {
                                        meta_iteration: *meta_iteration,
                                        meta,
                                        data_iteration: *data_iteration,
                                        data: new_data,
                                    };
                                    let record_bytes = to_bytes::<_, 128>(&record)?;
                                    db.insert(&key_bytes, record_bytes.as_slice())?;
                                    trace!(
                                        "updated record {}/{} d.it{}->{}",
                                        tree,
                                        key,
                                        old_record.data_iteration,
                                        data_iteration
                                    );
                                }
                                _ => unreachable!(),
                            }
                        }
                        ArchivedEvent::RecordRemoved { .. } => {
                            if db_tree.remove(&key_bytes)?.is_none() {
                                warn!(
                                    "{} tried to remove non-existing record: {}/{}",
                                    state.remote_addr, tree, key
                                );
                                return Ok(());
                            }
                        }
                        _ => unreachable!(),
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

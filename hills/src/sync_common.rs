use crate::common::{Error, ManagedTrees};
use crate::consts::{KEY_POOL, SELF_UUID};
use crate::index::{Action, TreeIndex, TypeErasedTree};
use crate::record::{Record, RecordMeta};
use crate::sync::{
    ArchivedHotSyncEvent, ArchivedHotSyncEventKind, ArchivedRecordIteration, ChangeKind, Event,
    HotSyncEvent, HotSyncEventKind, RecordHotChange, RecordIteration,
};
use futures_util::{Sink, SinkExt};
use hills_base::generic_key::ArchivedGenericKey;
use hills_base::GenericKey;
use log::{error, trace, warn};
use rkyv::collections::ArchivedHashMap;
use rkyv::vec::ArchivedVec;
use rkyv::{check_archived_root, to_bytes, AlignedVec, Deserialize};
use sled::Db;
use std::collections::HashMap;
use std::net::SocketAddr;
use tokio_tungstenite::tungstenite::Message;

pub async fn present_self(db: &Db, tx: &mut (impl Sink<Message> + Unpin)) -> Result<(), Error> {
    let Some(uuid_bytes) = db.get(SELF_UUID)? else {
        return Err(Error::Internal("self uuid is absent".into()));
    };
    if uuid_bytes.len() != 16 {
        return Err(Error::Internal("self uuid is not 16B long".into()));
    }
    let mut uuid = [0u8; 16];
    uuid[..].copy_from_slice(&uuid_bytes);
    let id_event = Event::PresentSelf { uuid };
    let id_event = to_bytes::<_, 8>(&id_event)?;
    tx.feed(Message::Binary(id_event.to_vec()))
        .await
        .map_err(|_| Error::Ws)?;

    tx.flush().await.map_err(|_| Error::Ws)?;
    Ok(())
}

pub async fn send_hot_change(
    db: &Db,
    change: RecordHotChange,
    ws_tx: &mut (impl Sink<Message> + Unpin),
    source_addr: Option<SocketAddr>,
) -> Result<(), Error> {
    trace!(
        "send_hot_change: {:?} for {}/{} m{} d{}",
        change.kind,
        change.tree,
        change.key,
        change.meta_iteration,
        change.data_iteration
    );
    let tree = db.open_tree(change.tree.as_str())?;
    let hot_change_ev = match change.kind {
        ChangeKind::Create | ChangeKind::ModifyMeta | ChangeKind::ModifyBoth => {
            let Some(record_bytes) = tree.get(change.key.to_bytes())? else {
                error!(
                    "send_hot_change for {}/{}: record do not actually exist",
                    change.tree, change.key
                );
                return Ok(());
            };
            let record = check_archived_root::<Record>(&record_bytes)?;
            let meta: RecordMeta = record.meta.deserialize(&mut rkyv::Infallible).expect("");
            let evolution = record
                .data_evolution
                .deserialize(&mut rkyv::Infallible)
                .expect("");
            match change.kind {
                ChangeKind::Create | ChangeKind::ModifyBoth => {
                    let data = record.data.to_vec();
                    match change.kind {
                        ChangeKind::Create => HotSyncEvent {
                            tree_name: change.tree,
                            key: change.key,
                            source_addr,
                            kind: HotSyncEventKind::Created {
                                meta,
                                meta_iteration: 0,
                                data,
                                data_iteration: 0,
                                data_evolution: evolution,
                            },
                        },
                        ChangeKind::ModifyBoth => HotSyncEvent {
                            tree_name: change.tree,
                            key: change.key,
                            source_addr,
                            kind: HotSyncEventKind::Changed {
                                meta,
                                meta_iteration: record.meta_iteration,
                                data,
                                data_iteration: record.data_iteration,
                                data_evolution: evolution,
                            },
                        },
                        _ => unreachable!(),
                    }
                }
                ChangeKind::ModifyMeta => HotSyncEvent {
                    tree_name: change.tree,
                    key: change.key,
                    source_addr,
                    kind: HotSyncEventKind::MetaChanged {
                        meta,
                        meta_iteration: record.meta_iteration,
                    },
                },
                _ => unreachable!(),
            }
        }
        ChangeKind::Remove => HotSyncEvent {
            tree_name: change.tree,
            key: change.key,
            source_addr,
            kind: HotSyncEventKind::Removed,
        },
    };
    let ev_bytes = to_bytes::<_, 128>(&Event::HotSyncEvent(hot_change_ev))?;
    ws_tx
        .send(Message::Binary(ev_bytes.to_vec()))
        .await
        .map_err(|_| Error::Ws)?;
    Ok(())
}

pub async fn send_tree_overviews(
    db: &Db,
    ws_tx: &mut (impl Sink<Message> + Unpin),
) -> Result<(), Error> {
    let trees = ManagedTrees::managed(db)?;
    for tree_name in trees {
        let tree = db.open_tree(&tree_name)?;
        let mut records = HashMap::new();
        for db_record in tree.iter() {
            let (key_bytes, record_bytes) = db_record?;
            if key_bytes == KEY_POOL {
                continue;
            }
            let Some(key) = GenericKey::from_bytes(&key_bytes) else {
                return Err(Error::Internal(
                    "Malformed key in tree {tree_name}: {key_bytes:?}".into(),
                ));
            };
            let record = check_archived_root::<Record>(&record_bytes)?;
            records.insert(
                key,
                RecordIteration {
                    meta_iteration: record.meta_iteration,
                    data_iteration: record.data_iteration,
                },
            );
        }
        let ev = Event::TreeOverview {
            tree: tree_name,
            records,
        };
        let ev_bytes = to_bytes::<_, 128>(&ev)?;
        ws_tx
            .send(Message::Binary(ev_bytes.to_vec()))
            .await
            .map_err(|_| Error::Ws)?;
    }
    Ok(())
}

pub async fn compare_and_request_missing_records(
    db: &Db,
    tree_name: impl AsRef<str>,
    records: &ArchivedHashMap<ArchivedGenericKey, ArchivedRecordIteration>,
    ws_tx: &mut (impl Sink<Message> + Unpin),
) -> Result<(), Error> {
    let tree = db.open_tree(tree_name.as_ref())?;
    let mut missing_or_outdated = Vec::new();
    for (key, remote_record) in records.iter() {
        let key = GenericKey::from_archived(key);
        let key_bytes = key.to_bytes();
        match tree.get(key_bytes)? {
            Some(record) => {
                let record = check_archived_root::<Record>(&record)?;
                if record.data_iteration < remote_record.data_iteration
                    || record.meta_iteration < remote_record.meta_iteration
                {
                    missing_or_outdated.push(key);
                }
            }
            None => {
                missing_or_outdated.push(key);
            }
        }
    }
    trace!(
        "{} missing or outdated: {missing_or_outdated:?}",
        tree_name.as_ref()
    );
    let ev = Event::RequestRecords {
        tree: tree_name.as_ref().to_string(),
        keys: missing_or_outdated,
    };
    let ev_bytes = to_bytes::<_, 128>(&ev)?;
    ws_tx
        .send(Message::Binary(ev_bytes.to_vec()))
        .await
        .map_err(|_| Error::Ws)?;
    Ok(())
}

#[macro_export]
macro_rules! handle_result {
    ($r:ident) => {{
        match $r {
            Err(Error::Sled(_f)) => {
                log::error!("Encountered sled error in event loop, terminating");
                return;
            }
            Err(Error::Ws) => {
                log::warn!("Encountered ws stream error in event loop, terminating");
                return;
            }
            Err(Error::Internal(i)) => {
                log::warn!("Encountered internal error in event loop: {i}, terminating");
                return;
            }
            Err(Error::RkyvSerializeError(_)) => {
                log::warn!("rkyv ser error");
            }
            Err(Error::RkyvDeserializeError(_)) => {
                log::warn!("rkyv deser error");
            }
            Err(Error::PostageBroadcast) => {
                log::error!("postage broadcast failed");
            }
            Ok(_) => {}
        }
    }};
}
pub use handle_result;

pub fn handle_incoming_record(
    db: &mut Db,
    ev: &ArchivedHotSyncEvent,
    remote_name: &str,
    indexers: Option<&mut HashMap<String, Vec<Box<dyn TreeIndex + Send>>>>,
) -> Result<(), Error> {
    let tree_name = ev.tree_name.as_str();
    let key = GenericKey::from_archived(&ev.key);
    let key_bytes = key.to_bytes();
    let db_tree = db.open_tree(tree_name)?;
    match &ev.kind {
        ArchivedHotSyncEventKind::Created {
            meta,
            data,
            meta_iteration,
            data_iteration,
            data_evolution,
        } => {
            if db_tree.contains_key(key_bytes)? {
                trace!(
                    "{} record {}/{} ignored, because it exists already",
                    remote_name,
                    tree_name,
                    key
                );
            } else {
                let data_evolution = data_evolution.deserialize(&mut rkyv::Infallible).expect("");

                let mut new_data = AlignedVec::new();
                new_data.extend_from_slice(data.as_slice());
                if let Some(indexers) = indexers {
                    if let Some(indexers) = indexers.get_mut(tree_name) {
                        for indexer in indexers {
                            if let Err(e) = indexer.update(
                                TypeErasedTree {
                                    tree: &db_tree,
                                    evolution: data_evolution,
                                },
                                key,
                                &new_data,
                                Action::Insert,
                            ) {
                                error!("indexer failed on hot sync on creation, {tree_name}:{key} {e:?}");
                            }
                        }
                    }
                }
                let meta: RecordMeta = meta.deserialize(&mut rkyv::Infallible).expect("");
                let record = Record {
                    meta_iteration: *meta_iteration,
                    meta,
                    data_iteration: *data_iteration,
                    data: new_data,
                    data_evolution,
                };
                let record_bytes = to_bytes::<_, 128>(&record)?;
                db_tree.insert(key_bytes, record_bytes.as_slice())?;
                trace!("{} created record {}/{}", remote_name, tree_name, key);
            }
        }
        ArchivedHotSyncEventKind::MetaChanged {
            meta,
            meta_iteration,
        }
        | ArchivedHotSyncEventKind::Changed {
            meta,
            meta_iteration,
            ..
        } => {
            let Some(record) = db_tree.get(key_bytes)? else {
                error!(
                    "{} tried to modify non-existing record: {}/{}",
                    remote_name, tree_name, key
                );
                return Ok(());
            };
            let old_record = check_archived_root::<Record>(&record)?;
            let meta: RecordMeta = meta.deserialize(&mut rkyv::Infallible).expect("");
            let data_evolution = old_record
                .data_evolution
                .deserialize(&mut rkyv::Infallible)
                .expect("");

            match &ev.kind {
                ArchivedHotSyncEventKind::MetaChanged { .. } => {
                    if *meta_iteration <= old_record.meta_iteration {
                        trace!(
                            "{remote_name} update meta {tree_name}/{key} ignored, because it's iteration is {meta_iteration} and this db have {}",
                            old_record.meta_iteration,
                        );
                        return Ok(());
                    }
                    let mut old_data = AlignedVec::new();
                    old_data.extend_from_slice(old_record.data.as_slice());
                    let record = Record {
                        meta_iteration: *meta_iteration,
                        meta,
                        data_iteration: old_record.data_iteration,
                        data: old_data,
                        data_evolution,
                    };
                    let record_bytes = to_bytes::<_, 128>(&record)?;
                    db_tree.insert(key_bytes, record_bytes.as_slice())?;
                    trace!(
                        "{} updated meta {}/{} m.it{}->{}",
                        remote_name,
                        tree_name,
                        key,
                        old_record.meta_iteration,
                        meta_iteration
                    );
                }
                ArchivedHotSyncEventKind::Changed {
                    data,
                    data_iteration,
                    data_evolution,
                    ..
                } => {
                    if *meta_iteration <= old_record.meta_iteration
                        || *data_iteration <= old_record.data_iteration
                    {
                        trace!(
                            "{remote_name} update record {tree_name}/{key} ignored, incoming (mit, dit) is ({meta_iteration}, {data_iteration}) this db ({}, {})",
                            old_record.meta_iteration,
                            old_record.data_iteration,
                        );
                        return Ok(());
                    }
                    let data_evolution =
                        data_evolution.deserialize(&mut rkyv::Infallible).expect("");

                    let mut new_data = AlignedVec::new();
                    new_data.extend_from_slice(data.as_slice());
                    if let Some(indexers) = indexers {
                        if let Some(indexers) = indexers.get_mut(tree_name) {
                            for indexer in indexers {
                                if let Err(e) = indexer.update(
                                    TypeErasedTree {
                                        tree: &db_tree,
                                        evolution: data_evolution,
                                    },
                                    key,
                                    &new_data,
                                    Action::Update,
                                ) {
                                    error!(
                                        "indexer failed on hot sync on change, {tree_name}:{key} {e:?}"
                                    );
                                }
                            }
                        }
                    }

                    let record = Record {
                        meta_iteration: *meta_iteration,
                        meta,
                        data_iteration: *data_iteration,
                        data: new_data,
                        data_evolution,
                    };
                    let record_bytes = to_bytes::<_, 128>(&record)?;
                    db_tree.insert(key_bytes, record_bytes.as_slice())?;
                    trace!(
                        "{} updated record {}/{} d.it{}->{}",
                        remote_name,
                        tree_name,
                        key,
                        old_record.data_iteration,
                        data_iteration
                    );
                }
                _ => unreachable!(),
            }
        }
        ArchivedHotSyncEventKind::Removed => match db_tree.get(key_bytes)? {
            Some(bytes) => {
                let archived_record = check_archived_root::<Record>(&bytes)?;
                let data_evolution = archived_record
                    .data_evolution
                    .deserialize(&mut rkyv::Infallible)
                    .expect("");

                if let Some(indexers) = indexers {
                    if let Some(indexers) = indexers.get_mut(tree_name) {
                        for indexer in indexers {
                            if let Err(e) = indexer.update(
                                TypeErasedTree {
                                    tree: &db_tree,
                                    evolution: data_evolution,
                                },
                                key,
                                &archived_record.data,
                                Action::Remove,
                            ) {
                                error!("indexer failed on hot sync on removal, {tree_name}:{key} {e:?}");
                            }
                        }
                    }
                }

                db_tree.remove(key_bytes)?;
            }
            None => {
                warn!(
                    "{} tried to remove non-existing record: {}/{}",
                    remote_name, tree_name, key
                );
            }
        },
    }
    Ok(())
}

pub async fn send_records(
    db: &Db,
    tree_name: impl AsRef<str>,
    keys: &ArchivedVec<ArchivedGenericKey>,
    ws_tx: &mut (impl Sink<Message> + Unpin),
) -> Result<(), Error> {
    let tree_name = tree_name.as_ref();
    let tree = db.open_tree(tree_name)?;
    for key in keys.iter() {
        let key = GenericKey::from_archived(key);
        let key_bytes = key.to_bytes();
        let Some(record_bytes) = tree.get(key_bytes)? else {
            warn!("send_records: {key} do not actually exist");
            continue;
        };
        let mut record = AlignedVec::new();
        record.extend_from_slice(&record_bytes);
        let ev = Event::RecordAsRequested {
            tree_name: tree_name.to_string(),
            key,
            record,
        };
        let ev_bytes = to_bytes::<_, 128>(&ev)?;
        ws_tx
            .send(Message::Binary(ev_bytes.to_vec()))
            .await
            .map_err(|_| Error::Ws)?;
    }
    Ok(())
}

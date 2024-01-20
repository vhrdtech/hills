use crate::sync::{ChangeKind, Event, RecordHotChange};
use futures_util::{Sink, SinkExt};
use log::error;
use rkyv::ser::serializers::{
    AllocScratchError, CompositeSerializerError, SharedSerializeMapError,
};
use rkyv::validation::validators::DefaultValidatorError;
use rkyv::validation::CheckArchiveError;
use rkyv::{archived_root, to_bytes, AlignedVec, Archive, Deserialize, Serialize};
use sled::Db;
use std::convert::Infallible;
use std::fmt::Debug;
use thiserror::Error;
use tokio_tungstenite::tungstenite::Message;

pub const SELF_UUID: &[u8] = b"_self_uuid";
pub const MANAGED_TREES: &[u8] = b"_managed_trees";

#[derive(Error, Debug)]
pub enum Error {
    #[error(transparent)]
    Sled(#[from] sled::Error),

    #[error("{}", .0)]
    Internal(String),

    #[error("Ws")]
    Ws,

    #[error("rkyv serialize: {}", .0)]
    RkyvSerializeError(String),

    #[error("rkyv check_archived_root failed: {}", .0)]
    RkyvDeserializeError(String),
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
    let id_event = to_bytes::<_, 8>(&id_event).unwrap();
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
) -> Result<(), Error> {
    let tree = db.open_tree(change.tree.as_str())?;
    let ev = match change.kind {
        ChangeKind::Create | ChangeKind::ModifyMeta | ChangeKind::ModifyBoth => {
            let Some(record_bytes) = tree.get(&change.key.to_bytes())? else {
                error!(
                    "send_hot_change for {}/{}: record do not actually exist",
                    change.tree, change.key
                );
                return Ok(());
            };
            let record = unsafe { archived_root::<Record>(&record_bytes) };
            let meta: RecordMeta = record.meta.deserialize(&mut rkyv::Infallible).expect("");
            match change.kind {
                ChangeKind::Create | ChangeKind::ModifyBoth => {
                    let mut data = AlignedVec::new();
                    data.extend_from_slice(record.data.as_slice());
                    match change.kind {
                        ChangeKind::Create => Event::RecordCreated {
                            tree: change.tree,
                            key: change.key,
                            meta,
                            data,
                        },
                        ChangeKind::ModifyBoth => Event::RecordChanged {
                            tree: change.tree,
                            key: change.key,
                            meta,
                            meta_iteration: record.meta_iteration,
                            data,
                            data_iteration: record.data_iteration,
                        },
                        _ => unreachable!(),
                    }
                }
                ChangeKind::ModifyMeta => Event::RecordMetaChanged {
                    tree: change.tree,
                    key: change.key,
                    meta,
                    meta_iteration: record.meta_iteration,
                },
                _ => unreachable!(),
            }
        }
        ChangeKind::Remove => Event::RecordRemoved {
            tree: change.tree,
            key: change.key,
        },
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
            Ok(_) => {}
        }
    }};
}
use crate::record::{Record, RecordMeta};
pub use handle_result;
use std::collections::HashSet;

#[derive(Archive, Serialize, Deserialize)]
#[archive(check_bytes)]
pub struct ManagedTrees {
    pub trees: HashSet<String>,
    pub _dummy22: [u8; 18],
}

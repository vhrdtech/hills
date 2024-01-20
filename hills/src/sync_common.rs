use crate::sync::{ChangeKind, Event, RecordHotChange};
use futures_util::{Sink, SinkExt};
use rkyv::to_bytes;
use sled::Db;
use thiserror::Error;
use tokio_tungstenite::tungstenite::Message;

pub const SELF_UUID: &[u8] = b"self_uuid";

#[derive(Error, Debug)]
pub enum Error {
    #[error(transparent)]
    Sled(#[from] sled::Error),

    #[error("{}", .0)]
    Internal(String),

    #[error("Ws")]
    Ws,
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
    tx: &mut (impl Sink<Message> + Unpin),
) -> Result<(), Error> {
    let tree = db.open_tree(change.tree.as_str())?;
    let (meta, data) = match &change.kind {
        ChangeKind::Remove => (None, None),
        _ => {
            let record = tree.get(&change.key.to_bytes())?;
            match &change.kind {
                ChangeKind::Create | ChangeKind::ModifyBoth => {
                    // let
                    (None, None)
                }
                ChangeKind::ModifyMeta => (None, None),
                _ => unreachable!(),
            }
        }
    };
    let ev = Event::RecordChanged { change, meta, data };
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
            Ok(_) => {}
        }
    }};
}
pub use handle_result;

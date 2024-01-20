use crate::handle_result;
use crate::sync::{ArchivedEvent, Event};
use crate::sync_common::{present_self, Error, SELF_UUID};
use futures_util::{Sink, Stream, StreamExt, TryStreamExt};
use log::{info, trace, warn};
use rkyv::archived_root;
use sled::Db;
use std::path::Path;
use tokio::net::TcpListener;
use tokio::runtime::Runtime;
use tokio::task::JoinHandle;
use tokio_tungstenite::tungstenite::Message;
use uuid::Uuid;

pub struct HillsServer {
    pub join: JoinHandle<()>,
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
                tokio::spawn(async move { ws_event_loop(ws_sink, ws_source, db_clone).await });
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
                        let r = process_message(message, &mut db).await;
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

async fn process_message(ws_message: Message, _db: &mut Db) -> Result<(), Error> {
    match ws_message {
        Message::Binary(bytes) => {
            let client_event = unsafe { archived_root::<Event>(&bytes) };
            match client_event {
                ArchivedEvent::PresentSelf { uuid } => {
                    trace!("Client presenting uuid: {uuid:x?}");
                }
                ArchivedEvent::Subscribe { .. } => {}
                ArchivedEvent::GetTreeOverview { .. } => {}
                ArchivedEvent::TreeOverview { .. } => {}
                ArchivedEvent::RecordChanged { change, meta, data } => {
                    trace!("hot change: {change:?}");
                }
                ArchivedEvent::GetKeySet { .. } => {}
                ArchivedEvent::KeySet { .. } => {}
                ArchivedEvent::CheckOut { .. } => {}
                ArchivedEvent::Return { .. } => {}
                ArchivedEvent::CheckedOut { .. } => {}
                ArchivedEvent::AlreadyCheckedOut { .. } => {}
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

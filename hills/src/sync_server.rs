use crate::sync::ClientEvent;
use futures_util::{Sink, Stream, StreamExt, TryStreamExt};
use log::{info, trace, warn};
use rkyv::check_archived_root;
use sled::Db;
use std::path::Path;
use thiserror::Error;
use tokio::net::TcpListener;
use tokio::runtime::Runtime;
use tokio::task::JoinHandle;
use tokio_tungstenite::tungstenite::Message;

pub struct HillsServer {
    pub join: JoinHandle<()>,
}

#[derive(Debug, Error)]
pub enum Error {
    #[error(transparent)]
    Sled(#[from] sled::Error),
}

impl HillsServer {
    pub fn start<P: AsRef<Path>>(path: P, rt: &Runtime) -> Result<Self, Error> {
        #[cfg(not(test))]
        let db = sled::open(path)?;
        #[cfg(test)]
        let db = sled::Config::new().temporary(true).path(path).open()?;

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
    mut ws_sink: impl Sink<Message> + Unpin,
    mut ws_source: impl Stream<Item = Result<Message, tokio_tungstenite::tungstenite::Error>> + Unpin,
    mut db: Db,
) {
    info!("Event loop started");
    // tokio::pin!(ws_sink);
    // tokio::pin!(ws_source);
    loop {
        tokio::select! {
            frame = ws_source.try_next() => {
                match frame {
                    Ok(Some(frame)) => {
                        let should_terminate = process_message(frame, &mut db).await;
                        if should_terminate {
                            break;
                        }
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

async fn process_message(ws_message: Message, _db: &mut Db) -> bool {
    match ws_message {
        Message::Binary(bytes) => {
            let client_event = check_archived_root::<ClientEvent>(&bytes).unwrap();
            trace!("{client_event:?}");
        }
        Message::Close(_) => {
            return true;
        }
        u => {
            warn!("Unsupported ws message: {u:?}");
        }
    }

    false
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

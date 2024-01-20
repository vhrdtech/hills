use crate::handle_result;
use crate::sync::{ArchivedEvent, Event, RecordHotChange};
use crate::sync_common::Error;
use crate::sync_common::{present_self, send_hot_change};
use futures_util::{
    stream::{SplitSink, SplitStream},
    StreamExt, TryStreamExt,
};
use log::{info, trace, warn};
use postage::mpsc::{channel, Receiver, Sender};
use postage::prelude::Stream;
use rkyv::archived_root;
use sled::Db;
use std::net::IpAddr;
use std::sync::{Arc, RwLock};
use tokio::runtime::Runtime;
use tokio::task::JoinHandle;
use tokio_tungstenite::tungstenite::Message;

pub struct VhrdDbSyncHandle {
    db: Db,
    event_rx: Receiver<RecordHotChange>,
}

impl VhrdDbSyncHandle {
    pub fn new(db: Db, rx: Receiver<RecordHotChange>) -> Self {
        Self { db, event_rx: rx }
    }

    pub fn start(self, rt: &Runtime) -> (Sender<SyncClientCommand>, VhrdDbTelem, JoinHandle<()>) {
        let (cmd_tx, cmd_rx) = channel(64);
        let telem = SyncClientTelemetry::default();
        let telem = Arc::new(RwLock::new(telem));
        let telem_2 = telem.clone();
        let join_handle =
            rt.spawn(async move { event_loop(self.db, self.event_rx, cmd_rx, telem_2).await });

        (cmd_tx, telem, join_handle)
    }
}

pub enum SyncClientCommand {
    Connect(IpAddr, u16),
    Disconnect,
    // FullReSync,
}

pub type VhrdDbCmdTx = Sender<SyncClientCommand>;

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
    telem: VhrdDbTelem,
) {
    let mut ws_txrx: Option<(SplitSink<_, _>, SplitStream<_>)> = None;
    let mut to_replay = Vec::new();

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
                    }
                }
                event = event_rx.recv() => {
                    trace!("ev: {event:?}");
                    if let Some(event) = event {
                        let r = send_hot_change(&db, event, ws_tx).await;
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
                            let (mut ws_tx, ws_rx) = ws_stream.split();
                            present_self(&db, &mut ws_tx).await;
                            ws_txrx = Some((ws_tx, ws_rx));
                        }
                        SyncClientCommand::Disconnect => {}
                    }
                }
                event = event_rx.recv() => {
                    trace!("ev: {event:?}");
                    if let Some(event) = event {
                        to_replay.push(event);
                        if let Ok(mut telem) = telem.write() {
                            telem.backlog = to_replay.len();
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
        }
    }
}

async fn process_message(ws_message: Message, _db: &mut Db) -> Result<(), Error> {
    match ws_message {
        Message::Binary(bytes) => {
            let ev = unsafe { archived_root::<Event>(&bytes) };
            match ev {
                ArchivedEvent::PresentSelf { uuid } => {
                    trace!("Server uuid is: {uuid:x?}");
                }
                ArchivedEvent::Subscribe { .. } => {}
                ArchivedEvent::GetTreeOverview { .. } => {}
                ArchivedEvent::TreeOverview { .. } => {}
                ArchivedEvent::RecordChanged { .. } => {}
                ArchivedEvent::GetKeySet { .. } => {}
                ArchivedEvent::KeySet { tree, keys } => {
                    trace!("Got more keys for {tree} {keys:?}");
                }
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

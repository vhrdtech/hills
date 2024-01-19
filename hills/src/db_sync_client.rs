use crate::sync::RecordHotChange;
use futures_util::{
    stream::{SplitSink, SplitStream},
    StreamExt,
};
use log::{info, trace, warn};
use postage::mpsc::{channel, Receiver, Sender};
use postage::prelude::Stream;
use sled::Db;
use std::net::IpAddr;
use std::sync::{Arc, RwLock};
use tokio::runtime::Runtime;
use tokio::task::JoinHandle;

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
    db: Db,
    mut event_rx: Receiver<RecordHotChange>,
    mut cmd_rx: Receiver<SyncClientCommand>,
    telem: VhrdDbTelem,
) {
    let mut ws_txrx: Option<(SplitSink<_, _>, SplitStream<_>)> = None;
    let mut to_replay = Vec::new();

    loop {
        match &ws_txrx {
            None => {
                tokio::select! {
                    cmd = cmd_rx.recv() => {
                        match cmd {
                            Some(cmd) => match cmd {
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
                                    let (ws_tx, ws_rx) = ws_stream.split();
                                    ws_txrx = Some((ws_tx, ws_rx));
                                }
                                SyncClientCommand::Disconnect => {}
                            }
                            None => {
                                info!("Sync client: tx end no longer exist, exiting");
                                break;
                            }
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
            Some((ws_tx, ws_rx)) => {}
        }
    }
}
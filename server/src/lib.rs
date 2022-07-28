#[macro_use]
extern crate log;

use std::collections::HashMap;
use std::net::SocketAddr;

use bytes::{Buf, Bytes};
use nohash_hasher::IntSet;
use once_cell::sync::OnceCell;
pub use valley::codec::{Encode, Decode};
use valley::connection::quic::QUIConnBuilder;
use valley::connection::tcp::TcpConnBuilder;
pub use valley::errors::VError as ServerError;
use valley::name_service::StaticNameService;
use valley::server::ValleyServer;
use valley::Message;
pub use valley::{ChannelId};
use pegasus_common::config::ServerId;

use crate::consumer::Consumer;
use crate::naming::NameServiceImpl;
use crate::producer::Producer;

static GLOBAL_SERVER_PROXY: OnceCell<ServerInstance> = OnceCell::new();

enum ServerKind {
    TCPServer(ValleyServer<NameServiceImpl, TcpConnBuilder>),
    QUICServer(ValleyServer<NameServiceImpl, QUIConnBuilder>),
}

pub struct ServerInstance {
    id: ServerId,
    kind: ServerKind,
}

impl ServerInstance {
    pub fn global() -> &'static ServerInstance {
        GLOBAL_SERVER_PROXY
            .get()
            .expect("global server proxy not inited;")
    }

    pub async fn start_with_hosts(
        server_id: ServerId, addr: SocketAddr, hosts: Vec<(ServerId, SocketAddr)>,
    ) -> Result<Self, ServerError> {
        let mut hosts_cp = Vec::with_capacity(hosts.len());
        for (id, addr) in hosts {
            hosts_cp.push((id as valley::ServerId, addr));
        }
        let name_service = NameServiceImpl::StaticConfig(StaticNameService::new(hosts_cp));
        let mut server = valley::server::new_tcp_server(server_id as valley::ServerId, addr, name_service);
        server.start().await?;
        Ok(ServerInstance { id: server_id, kind: ServerKind::TCPServer(server) })
    }

    pub fn set_global(self) {
        GLOBAL_SERVER_PROXY
            .set(self)
            .expect("server is already started;");
    }

    pub fn get_id(&self) -> ServerId {
        self.id
    }

    pub async fn alloc_ipc_channel<T, R>(
        &self, ch_id: ChannelId, servers: &[ServerId], mut consumers: Vec<R>,
    ) -> Result<HashMap<ServerId, Producer<T>>, ServerError>
    where
        T: Encode + Send + 'static,
        R: Consumer + Send + 'static,
    {
        let mut server_ids = Vec::with_capacity(servers.len());
        for id in servers {
            server_ids.push(*id as valley::ServerId)
        }
        let (t, mut r) = match &self.kind {
            ServerKind::TCPServer(s) => {
                s.alloc_symmetry_channel_unbound_send::<T>(ch_id, &server_ids)
                    .await
            }
            ServerKind::QUICServer(s) => {
                s.alloc_symmetry_channel_unbound_send::<T>(ch_id, &server_ids)
                    .await
            }
        }?;

        let peers = consumers.len();
        let _g = tokio::spawn(async move {
            let mut server_source = IntSet::<ServerId>::default();
            while let Some(x) = r.recv().await {
                let len = x.get_payload().len();
                let src = x.get_source() as ServerId;
                debug!("channel[{}]: get message(len={}) from server: {};", ch_id, len, src);
                if len > 1 {
                    let mut payload = x.take_payload();
                    let target = payload.get_u8() as usize;
                    if target >= peers {
                        error!("no receiver {} found for message;", target);
                    } else {
                        if let Err(e) = consumers[target].consume(payload).await {
                            error!("consume message error: {};", e);
                            break;
                        }
                    }
                } else {
                    debug!("channel[{}]: get eof of server {};", ch_id, src);
                    server_source.remove(&src);
                    if server_source.is_empty() {
                        for mut r in consumers {
                            r.close().await;
                        }
                        break;
                    }
                }
            };
            info!("finish receive messages of channel {};", ch_id);
        });

        let sends = t.split();
        let mut producers = HashMap::with_capacity(sends.len());
        for p in t.split() {
            producers.insert(p.get_target_server_id() as u8, Producer::new(p));
        }
        Ok(producers)
    }
}

pub mod consumer;
pub mod producer;

pub mod naming;

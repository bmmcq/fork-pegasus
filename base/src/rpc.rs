use std::net::SocketAddr;
use crate::BaseServer;

pub trait RpcServer : BaseServer {
    fn get_rpc_address(&self) -> Option<SocketAddr>;
}
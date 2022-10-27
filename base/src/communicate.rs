use std::net::SocketAddr;
use crate::BaseServer;

pub trait Communicator : BaseServer {
   fn get_communicate_addr(&self) -> Option<SocketAddr>;
}
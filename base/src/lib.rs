use crate::error::SeverError;

pub mod error;

#[derive(Debug, Clone, Copy, PartialEq, Eq, PartialOrd, Ord)]
pub struct ServerId(pub u16);

pub trait BaseServer {
    fn start(&mut self) -> Result<(), SeverError>;

    fn is_started(&self) -> bool;

    fn get_server_id(&self) -> ServerId;

    fn close(&mut self) -> Result<(), SeverError>;
}

pub mod rpc;
pub mod communicate;
pub mod execute;

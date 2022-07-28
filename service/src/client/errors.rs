use std::error::Error;
use std::fmt::{Display, Formatter};

use tonic::Status;

use crate::ServerId;

#[derive(Debug)]
pub enum JobExecError {
    InvalidConfig(String),
    ConnectError(ConnectError),
    ServerError(tonic::Status),
}

impl Display for JobExecError {
    fn fmt(&self, f: &mut Formatter<'_>) -> std::fmt::Result {
        match self {
            JobExecError::InvalidConfig(msg) => write!(f, "Invalid config: {}", msg),
            JobExecError::ConnectError(e) => write!(f, "Fail to connect {}", e),
            JobExecError::ServerError(status) => write!(f, "ServerError: {}", status),
        }
    }
}

impl Error for JobExecError {}

impl From<Status> for JobExecError {
    fn from(e: Status) -> Self {
        JobExecError::ServerError(e)
    }
}

#[derive(Debug)]
pub enum ConnectError {
    ServerNotFount(ServerId),
    AddrTableNotFount,
    FailConnect(ServerId, tonic::transport::Error),
}

impl Display for ConnectError {
    fn fmt(&self, f: &mut Formatter<'_>) -> std::fmt::Result {
        match self {
            ConnectError::ServerNotFount(server_id) => write!(f, "service {} not found;", server_id),
            ConnectError::AddrTableNotFount => write!(f, "Server Address Table not register;"),
            ConnectError::FailConnect(server_id, source) => {
                write!(f, "fail to connect service {} because {};", server_id, source)
            }
        }
    }
}

impl Error for ConnectError {}

impl From<ConnectError> for JobExecError {
    fn from(e: ConnectError) -> Self {
        JobExecError::ConnectError(e)
    }
}

use std::sync::Weak;

use pegasus_common::tag::Tag;
use pegasus_server::SendError;
use thiserror::Error;

#[derive(Error, Debug)]
pub enum PushError {
    #[error("can't  push to a closed channel;")]
    AlreadyClosed,
    #[error("push to a disconnected channel;")]
    Disconnected,
    #[error("fail to encode data;")]
    EncodeError,
    #[error("push would block;")]
    WouldBlock(Option<(Tag, Weak<()>)>),
    #[error("data of {0} has been aborted;")]
    Aborted(Tag),
    #[error("io errors, caused by {source}")]
    SystemIO {
        #[from]
        source: std::io::Error,
    },
    #[error("push to remote server errors: {source}")]
    IPCSendErr {
        #[from]
        source: SendError,
    },
    #[error("unknown push errors: {source:?}")]
    Unknown {
        #[from]
        source: anyhow::Error,
    },
}

impl PushError {
    pub fn is_would_block(&self) -> bool {
        matches!(self, PushError::WouldBlock(_))
    }

    pub fn is_abort(&self) -> bool {
        matches!(self, PushError::Aborted(_))
    }
}

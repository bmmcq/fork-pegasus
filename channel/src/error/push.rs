use std::sync::Weak;
use thiserror::Error;
use pegasus_common::tag::Tag;
use pegasus_server::SendError;

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
    #[error("io error, caused by {source}")]
    SystemIO {
        #[from]
        source: std::io::Error
    },
    #[error("push to remote server error: {source}")]
    IPCSendErr {
        #[from]
        source: SendError
    },
    #[error("unknown push error: {source:?}")]
    Unknown {
        #[from]
        source: anyhow::Error
    }
}
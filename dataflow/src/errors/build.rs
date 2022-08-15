use pegasus_channel::error::IOError;
use thiserror::Error;

#[derive(Error, Debug)]
pub enum JobBuildError {
    #[error("alloc channel fail : {source};")]
    ChannelAllocError {
        #[from]
        source: IOError,
    },
    #[error("data type cast fail: {0};")]
    TypeCastError(String),
    #[error("channel not allocated: {0};")]
    ChannelNotAlloc(String),
}

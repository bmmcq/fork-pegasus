use pegasus_channel::block::BlockGuard;
use pegasus_channel::error::{IOError, IOErrorKind, PullError, PushError};
use thiserror::Error;

#[derive(Error, Debug)]
pub enum InnerError {
    #[error("{source}")]
    IO {
        #[from]
        source: IOError,
    },
}

impl InnerError {
    pub fn check_data_block(&mut self) -> Option<BlockGuard> {
        match self {
            InnerError::IO { source } => match source.cause() {
                IOErrorKind::PushErr { source } => match source {
                    PushError::WouldBlock(b) => b.take().map(BlockGuard::from),
                    _ => None,
                },
                _ => None,
            },
            //_ => None
        }
    }

    // pub fn check_data_abort(&mut self) -> Option<Tag> {
    //     match self {
    //         InnerError::IO { source } => match source.cause() {
    //             IOErrorKind::PushErr { source } => match source {
    //                 PushError::Aborted(tag) => Some(tag.clone()),
    //                 _ => None,
    //             },
    //             _ => None,
    //         },
    //         //_ => None
    //     }
    // }
}

#[derive(Error, Debug)]
pub enum JobExecError {
    #[error("JobExec inner errors: {source}")]
    Inner {
        #[from]
        source: InnerError,
    },
    #[error("JobExec user errors: {source}")]
    UserError {
        #[from]
        source: anyhow::Error,
    },
}

impl From<IOError> for JobExecError {
    fn from(source: IOError) -> Self {
        let source = InnerError::IO { source };
        JobExecError::Inner { source }
    }
}

impl From<IOErrorKind> for JobExecError {
    fn from(source: IOErrorKind) -> Self {
        JobExecError::from(IOError::from(source))
    }
}

impl From<PushError> for JobExecError {
    fn from(source: PushError) -> Self {
        JobExecError::from(IOError::new(source))
    }
}

impl From<PullError> for JobExecError {
    fn from(source: PullError) -> Self {
        JobExecError::from(IOError::new(source))
    }
}

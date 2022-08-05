use pegasus_channel::error::IOError;
use thiserror::Error;

#[derive(Error, Debug)]
pub enum InnerError {
    #[error("{source}")]
    IO {
        #[from]
        source: IOError,
    },
}

#[derive(Error, Debug)]
pub enum JobExecError {
    #[error("JobExec inner error: {source}")]
    Inner {
        #[from]
        source: InnerError,
    },
    #[error("JobExec user error: {source}")]
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
use thiserror::Error;

#[derive(Error, Debug)]
pub enum PullError {
    #[error("get eof of channel;")]
    Eof,
    #[error("unexpected eof of channel;")]
    UnexpectedEof,
    #[error("decode data fail, because {source};")]
    DecodeError {
        #[source]
        source: std::io::Error,
    },
    #[error("io errors, caused by {source};")]
    SystemIO {
        #[from]
        source: std::io::Error,
    },
    #[error("unknown pull errors: {source};")]
    Unknown {
        #[from]
        source: anyhow::Error,
    },
}

impl PullError {
    pub fn is_eof(&self) -> bool {
        matches!(self, PullError::Eof)
    }
}

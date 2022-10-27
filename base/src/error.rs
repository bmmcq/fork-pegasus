use std::error::Error;
use std::fmt::Display;
use crate::ServerId;

#[derive(Debug)]
pub enum ServerErrorKind {
    ServerNotStart,
    ServerAlreadyStarted,
}

#[derive(Debug)]
pub struct SeverError {
    id: ServerId,
    kind: ServerErrorKind,
}

impl SeverError {
    pub fn not_start(id: ServerId) -> Self {
        Self {
            id,
            kind: ServerErrorKind::ServerNotStart
        }
    }

    pub fn already_started(id: ServerId) -> Self {
        Self {
            id,
            kind: ServerErrorKind::ServerAlreadyStarted
        }
    }
}

impl Display for SeverError {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        match self.kind {
            ServerErrorKind::ServerNotStart => {
                write!(f, "server-{} not start;", self.id.0)
            }
            ServerErrorKind::ServerAlreadyStarted => {
                write!(f, "server-{} already start;", self.id.0)
            }
        }
    }
}

impl Error for SeverError {

}

//
//! Copyright 2020 Alibaba Group Holding Limited.
//!
//! Licensed under the Apache License, Version 2.0 (the "License");
//! you may not use this file except in compliance with the License.
//! You may obtain a copy of the License at
//!
//! http://www.apache.org/licenses/LICENSE-2.0
//!
//! Unless required by applicable law or agreed to in writing, software
//! distributed under the License is distributed on an "AS IS" BASIS,
//! WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
//! See the License for the specific language governing permissions and
//! limitations under the License.

use std::error::Error;
use std::fmt::{Debug, Display};
use std::io;
use std::sync::Weak;

use pegasus_common::channel::RecvError;
use pegasus_common::tag::Tag;

use crate::ChannelId;

pub type IOResult<D> = Result<D, IOError>;

#[derive(Clone, Debug)]
pub enum IOErrorKind {
    Eof,
    UnexpectedEof,
    SendAfterClose,
    SendToDisconnect,
    EncodeError(&'static str),
    DecodeError(&'static str),
    // IO error from system's IO derive, like network(tcp..), files,
    SystemIO(io::ErrorKind),
    // block by flow control;
    WouldBlock(Option<(Tag, Weak<()>)>),
    // try to block but can't;
    CannotBlock,
    DataAborted(Tag),
    Unknown,
}

impl From<io::ErrorKind> for IOErrorKind {
    fn from(kind: io::ErrorKind) -> Self {
        IOErrorKind::SystemIO(kind)
    }
}

#[derive(Debug)]
pub struct IOError {
    ch_id: Option<ChannelId>,
    kind: IOErrorKind,
    cause: Option<Box<dyn Error + Send + 'static>>,
    origin: Option<String>,
}

impl From<IOErrorKind> for IOError {
    fn from(e: IOErrorKind) -> Self {
        IOError::new(e)
    }
}

impl IOError {
    pub fn new<K: Into<IOErrorKind>>(kind: K) -> Self {
        IOError { ch_id: None, kind: kind.into(), cause: None, origin: None }
    }

    pub fn eof() -> Self {
        IOError::new(IOErrorKind::Eof)
    }

    pub fn would_block() -> Self {
        IOError::new(IOErrorKind::WouldBlock(None))
    }

    pub fn cannot_block() -> Self {
        IOError::new(IOErrorKind::CannotBlock)
    }

    pub fn set_ch_id(&mut self, ch_id: ChannelId) {
        self.ch_id = Some(ch_id)
    }

    pub fn set_cause(&mut self, err: Box<dyn Error + Send + 'static>) {
        self.cause = Some(err);
    }

    pub fn set_origin(&mut self, origin: String) {
        self.origin = Some(origin);
    }

    pub fn is_eof(&self) -> bool {
        matches!(self.kind, IOErrorKind::Eof)
    }

    pub fn is_would_block(&self) -> bool {
        matches!(self.kind, IOErrorKind::WouldBlock(_))
    }

    pub fn kind(&self) -> &IOErrorKind {
        &self.kind
    }
}

impl Default for IOError {
    fn default() -> Self {
        IOError::new(IOErrorKind::Unknown)
    }
}

impl From<io::Error> for IOError {
    fn from(e: io::Error) -> Self {
        let mut error = IOError::new(IOErrorKind::SystemIO(e.kind()));
        error.set_cause(Box::new(e));
        error
    }
}

impl From<RecvError> for IOError {
    fn from(e: RecvError) -> Self {
        match e {
            RecvError::Eof => IOError::eof(),
            RecvError::UnexpectedEof => IOError::new(IOErrorKind::UnexpectedEof),
        }
    }
}

impl From<Box<dyn Error + Send + 'static>> for IOError {
    fn from(e: Box<dyn Error + Send + 'static>) -> Self {
        let mut err = IOError::new(IOErrorKind::Unknown);
        err.set_cause(e);
        err
    }
}

impl Display for IOError {
    fn fmt(&self, f: &mut std::fmt::Formatter) -> std::fmt::Result {
        write!(f, "IOError(kind={:?})", self.kind)?;

        if let Some(ch_id) = self.ch_id {
            write!(f, " from channel[{}]", ch_id.index)?;
        }

        if let Some(ref origin) = self.origin {
            write!(f, ", occurred at: {}", origin)?;
        }

        if let Some(ref cause) = self.cause {
            write!(f, ", caused by {}", cause)?;
        }

        write!(f, " ;")
    }
}

impl Error for IOError {}

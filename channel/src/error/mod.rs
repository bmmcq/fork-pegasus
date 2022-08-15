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

use std::fmt::Debug;

use pegasus_server::VError;
use thiserror::Error;

pub use self::pull::PullError;
pub use self::push::PushError;
use crate::ChannelId;

pub type IOResult<D> = Result<D, IOError>;

mod pull;
mod push;

#[derive(Error, Debug)]
pub enum IOErrorKind {
    #[error("push errors {source};")]
    PushErr {
        #[from]
        source: PushError,
    },
    #[error("pull errors {source};")]
    PullErr {
        #[from]
        source: PullError,
    },
    #[error("ipc connect errors {source};")]
    ConnectError {
        #[from]
        source: VError,
    },
    #[error("io errors {source};")]
    SystemIO {
        #[from]
        source: std::io::Error,
    },
    #[error("unknown errors {source:?}")]
    Unknown {
        #[from]
        source: anyhow::Error,
    },
}

#[derive(Error, Debug)]
#[error("io errors : {source} at channel {ch_id:?};")]
pub struct IOError {
    ch_id: Option<ChannelId>,
    #[source]
    source: IOErrorKind,
}

impl From<IOErrorKind> for IOError {
    fn from(e: IOErrorKind) -> Self {
        IOError::new(e)
    }
}

impl From<VError> for IOError {
    fn from(source: VError) -> Self {
        Self { ch_id: None, source: IOErrorKind::ConnectError { source } }
    }
}

impl IOError {
    pub fn new<K: Into<IOErrorKind>>(kind: K) -> Self {
        IOError { ch_id: None, source: kind.into() }
    }

    pub fn set_ch_id(&mut self, ch_id: ChannelId) {
        self.ch_id = Some(ch_id)
    }

    pub fn cause(&mut self) -> &mut IOErrorKind {
        &mut self.source
    }
}

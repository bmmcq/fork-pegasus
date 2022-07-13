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

use pegasus_common::channel::RecvError;
use pegasus_network::{IPCReceiver, IPCRecvError, IPCSender};

use crate::channel_id::ChannelId;
use crate::data_plane::intra_process::IntraProcessPull;
use crate::data_plane::{Pull, Push};
use crate::errors::{IOError, IOErrorKind};
use crate::Data;

pub struct RemotePush<T: Data> {
    pub id: ChannelId,
    push: IPCSender<T>,
}

impl<T: Data> Push<T> for RemotePush<T> {
    fn push(&mut self, msg: T) -> Result<(), IOError> {
        Ok(self.push.send(&msg)?)
    }

    fn flush(&mut self) -> Result<(), IOError> {
        Ok(())
    }

    fn close(&mut self) -> Result<(), IOError> {
        Ok(self.push.close()?)
    }
}

impl<T: Data> RemotePush<T> {
    pub fn new(id: ChannelId, push: IPCSender<T>) -> Self {
        RemotePush { id, push }
    }
}

pub struct CombinationPull<T: Data> {
    pub id: ChannelId,
    local: IntraProcessPull<T>,
    remote: IPCReceiver<T>,
    cached: Option<T>,
    local_end: bool,
    remote_end: bool,
}

impl<T: Data> Pull<T> for CombinationPull<T> {
    fn pull_next(&mut self) -> Result<Option<T>, IOError> {
        if let Some(data) = self.cached.take() {
            return Ok(Some(data));
        }

        if !self.remote_end {
            match self.remote.recv() {
                Ok(Some(data)) => return Ok(Some(data)),
                Err(e) => match e {
                    IPCRecvError::RecvErr(RecvError::Eof) => {
                        self.remote_end = true;
                    }
                    IPCRecvError::RecvErr(RecvError::UnexpectedEof) => {
                        return Err(IOErrorKind::UnexpectedEof)?;
                    }
                    IPCRecvError::DecodeErr(e) => {
                        let type_name = std::any::type_name::<T>();
                        let mut error = IOError::new(IOErrorKind::DecodeError(type_name));
                        error.set_cause(Box::new(e));
                        return Err(error);
                    }
                },
                Ok(None) => (),
            }
        }

        if !self.local_end {
            match self.local.pull_next() {
                Ok(Some(data)) => return Ok(Some(data)),
                Err(err) => {
                    if err.is_eof() {
                        self.local_end = true;
                    } else {
                        return Err(err);
                    }
                }
                Ok(None) => (),
            }
        }

        if self.local_end && self.remote_end {
            Err(IOError::eof())
        } else {
            Ok(None)
        }
    }

    fn has_next(&mut self) -> Result<bool, IOError> {
        if self.cached.is_some() {
            Ok(true)
        } else if self.local.has_next()? {
            Ok(true)
        } else if !self.remote_end {
            match self.remote.recv() {
                Ok(d) => self.cached = d,
                Err(e) => match e {
                    IPCRecvError::RecvErr(RecvError::Eof) => {
                        self.remote_end = true;
                    }
                    IPCRecvError::RecvErr(RecvError::UnexpectedEof) => {
                        return Err(IOErrorKind::UnexpectedEof)?;
                    }
                    IPCRecvError::DecodeErr(e) => {
                        let type_name = std::any::type_name::<T>();
                        let mut error = IOError::new(IOErrorKind::DecodeError(type_name));
                        error.set_cause(Box::new(e));
                        error.set_ch_id(self.id);
                        return Err(error);
                    }
                },
            }
            Ok(self.cached.is_some())
        } else {
            Ok(false)
        }
    }
}

impl<T: Data> CombinationPull<T> {
    pub fn new(id: ChannelId, local: IntraProcessPull<T>, remote: IPCReceiver<T>) -> Self {
        CombinationPull { id, local, remote, local_end: false, remote_end: false, cached: None }
    }
}

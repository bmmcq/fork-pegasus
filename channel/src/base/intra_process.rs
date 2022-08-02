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

use pegasus_common::channel::*;

use crate::data::Data;
use crate::error::{PushError, PullError};
use crate::{ChannelId, Pull, Push};

pub struct IntraProcessPush<T> {
    index: u16,
    ch_id: ChannelId,
    sender: MessageSender<T>,
}

impl<T> IntraProcessPush<T> {
    pub fn new(index: u16, ch_id: ChannelId, sender: MessageSender<T>) -> Self {
        IntraProcessPush { index, ch_id, sender }
    }
}

impl<T: Data> Push<T> for IntraProcessPush<T> {
    fn push(&mut self, msg: T) -> Result<(), PushError> {
        self.sender.send(msg).map_err(|_| {
            error!("IntraProcessPush({})#push: send data failure;", self.index);
            PushError::Disconnected
        })
    }

    #[inline]
    fn close(&mut self) -> Result<(), PushError> {
        self.sender.close();
        Ok(())
    }
}

impl<T> Clone for IntraProcessPush<T> {
    fn clone(&self) -> Self {
        Self { index: self.index, ch_id: self.ch_id, sender: self.sender.clone() }
    }
}

pub struct IntraProcessPull<T> {
    is_closed: bool,
    #[allow(dead_code)]
    ch_id: ChannelId,
    recv: MessageReceiver<T>,
    cached: Option<T>,
}

impl<T> IntraProcessPull<T> {
    pub fn new(ch_id: ChannelId, recv: MessageReceiver<T>) -> Self {
        IntraProcessPull { is_closed: false, ch_id, recv, cached: None }
    }
}

impl<T: Data> Pull<T> for IntraProcessPull<T> {
    fn pull_next(&mut self) -> Result<Option<T>, PullError> {
        if self.is_closed {
            return Err(PullError::Eof);
        }

        if let Some(data) = self.cached.take() {
            return Ok(Some(data));
        }

        match self.recv.try_recv() {
            Ok(data) => Ok(data),
            Err(e) => {
                if e.is_eof() {
                    self.is_closed = true;
                    Err(PullError::Eof)
                } else {
                    Err(PullError::UnexpectedEof)
                }
            }
        }
    }

    fn has_next(&mut self) -> Result<bool, PullError> {
        if self.cached.is_some() {
            Ok(true)
        } else {
            if self.is_closed {
                Ok(false)
            } else {
                match self.recv.try_recv() {
                    Ok(d) => self.cached = d,
                    Err(e) => {
                        if e.is_eof() {
                            self.is_closed = true;
                        } else {
                            return Err(PullError::UnexpectedEof);
                        }
                    }
                }
                Ok(self.cached.is_some())
            }
        }
    }
}

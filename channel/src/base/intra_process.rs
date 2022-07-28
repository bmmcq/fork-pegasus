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
use pegasus_common::bytes::Bytes;
use pegasus_common::channel::*;


use pegasus_server::consumer::Consumer;
use pegasus_server::Decode;
use async_trait::async_trait;

use crate::error::{ErrMsg, IOErrorKind};
use crate::{ChannelId, IOError, Pull, Push};

pub struct IntraProcessPush<T: Send> {
    index: u16,
    pub ch_id: ChannelId,
    sender: MessageSender<T>,
}

impl<T: Send> IntraProcessPush<T> {
    pub fn new(index: u16, ch_id: ChannelId, sender: MessageSender<T>) -> Self {
        IntraProcessPush { index, ch_id, sender }
    }
}

impl<T: Send> Push<T> for IntraProcessPush<T> {
    fn push(&mut self, msg: T) -> Result<(), IOError> {
        self.sender.send(msg).map_err(|_| {
            error!("IntraProcessPush({})#push: send data failure;", self.index);
            IOError::new(IOErrorKind::SendToDisconnect)
        })
    }

    #[inline]
    fn close(&mut self) -> Result<(), IOError> {
        self.sender.close();
        Ok(())
    }
}

#[async_trait]
impl<T: Decode> Consumer for IntraProcessPush<T> {
    async fn consume(&mut self, msg: Bytes) -> Result<(), Box<dyn Error>> {
        match T::read_from(&mut msg.as_ref()) {
            Ok(v) => {
                if let Err(e) = self.push(v) {
                    Err(Box::new(e))
                } else {
                    Ok(())
                }
            }
            Err(e) => {
                Err(Box::new(IOError::new(IOErrorKind::DecodeError(ErrMsg::Own(e.to_string())))))
            }
        }
    }

    async fn close(&mut self) {
       self.sender.close();
    }
}

impl <T> Clone for IntraProcessPush<T> {
    fn clone(&self) -> Self {
        Self {
            index: self.index,
            ch_id: self.ch_id,
            sender: self.sender.clone()
        }
    }
}

pub struct IntraProcessPull<T: Send> {
    is_closed: bool,
    ch_id: ChannelId,
    recv: MessageReceiver<T>,
    cached: Option<T>,
}

impl<T: Send> IntraProcessPull<T> {
    pub fn new(ch_id: ChannelId, recv: MessageReceiver<T>) -> Self {
        IntraProcessPull { is_closed: false, ch_id, recv, cached: None }
    }
}

impl<T: Send> Pull<T> for IntraProcessPull<T> {
    fn pull_next(&mut self) -> Result<Option<T>, IOError> {
        if self.is_closed {
            let mut eof = IOError::eof();
            eof.set_ch_id(self.ch_id);
            return Err(eof);
        }

        if let Some(data) = self.cached.take() {
            return Ok(Some(data));
        }

        match self.recv.try_recv() {
            Ok(data) => Ok(data),
            Err(e) => {
                if e.is_eof() {
                    self.is_closed = true;
                    let mut eof = IOError::eof();
                    eof.set_ch_id(self.ch_id);
                    Err(eof)
                } else {
                    Err(e)?
                }
            }
        }
    }

    fn has_next(&mut self) -> Result<bool, IOError> {
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
                            return Err(e)?;
                        }
                    }
                }
                Ok(self.cached.is_some())
            }
        }
    }
}

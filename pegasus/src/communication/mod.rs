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

use std::any::Any;
use std::cell::RefCell;
use std::collections::{HashMap, LinkedList};

use crate::data_plane::ChannelResource;
use crate::errors::{BuildJobError, IOError};
use crate::{Data, JobConf};

mod abort;
mod block;
mod buffer;
pub mod output;
pub mod input;
pub mod builder;
pub use builder::ChannelBuilder;
use crate::graph::Port;

pub type IOResult<D> = Result<D, IOError>;


#[derive(Copy, Clone, Hash, Eq, PartialEq, Debug)]
pub struct ChannelId {
    /// The sequence number of task the communication_old belongs to;
    pub job_seq: u64,
    /// The index of a communication_old channel in the dataflow execution plan;
    pub index: u16,
}

#[derive(Copy, Clone, Hash, Eq, PartialEq, Debug)]
pub struct ChannelInfo {
    pub ch_id: ChannelId,
    pub scope_level: u8,
    pub source_peers: u16,
    pub target_peers: u16,
    pub batch_size: u16,
    pub batch_capacity: u16,
    pub source_port: Port,
    pub target_port: Port,
}



thread_local! {
    static CHANNEL_RESOURCES : RefCell<HashMap<ChannelId, LinkedList<Box<dyn Any>>>> = RefCell::new(Default::default());
}

pub(crate) fn build_channel<T: Data>(
    ch_id: ChannelId, conf: &JobConf,
) -> Result<ChannelResource<T>, BuildJobError> {
    let worker_id = crate::worker_id::get_current_worker();
    let ch = CHANNEL_RESOURCES.with(|res| {
        let mut map = res.borrow_mut();
        map.get_mut(&ch_id)
            .and_then(|ch| ch.pop_front())
    });

    if let Some(ch) = ch {
        let ch = ch
            .downcast::<ChannelResource<T>>()
            .map_err(|_| {
                BuildJobError::Unsupported(format!(
                    "type {} is unsupported in channel {}",
                    std::any::type_name::<T>(),
                    ch_id.index
                ))
            })?;
        Ok(*ch)
    } else {
        let local_workers = worker_id.local_peers;
        let server_index = worker_id.server_index;
        let mut resources =
            crate::data_plane::build_channels::<T>(ch_id, local_workers, server_index, conf.servers())?;
        if let Some(ch) = resources.pop_front() {
            if !resources.is_empty() {
                let mut upcast = LinkedList::new();
                for item in resources {
                    upcast.push_back(Box::new(item) as Box<dyn Any>);
                }
                CHANNEL_RESOURCES.with(|res| {
                    let mut map = res.borrow_mut();
                    map.insert(ch_id, upcast);
                })
            }
            Ok(ch)
        } else {
            BuildJobError::server_err(format!("channel {} resources is empty;", ch_id.index))
        }
    }
}

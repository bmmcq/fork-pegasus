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

use crate::api::function::{BatchRouteFunction, FnResult, RouteFunction};
use crate::api::scope::{MergedScopeDelta, ScopeDelta};
use crate::channel::buffer::ScopeBuffer;
use crate::data::MicroBatch;
use crate::data_plane::{DataPlanePull, DataPlanePush};
use crate::dataflow::DataflowBuilder;
use crate::graph::Port;
use crate::BuildJobError;
use crate::channel::{ChannelId, ChannelInfo};
use crate::channel::output::builder::SharedOutputBuild;
use crate::channel::output::unify::EnumStreamPush;
use crate::Data;
use crate::Tag::Root;

pub enum ChannelKind<T> {
    Pipeline,
    Exchange(Box<dyn RouteFunction<T>>),
    Aggregate,
    Broadcast,
}

impl<T: Data> ChannelKind<T> {
    pub fn is_pipeline(&self) -> bool {
       matches!(self, Self::Pipeline)
    }
}

pub struct ChannelBuilder<T: Data> {

    source: SharedOutputBuild<T>,

    batch_size: u16,

    batch_capacity: u16,

    max_concurrent_scopes: u16,

    kind: ChannelKind<T>,
}



pub(crate) struct MaterializedChannel<T: Data> {
    push: Producer<T>,
    pull: DataPlanePull<MicroBatch<T>>,
    notify: Option<DataPlanePush<MicroBatch<T>>>,
}

impl<T: Data> MaterializedChannel<T> {
    pub fn take(self) -> (Producer<T>, DataPlanePull<MicroBatch<T>>, Option<DataPlanePush<MicroBatch<T>>>) {
        (self.push, self.pull, self.notify)
    }
}

impl<T: Data> ChannelBuilder<T> {

    pub fn set_batch_size(&mut self, batch_size: u16) -> &mut Self {
        self.batch_size = batch_size;
        self
    }

    pub fn get_batch_size(&self) -> u16 {
        self.batch_size
    }

    pub fn set_batch_capacity(&mut self, capacity: u16) -> &mut Self {
        self.batch_capacity = capacity;
        self
    }

    pub fn get_batch_capacity(&self) -> u16 {
        self.batch_capacity
    }

    pub fn set_channel_kind(&mut self, kind: ChannelKind<T>) -> &mut Self {
        self.kind = kind;
        self
    }

    pub fn add_delta(&mut self, delta: ScopeDelta) -> Option<ScopeDelta> {
        self.source.add_delta(delta)
    }

    pub fn get_scope_level(&self) -> u8 {
        self.source.get_scope_level()
    }

    pub fn is_pipeline(&self) -> bool {
        self.kind.is_pipeline()
    }
}

impl<T: Data> ChannelBuilder<T> {
    fn build_pipeline(self, target_port: Port, ch_id: ChannelId) -> MaterializedChannel<T> {
        let (tx, rx) = crate::data_plane::pipeline::<MicroBatch<T>>(ch_id);
        let scope_level = self.get_scope_level();

        let ch_info = ChannelInfo {
            ch_id,
            scope_level,
            source_peers: 1,
            target_peers: 1,
            batch_size: self.batch_size,
            batch_capacity: self.batch_capacity,
            source_port: self.source.get_port(),
            target_port
        };
        let push = DataPlanePush::IntraThread(tx);
        if scope_level == 0 {
            let tag = self.source.get_delta().evolve(&Root);
            self.source.set_push(EnumStreamPush::pipeline(ch_info, tag, push));
        } else {
            self.source.set_push(EnumStreamPush::multi_scope_pipeline(ch_info, self.max_concurrent_scopes, push));
        };

    }

    fn build_remote(
        &self, scope_level: u32, target: Port, id: ChannelId, dfb: &DataflowBuilder,
    ) -> Result<
        (ChannelInfo, Vec<EventEmitPush<T>>, DataPlanePull<MicroBatch<T>>, DataPlanePush<MicroBatch<T>>),
        BuildJobError,
    > {
        let (mut raw, pull) = crate::channel::build_channel::<MicroBatch<T>>(id, &dfb.config)?.take();
        let worker_index = crate::worker_id::get_current_worker().index as usize;
        let notify = raw.swap_remove(worker_index);
        let ch_info = ChannelInfo::new(id, scope_level, raw.len(), raw.len(), self.source, target);
        let mut pushes = Vec::with_capacity(raw.len());
        let source = dfb.worker_id.index;
        for (idx, p) in raw.into_iter().enumerate() {
            let push = EventEmitPush::new(ch_info, source, idx as u32, p, dfb.event_emitter.clone());
            pushes.push(push);
        }
        Ok((ch_info, pushes, pull, notify))
    }

    pub(crate) fn connect_to(
        mut self, target: Port, dfb: &DataflowBuilder,
    ) -> Result<MaterializedChannel<T>, BuildJobError> {
        let index = dfb.next_channel_index();

        let id = ChannelId { job_seq: dfb.config.job_id as u64, index };

        let batch_size = self.batch_size;
        let scope_level = self.get_scope_level();
        let batch_capacity = self.batch_capacity as usize;

        if index > 1 {
            trace_worker!(
                "channel[{}] : config batch size = {}, runtime batch size = {}, scope_level = {}",
                index,
                self.batch_size,
                batch_size,
                self.get_scope_level()
            );
        }

        if dfb.worker_id.total_peers() == 1 {
            return Ok(self.build_pipeline(target, id));
        }

        let kind = std::mem::replace(&mut self.kind, ChannelKind::Pipeline);
        match kind {
            ChannelKind::Pipeline => Ok(self.build_pipeline(target, id)),
            ChannelKind::Shuffle(r) => {
                let (info, pushes, pull, notify) = self.build_remote(scope_level, target, id, dfb)?;
                let mut buffers = Vec::with_capacity(pushes.len());
                for _ in 0..pushes.len() {
                    let b = ScopeBuffer::new(batch_size, batch_capacity, scope_level);
                    buffers.push(b);
                }
                let push = ExchangeByDataPush::new(info, r, buffers, pushes);
                let ch = push.get_cancel_handle();
                let push = Producer::new(info, self.scope_delta, MicroBatchPush::Exchange(push), ch);
                Ok(MaterializedChannel { push, pull: pull.into(), notify: Some(notify) })
            }
            ChannelKind::BatchShuffle(route) => {
                let (info, pushes, pull, notify) = self.build_remote(scope_level, target, id, dfb)?;
                let push = ExchangeByBatchPush::new(info, route, pushes);
                let cancel = push.get_cancel_handle();
                let push =
                    Producer::new(info, self.scope_delta, MicroBatchPush::ExchangeByBatch(push), cancel);
                Ok(MaterializedChannel { push, pull: pull.into(), notify: Some(notify) })
            }
            ChannelKind::Broadcast => {
                let (info, pushes, pull, notify) = self.build_remote(scope_level, target, id, dfb)?;
                let push = BroadcastBatchPush::new(info, pushes);
                let ch = push.get_cancel_handle();
                let push = Producer::new(info, self.scope_delta, MicroBatchPush::Broadcast(push), ch);
                Ok(MaterializedChannel { push, pull: pull.into(), notify: Some(notify) })
            }
            ChannelKind::Aggregate => {
                let (mut ch_info, pushes, pull, notify) =
                    self.build_remote(scope_level, target, id, dfb)?;
                ch_info.target_peers = 1;
                let push = AggregateBatchPush::new(ch_info, pushes);
                let cancel = push.get_cancel_handle();
                let push =
                    Producer::new(ch_info, self.scope_delta, MicroBatchPush::Aggregate(push), cancel);
                Ok(MaterializedChannel { push, pull: pull.into(), notify: Some(notify) })
            }
        }
    }
}

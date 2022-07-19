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

use std::cell::RefCell;
use std::rc::Rc;

use pegasus_common::downcast::*;

use crate::api::scope::{MergedScopeDelta, ScopeDelta};
use crate::communication::output::handle::OutputHandle;
use crate::communication::output::output::{OutputAbortNotify, Producer};
use crate::communication::output::streaming::EnumStreamPush;
use crate::communication::output::{OutputBuilder, OutputInfo, OutputProxy, RefWrapOutput};
use crate::graph::Port;
use crate::Data;

pub trait OutputBuilder {
    fn build(self: Box<Self>) -> Option<Box<dyn OutputProxy>>;
}

pub struct OutputBuilderImpl<D> {
    info: OutputInfo,
    delta: MergedScopeDelta,
    push: Option<EnumStreamPush<D>>,
}

impl<D> OutputBuilderImpl<D> {
    pub fn new(port: Port, scope_level: u8) -> Self {
        OutputBuilderImpl {
            delta: MergedScopeDelta::new(scope_level),
            info: OutputInfo { port, scope_level, targets: 1 },
            push: None,
        }
    }

    pub fn get_port(&self) -> Port {
        self.info.port
    }

    pub fn get_batch_size(&self) -> u16 {
        self.info.batch_size
    }

    pub fn get_batch_capacity(&self) -> u16 {
        self.info.batch_capacity
    }

    pub fn get_scope_level(&self) -> u8 {
        self.info.scope_level
    }

    pub fn set_batch_size(&mut self, size: u16) {
        self.info.batch_size = size;
    }

    pub fn set_batch_capacity(&mut self, cap: u16) {
        self.info.batch_capacity = cap;
    }

    pub fn set_push(&mut self, push: EnumStreamPush<D>) {
        self.push = Some(push);
    }

    pub fn add_delta(&mut self, delta: ScopeDelta) {
        self.delta.add_delta(delta);
    }
}

impl<D: Data> OutputBuilder for OutputBuilderImpl<D> {
    fn build(self: Box<Self>) -> Option<Box<dyn OutputProxy>> {
        todo!()
    }
}

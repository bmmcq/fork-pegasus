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

use crate::api::scope::{MergedScopeDelta, ScopeDelta};
use crate::channel::output::handle::OutputHandle;
use crate::channel::output::streaming::StreamPush;
use crate::channel::output::{Output, OutputInfo};
use crate::channel::output::output::{MultiScopeOutputProxy, OutputProxy};
use crate::channel::output::unify::EnumStreamPush;
use crate::graph::Port;
use crate::Data;

pub trait OutputBuilder {
    fn build(self: Box<Self>) -> Option<Box<dyn Output>>;
}

pub struct OutputBuilderImpl<D: Data> {
    info: OutputInfo,
    delta: MergedScopeDelta,
    push: Option<EnumStreamPush<D>>,
}

impl<D: Data> OutputBuilderImpl<D>
{
    pub fn new(port: Port, scope_level: u8) -> Self {
        OutputBuilderImpl {
            delta: MergedScopeDelta::new(scope_level),
            info: OutputInfo { port, scope_level },
            push: None,
        }
    }

    pub fn get_port(&self) -> Port {
        self.info.port
    }

    pub fn get_scope_level(&self) -> u8 {
        self.info.scope_level
    }

    pub fn set_push(&mut self, push: EnumStreamPush<D>) {
        self.push = Some(push);
    }

    pub fn add_delta(&mut self, delta: ScopeDelta) -> Option<ScopeDelta> {
        self.delta.add_delta(delta)
    }
}


pub struct SharedOutputBuild<D: Data> {
    inner: Rc<RefCell<OutputBuilderImpl<D>>>
}

impl <D: Data> SharedOutputBuild<D> {
    pub fn get_port(&self) -> Port {
        self.inner.borrow().get_port()
    }

    pub fn get_scope_level(&self) -> u8 {
        self.inner.borrow().get_scope_level()
    }

    pub fn set_push(&self, push: EnumStreamPush<D>) {
        self.inner.borrow_mut().set_push(push)
    }

    pub fn add_delta(&self, delta: ScopeDelta) -> Option<ScopeDelta> {
        self.inner.borrow_mut().add_delta(delta)
    }

    pub fn get_delta(&self) -> MergedScopeDelta {
        self.inner.borrow().delta
    }
}

impl <D: Data> OutputBuilder for SharedOutputBuild<D>  {
    fn build(self: Box<Self>) -> Option<Box<dyn Output>> {
        let bm  = self.inner.borrow_mut();
        let push = bm.push.take()?;
        if bm.info.scope_level == 0 {
            Some(Box::new(OutputProxy::new(bm.info, bm.delta, push)))
        } else {
            Some(Box::new(MultiScopeOutputProxy::new(bm.info, bm.delta, push)))
        }
    }
}

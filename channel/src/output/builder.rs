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

use pegasus_common::tag::Tag;

use crate::data::Data;
use crate::output::delta::ScopeDelta;
use crate::output::proxy::OutputProxy;
use crate::output::unify::EnumStreamBufPush;
use crate::output::{AnyOutput, OutputInfo};
use crate::Port;

pub trait OutputBuilder {
    fn build(self: Box<Self>) -> Box<dyn AnyOutput>;
}

pub struct OutputBuilderImpl<D: Data> {
    worker_index: u16,
    info: OutputInfo,
    tag: Tag,
    delta: Option<ScopeDelta>,
    push: Option<EnumStreamBufPush<D>>,
}

impl <D: Data> OutputBuilderImpl<D> {
    pub fn new(worker_index: u16, port: Port, tag: Tag) -> Self {
        let scope_level = tag.len() as u8;
        OutputBuilderImpl {
            worker_index,
            tag,
            delta: None,
            info: OutputInfo { port, scope_level },
            push: None,
        }
    }

    pub fn get_port(&self) -> Port {
        self.info.port
    }

    pub fn get_inbound_tag(&self) -> &Tag {
        &self.tag
    }

    pub fn get_outbound_scope_level(&self) -> u8 {
        match self.delta {
            None => self.info.scope_level,
            Some(ScopeDelta::None) => self.info.scope_level,
            Some(ScopeDelta::ToSibling) => self.info.scope_level,
            Some(ScopeDelta::ToChild) => self.info.scope_level + 1,
            Some(ScopeDelta::ToParent) => {
                assert!(self.info.scope_level > 0);
                self.info.scope_level - 1
            },
        }
    }

    pub fn set_push(&mut self, push: EnumStreamBufPush<D>) {
        assert!(self.push.is_none(), "conflict set push;");
        self.push = Some(push);
    }

    pub fn set_delta(&mut self, delta: ScopeDelta) {
        assert!(self.delta.is_none(), "conflict set delta;");
        self.delta = Some(delta)
    }

    pub fn shared(self) -> OutputBuildRef<D> {
        OutputBuildRef { inner: Rc::new(RefCell::new(self)) }
    }
}

pub struct MultiScopeOutputBuilder<D: Data> {
    worker_index: u16,
    info : OutputInfo,
    delta: Option<ScopeDelta>,
    push: Option<EnumStreamBufPush<D>>
}

impl <D: Data> MultiScopeOutputBuilder<D> {
    pub fn new(worker_index: u16, inbound_scope_level: u8, port: Port) -> Self {
        Self {
            worker_index,
            info: OutputInfo { port, scope_level: inbound_scope_level },
            delta: None,
            push: None,
        }
    }

    pub fn get_port(&self) -> Port {
        self.info.port
    }

    pub fn get_inbound_scope_level(&self) -> u8 {
        self.info.scope_level
    }

    pub fn get_outbound_scope_level(&self) -> u8 {
        match self.delta {
            None => self.info.scope_level,
            Some(ScopeDelta::None) => self.info.scope_level,
            Some(ScopeDelta::ToSibling) => self.info.scope_level,
            Some(ScopeDelta::ToChild) => self.info.scope_level + 1,
            Some(ScopeDelta::ToParent) => {
                assert!(self.info.scope_level > 0);
                self.info.scope_level - 1
            },
        }
    }

    pub fn set_push(&mut self, push: EnumStreamBufPush<D>) {
        assert!(self.delta.is_none(), "conflict set delta;");
        self.push = Some(push);
    }

    pub fn set_delta(&mut self, delta: ScopeDelta) {
        assert!(self.delta.is_none(), "conflict set delta;");
        self.delta = Some(delta)
    }

}

pub struct OutputBuildRef<D: Data> {
    inner: Rc<RefCell<OutputBuilderImpl<D>>>,
}

impl<D: Data> OutputBuildRef<D> {
    pub fn get_port(&self) -> Port {
        self.inner.borrow().get_port()
    }

    pub fn get_outbound_scope_level(&self) -> u8 {
        self.inner.borrow().get_outbound_scope_level()
    }

    pub fn set_push(&self, push: EnumStreamBufPush<D>) {
        self.inner.borrow_mut().set_push(push)
    }

    pub fn set_delta(&self, delta: ScopeDelta) {
        self.inner.borrow_mut().set_delta(delta)
    }
}

impl<D: Data> Clone for OutputBuildRef<D> {
    fn clone(&self) -> Self {
        Self { inner: self.inner.clone() }
    }
}

impl<D: Data> OutputBuilder for OutputBuildRef<D> {
    fn build(self: Box<Self>) -> Box<dyn AnyOutput> {
        let mut bm = self.inner.borrow_mut();
        let push = bm
            .push
            .take()
            .unwrap_or(EnumStreamBufPush::Null);
        let worker_index = self.inner.borrow().worker_index;
        let delta = bm.delta.unwrap_or(ScopeDelta::None);
        Box::new(OutputProxy::new(worker_index, bm.tag.clone(), bm.info, delta, push))
    }
}

pub struct MultiScopeOutputBuilderRef<D: Data> {
    inner: Rc<RefCell<MultiScopeOutputBuilder<D>>>
}

impl <D: Data> MultiScopeOutputBuilderRef<D> {
    pub fn get_port(&self) -> Port {
        self.inner.borrow().get_port()
    }


}


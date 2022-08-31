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
use crate::output::proxy::{MultiScopeOutputProxy, OutputProxy};
use crate::output::unify::EnumStreamBufPush;
use crate::output::{AnyOutput, OutputInfo};
use crate::Port;

pub trait OutputBuilder {
    fn build(self: Box<Self>) -> Box<dyn AnyOutput>;
}

pub struct StreamOutputBuilder<D: Data> {
    worker_index: u16,
    info: OutputInfo,
    tag: Tag,
    push: Option<EnumStreamBufPush<D>>,
}

impl<D: Data> StreamOutputBuilder<D> {
    pub fn new(worker_index: u16, port: Port, tag: Tag) -> Self {
        let scope_level = tag.len() as u8;
        StreamOutputBuilder { worker_index, tag, info: OutputInfo { port, scope_level }, push: None }
    }

    pub fn get_port(&self) -> Port {
        self.info.port
    }

    pub fn get_tag(&self) -> Tag {
        self.tag.clone()
    }

    pub fn get_scope_level(&self) -> u8 {
        self.info.scope_level
    }

    pub fn set_push(&mut self, push: EnumStreamBufPush<D>) {
        assert!(self.push.is_none(), "conflict set push;");
        self.push = Some(push);
    }

    pub fn shared(self) -> StreamBuilder<D> {
        StreamBuilder { inner: Rc::new(RefCell::new(self)) }
    }
}

pub struct MultiScopeStreamOutputBuilder<D: Data> {
    worker_index: u16,
    info: OutputInfo,
    push: Option<EnumStreamBufPush<D>>,
}

impl<D: Data> MultiScopeStreamOutputBuilder<D> {
    pub fn new(worker_index: u16, scope_level: u8, port: Port) -> Self {
        Self { worker_index, info: OutputInfo { port, scope_level }, push: None }
    }

    pub fn get_port(&self) -> Port {
        self.info.port
    }

    pub fn get_scope_level(&self) -> u8 {
        self.info.scope_level
    }

    pub fn set_push(&mut self, push: EnumStreamBufPush<D>) {
        self.push = Some(push);
    }

    pub fn shared(self) -> MultiScopeStreamBuilder<D> {
        MultiScopeStreamBuilder { inner: Rc::new(RefCell::new(self)) }
    }
}

pub struct StreamBuilder<D: Data> {
    inner: Rc<RefCell<StreamOutputBuilder<D>>>,
}

impl<D: Data> StreamBuilder<D> {
    pub fn get_port(&self) -> Port {
        self.inner.borrow().get_port()
    }

    pub fn get_tag(&self) -> Tag {
        self.inner.borrow().get_tag()
    }

    pub fn get_scope_level(&self) -> u8 {
        self.inner.borrow().get_scope_level()
    }

    pub fn set_push(&self, push: EnumStreamBufPush<D>) {
        self.inner.borrow_mut().set_push(push)
    }
}

impl<D: Data> Clone for StreamBuilder<D> {
    fn clone(&self) -> Self {
        Self { inner: self.inner.clone() }
    }
}

impl<D: Data> OutputBuilder for StreamBuilder<D> {
    fn build(self: Box<Self>) -> Box<dyn AnyOutput> {
        let mut bm = self.inner.borrow_mut();
        let push = bm
            .push
            .take()
            .unwrap_or(EnumStreamBufPush::Null);
        let worker_index = self.inner.borrow().worker_index;
        Box::new(OutputProxy::new(worker_index, bm.tag.clone(), bm.info, push))
    }
}

pub struct MultiScopeStreamBuilder<D: Data> {
    inner: Rc<RefCell<MultiScopeStreamOutputBuilder<D>>>,
}

impl<D: Data> MultiScopeStreamBuilder<D> {
    pub fn new(worker_index: u16, input_scope_level: u8, port: Port) -> Self {
        Self {
            inner: Rc::new(RefCell::new(MultiScopeStreamOutputBuilder::new(
                worker_index,
                input_scope_level,
                port,
            ))),
        }
    }

    pub fn get_port(&self) -> Port {
        self.inner.borrow().get_port()
    }

    pub fn get_scope_level(&self) -> u8 {
        self.inner.borrow().get_scope_level()
    }

    pub fn set_push(&self, push: EnumStreamBufPush<D>) {
        self.inner.borrow_mut().set_push(push)
    }
}

impl<D: Data> Clone for MultiScopeStreamBuilder<D> {
    fn clone(&self) -> Self {
        Self { inner: self.inner.clone() }
    }
}

impl<D: Data> OutputBuilder for MultiScopeStreamBuilder<D> {
    fn build(self: Box<Self>) -> Box<dyn AnyOutput> {
        let mut bm = self.inner.borrow_mut();
        let push = bm
            .push
            .take()
            .unwrap_or(EnumStreamBufPush::Null);
        let worker_index = self.inner.borrow().worker_index;
        Box::new(MultiScopeOutputProxy::new(worker_index, bm.info, push))
    }
}

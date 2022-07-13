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

use crate::api::scope::ScopeDelta;
use crate::communication::output::handle::OutputHandle;
use crate::communication::output::output::{OutputAbortNotify, Producer};
use crate::communication::output::{OutputBuilder, OutputProxy, RefWrapOutput};
use crate::graph::Port;
use crate::Data;

#[derive(Copy, Clone, Debug)]
pub struct OutputMeta {
    pub port: Port,
    /// This is the the scope level of operator with this output port belongs to.
    pub scope_level: u32,
    pub batch_size: usize,
    pub batch_capacity: u32,
}

pub struct OutputBuilderImpl<D: Data> {
    ///
    meta: Rc<RefCell<OutputMeta>>,
    ///
    cursor: usize,
    ///
    shared: Rc<RefCell<Option<Producer<D>>>>,
}

impl<D: Data> OutputBuilderImpl<D> {
    pub fn new(port: Port, scope_level: u32, batch_size: usize, batch_capacity: u32) -> Self {
        let shared = None;
        OutputBuilderImpl {
            meta: Rc::new(RefCell::new(OutputMeta { port, scope_level, batch_size, batch_capacity })),
            cursor: 0,
            shared: Rc::new(RefCell::new(shared)),
        }
    }

    pub fn get_port(&self) -> Port {
        self.meta.borrow().port
    }

    pub fn get_batch_size(&self) -> usize {
        self.meta.borrow().batch_size
    }

    pub fn get_batch_capacity(&self) -> u32 {
        self.meta.borrow().batch_capacity
    }

    pub fn get_scope_level(&self) -> u32 {
        self.meta.borrow().scope_level
    }

    pub fn set_batch_size(&self, size: usize) {
        if self.cursor != 0 {
            warn!("detect reset batch size after stream copy, copy after set batch size would be better;")
        }
        self.meta.borrow_mut().batch_size = size;
    }

    pub fn set_batch_capacity(&self, cap: u32) {
        if self.cursor != 0 {
            warn!("detect reset batch capacity after stream copy, copy after set batch capacity would be better;")
        }
        self.meta.borrow_mut().batch_capacity = cap;
    }

    #[inline]
    pub(crate) fn set_push(&self, push: Producer<D>) {
        self.shared.borrow_mut().replace(push);
    }

    pub fn add_output_delta(&self, delta: ScopeDelta) {
        let mut b = self.shared.borrow_mut();
        if let Some(p) = b.as_mut() {
            p.delta.add_delta(delta);
        }
    }
}

impl<D: Data> Clone for OutputBuilderImpl<D> {
    fn clone(&self) -> Self {
        OutputBuilderImpl { meta: self.meta.clone(), cursor: self.cursor, shared: self.shared.clone() }
    }
}

impl_as_any!(OutputBuilderImpl<D: Data>);

impl<D: Data> OutputBuilder for OutputBuilderImpl<D> {
    fn get_abort_notify(&self) -> Option<OutputAbortNotify> {
        let shared = self.shared.borrow();
        shared.as_ref().map(|p| p.get_abort_notify())
    }

    fn build(self: Box<Self>) -> Option<Box<dyn OutputProxy>> {
        let meta = self.meta.borrow();
        let mut shared = self.shared.borrow_mut();
        let b = shared.take()?;
        let output = OutputHandle::new(*meta, b);
        Some(Box::new(RefWrapOutput::wrap(output)) as Box<dyn OutputProxy>)
    }
}

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

use pegasus_common::downcast::AsAny;

use crate::errors::IOResult;
use crate::graph::Port;
use crate::progress::Eos;
use crate::{Data, Tag};

#[derive(Copy, Clone, Debug)]
pub struct OutputInfo {
    pub port: Port,
    pub scope_level: u8, // 0 ~ 512, This is the the scope level of operator with this output port belongs to.
    pub targets: u16,    // 0 ~ 65536, the number of workers who consume data sending from this output;
}

pub trait OutputProxy: AsAny + Send {
    fn info(&self) -> &OutputInfo;

    fn flush(&self) -> IOResult<()>;

    fn notify_end(&self, end: Eos) -> IOResult<()>;

    fn close(&self) -> IOResult<()>;

    fn is_closed(&self) -> bool;
}

mod builder;
mod handle;

mod batched;
mod streaming;
mod output;

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

use pegasus_common::downcast::AsAny;

use crate::eos::Eos;
use crate::error::PushError;
use crate::{Port};

#[derive(Copy, Clone)]
pub struct OutputInfo {
    pub port: Port,
    pub scope_level: u8, // 0 ~ 512, This is the the scope level of operator with this output port belongs to.
}

pub trait Output: Send {
    fn info(&self) -> OutputInfo;

    fn flush(&self) -> Result<(), PushError>;

    fn notify_eos(&self, end: Eos) -> Result<(), PushError>;

    fn close(&self) -> Result<(), PushError>;

    fn is_closed(&self) -> bool;
}

pub trait AnyOutput: AsAny + Output {}

impl<T> AnyOutput for T where T: AsAny + Output {}

pub enum Rectifier {
    And(u64),
    Mod(u64),
}

impl Rectifier {
    pub fn new(length: usize) -> Self {
        if length & (length - 1) == 0 {
            Rectifier::And(length as u64 - 1)
        } else {
            Rectifier::Mod(length as u64)
        }
    }

    #[inline(always)]
    pub fn get(&self, v: u64) -> usize {
        let r = match self {
            Rectifier::And(b) => v & *b,
            Rectifier::Mod(b) => v % *b,
        };
        r as usize
    }
}

pub mod batched;
pub mod builder;
pub mod delta;
pub mod handle;
pub mod proxy;
pub mod streaming;
pub mod unify;

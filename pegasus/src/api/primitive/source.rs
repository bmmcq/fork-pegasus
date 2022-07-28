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

use pegasus_channel::data::Data;
use crate::dataflow::DataflowBuilder;
use crate::errors::BuildJobError;
use crate::stream::Stream;

pub trait IntoDataflow<D: Data> {
    fn into_dataflow(self, entry: Stream<D>) -> Result<Stream<D>, BuildJobError>;
}

pub struct Source {
    dfb: DataflowBuilder,
}

impl Source {
    pub fn new( dfb: &DataflowBuilder) -> Self {
        Source { dfb: dfb.clone() }
    }

    pub fn input_from<I, D>(self, source: I) -> Result<Stream<D>, BuildJobError>
    where
        D: Data,
        I: IntoIterator<Item = D>,
        I::IntoIter: Send + 'static,
    {
        source.into_dataflow(stream)
    }

    pub fn get_worker_index(&self) -> u32 {
        self.dfb.worker_id.index
    }
}

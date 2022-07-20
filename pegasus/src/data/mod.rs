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

use std::fmt::Debug;

use pegasus_common::buffer::{Buffer, ReadBuffer};
use pegasus_common::codec::{Codec, Decode, Encode};
use pegasus_common::io::{ReadExt, WriteExt};

use crate::data::batching::{RoBatch, WoBatch};
use crate::progress::Eos;
use crate::tag::Tag;

pub trait Data: Send + Debug + Codec + 'static {}

impl<T: Send + Debug + Codec + 'static> Data for T {}

pub struct Item<T> {
    pub tag: Tag,
    data: Option<T>,
    eos: Option<Eos>,
}

impl<T> Item<T> {
    pub fn data(tag: Tag, item: T) -> Self {
        Self { tag, data: Some(item), eos: None }
    }

    pub fn take_data(&mut self) -> Option<T> {
        self.data.take()
    }
}

pub struct Package<T> {
    pub src: u32,
    data: RoBatch<Item<T>>,
}

impl<T> Package<T> {
    pub fn new(src: u32, data: RoBatch<Item<T>>) -> Self {
        Self { src, data }
    }
}

pub struct MicroBatch<T> {
    /// the tag of scope this data set belongs to;
    pub tag: Tag,
    /// the index of worker who created this dataset;
    pub src: u16,
    /// if this is the last batch of a scope;
    end: Option<Eos>,
    /// read only data details;
    data: RoBatch<T>,
}

#[allow(dead_code)]
impl<D> MicroBatch<D> {
    #[inline]
    pub fn empty() -> Self {
        MicroBatch { tag: Tag::Root, src: 0, end: None, data: RoBatch::default() }
    }

    pub fn new(tag: Tag, src: u16, data: RoBatch<D>) -> Self {
        MicroBatch { tag, src, end: None, data }
    }

    pub fn set_end(&mut self, end: Eos) {
        self.end = Some(end);
    }

    pub fn set_tag(&mut self, tag: Tag) {
        if let Some(end) = self.end.as_mut() {
            end.tag = tag.clone();
        }
        self.tag = tag;
    }

    pub fn set_seq(&mut self, seq: u64) {
        self.seq = seq;
    }

    pub fn get_end_mut(&mut self) -> Option<&mut Eos> {
        self.end.as_mut()
    }

    pub fn take_end(&mut self) -> Option<Eos> {
        self.end.take()
    }

    pub fn take_data(&mut self) -> RoBatch<D> {
        std::mem::replace(&mut self.data, RoBatch::default())
    }

    pub fn clear(&mut self) {
        self.take_data();
    }

    pub fn get_seq(&self) -> u64 {
        self.seq
    }

    pub fn is_last(&self) -> bool {
        self.end.is_some()
    }

    pub fn get_end(&self) -> Option<&Eos> {
        self.end.as_ref()
    }

    pub fn is_empty(&self) -> bool {
        self.data.is_empty()
    }

    #[inline]
    pub fn tag(&self) -> &Tag {
        &self.tag
    }
}

impl<D> MicroBatch<D> {
    #[inline]
    pub fn drain(&mut self) -> impl Iterator<Item = D> + '_ {
        &mut self.data
    }
}

impl<D> Debug for MicroBatch<D> {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        write!(f, "batch[{:?} len={}]", self.tag, self.data.len())
    }
}

impl<D> std::ops::Deref for MicroBatch<D> {
    type Target = ReadBuffer<D>;

    fn deref(&self) -> &Self::Target {
        &self.data
    }
}

impl<D> std::ops::DerefMut for MicroBatch<D> {
    fn deref_mut(&mut self) -> &mut Self::Target {
        &mut self.data
    }
}

impl<D: Clone> Clone for MicroBatch<D> {
    fn clone(&self) -> Self {
        MicroBatch { tag: self.tag.clone(), src: self.src, end: self.end.clone(), data: self.data.clone() }
    }
}

impl<D: Data> Encode for MicroBatch<D> {
    fn write_to<W: WriteExt>(&self, writer: &mut W) -> std::io::Result<()> {
        self.tag.write_to(writer)?;
        writer.write_u16(self.src)?;
        let len = self.data.len() as u64;
        writer.write_u64(len)?;
        for data in self.data.iter() {
            data.write_to(writer)?;
        }
        self.end.write_to(writer)?;
        Ok(())
    }
}

impl<D: Data> Decode for MicroBatch<D> {
    fn read_from<R: ReadExt>(reader: &mut R) -> std::io::Result<Self> {
        let tag = Tag::read_from(reader)?;
        let src = reader.read_u16()?;
        let len = reader.read_u64()? as usize;
        let mut buf = WoBatch::new(len);
        for _ in 0..len {
            buf.push(D::read_from(reader)?);
        }
        let data = buf.finalize();
        let end = Option::<Eos>::read_from(reader)?;
        Ok(MicroBatch { tag, src, end, data })
    }
}

pub mod batching;

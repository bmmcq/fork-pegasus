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

use std::fmt::{Debug, Formatter};

use pegasus_common::codec::*;

use crate::graph::Port;
use crate::progress::Eos;
use crate::Tag;

#[derive(Clone)]
pub enum EventKind {
    Eos(Eos),
    Abort(Tag),
}

impl Debug for EventKind {
    fn fmt(&self, f: &mut Formatter<'_>) -> std::fmt::Result {
        match self {
            EventKind::Eos(es) => {
                write!(f, "Eos({:?})", es.tag())
            }
            EventKind::Abort(tag) => {
                write!(f, "Abort({:?})", tag)
            }
        }
    }
}

#[derive(Debug, Clone)]
pub struct Event {
    pub from_worker: u32,
    pub target_port: Port,
    kind: EventKind,
}

impl Event {
    pub fn new(worker: u32, target: Port, kind: EventKind) -> Self {
        Event { from_worker: worker, target_port: target, kind }
    }

    pub fn take_kind(self) -> EventKind {
        self.kind
    }
}

impl Encode for Event {
    fn write_to<W: WriteExt>(&self, writer: &mut W) -> std::io::Result<()> {
        writer.write_u32(self.from_worker)?;
        writer.write_u32(self.target_port.index as u32)?;
        writer.write_u32(self.target_port.port as u32)?;
        match &self.kind {
            EventKind::Eos(end) => {
                writer.write_u8(0)?;
                end.write_to(writer)?;
            }
            EventKind::Abort(tag) => {
                writer.write_u8(1)?;
                tag.write_to(writer)?;
            }
        }
        Ok(())
    }
}

impl Decode for Event {
    fn read_from<R: ReadExt>(reader: &mut R) -> std::io::Result<Event> {
        let from_worker = reader.read_u32()?;
        let index = reader.read_u32()? as usize;
        let port = reader.read_u32()? as usize;
        let target_port = Port::new(index, port);
        let e = reader.read_u8()?;
        match e {
            0 => {
                let end = Eos::read_from(reader)?;
                Ok(Event { from_worker, target_port, kind: EventKind::Eos(end) })
            }
            1 => {
                let tag = Tag::read_from(reader)?;
                Ok(Event { from_worker, target_port, kind: EventKind::Abort(tag) })
            }
            _ => Err(std::io::ErrorKind::InvalidData)?,
        }
    }
}

pub mod emitter;

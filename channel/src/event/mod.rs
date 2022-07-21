use std::fmt::{Debug, Formatter};

use pegasus_common::codec::{Decode, Encode, ReadExt, WriteExt};
use pegasus_common::tag::Tag;

use crate::eos::Eos;
use crate::{Port};

#[derive(Clone)]
pub enum EventKind {
    Eos(Eos),
    Abort(Tag),
}

impl Debug for EventKind {
    fn fmt(&self, f: &mut Formatter<'_>) -> std::fmt::Result {
        match self {
            EventKind::Eos(es) => {
                write!(f, "Eos({:?})", es.tag)
            }
            EventKind::Abort(tag) => {
                write!(f, "Abort({:?})", tag)
            }
        }
    }
}

#[derive(Debug, Clone)]
pub struct Event {
    pub from_worker: u16,
    pub target_port: Port,
    kind: EventKind,
}

impl Event {
    pub fn new(worker: u16, target: Port, kind: EventKind) -> Self {
        Event { from_worker: worker, target_port: target, kind }
    }

    pub fn take_kind(self) -> EventKind {
        self.kind
    }
}

impl Encode for Event {
    fn write_to<W: WriteExt>(&self, writer: &mut W) -> std::io::Result<()> {
        writer.write_u16(self.from_worker)?;
        writer.write_u16(self.target_port.index)?;
        writer.write_u8(self.target_port.port)?;
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
        let from_worker = reader.read_u16()?;
        let index = reader.read_u16()?;
        let port = reader.read_u8()?;
        let target_port = (index, port).into();
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

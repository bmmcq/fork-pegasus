use std::fmt::{Debug, Formatter};

use pegasus_common::codec::Buf;
use pegasus_common::tag::Tag;
use pegasus_server::{BufMut, Decode, Encode};

use crate::eos::Eos;
use crate::Port;

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
    fn write_to<W: BufMut>(&self, _writer: &mut W) {
        todo!()
    }
}

impl Decode for Event {
    fn read_from<R: Buf>(_reader: &mut R) -> std::io::Result<Self> {
        todo!()
    }
}

pub mod emitter;

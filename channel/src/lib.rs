#[macro_use]
extern crate enum_dispatch;
#[macro_use]
extern crate log;

use std::fmt::{Display, Formatter};

use crate::error::IOError;

#[derive(Copy, Clone, Debug, Hash, Eq, PartialEq, Default)]
pub struct Port {
    pub index: u16,
    pub port: u8,
}

impl From<(u16, u8)> for Port {
    fn from(raw: (u16, u8)) -> Self {
        Self { index: raw.0, port: raw.1 }
    }
}

impl Display for Port {
    fn fmt(&self, f: &mut Formatter<'_>) -> std::fmt::Result {
        write!(f, "[{}:{}]", self.index, self.port)
    }
}

pub type ChannelIndex = u16;

#[derive(Copy, Clone, Hash, Eq, PartialEq, Debug)]
pub struct ChannelId {
    /// The sequence number of job this channel belongs to;
    pub job_seq: u64,
    /// The index of a channel in the dataflow execution plan;
    pub index: ChannelIndex,
}

#[derive(Copy, Clone, Hash, Eq, PartialEq)]
pub struct ChannelInfo {
    pub ch_id: ChannelId,
    pub scope_level: u8,
    pub source_peers: u16,
    pub target_peers: u16,
    pub batch_size: u16,
    pub batch_capacity: u16,
    pub source_port: Port,
    pub target_port: Port,
}

#[enum_dispatch]
pub trait Push<T>: Send {
    /// Push message into communication_old channel, returns [`Err(IOError)`] if failed;
    /// Check the error to get more information;
    fn push(&mut self, msg: T) -> Result<(), IOError>;

    /// Since some implementation may buffer messages, override this method
    /// to do flush;
    /// For the no-buffer communication_old implementations, invoke this should have no side-effect;
    fn flush(&mut self) -> Result<(), IOError> {
        Ok(())
    }

    /// Close the current [`Push`], it can't push messages any more;
    fn close(&mut self) -> Result<(), IOError>;
}

/// Abstraction of the receive side of a channel which transforms messages of type [`T`];
#[enum_dispatch]
pub trait Pull<T>: Send {
    /// Pull message out of the underlying channel;
    ///
    /// This function won't block;
    ///
    /// Returns [`Ok(Some(T))`] immediately if any message is available in the channel, otherwise
    /// returns [`Ok(None)`].
    ///
    /// Error([`Err(IOError)`]) occurs if the channel is in exception; Check the returned [`IOError`]
    /// for more details about the error;
    fn pull_next(&mut self) -> Result<Option<T>, IOError>;

    /// Check if there is any message in the channel;
    fn has_next(&mut self) -> Result<bool, IOError>;
}

impl<T, P: ?Sized + Push<T>> Push<T> for Box<P> {
    #[inline]
    fn push(&mut self, msg: T) -> Result<(), IOError> {
        (**self).push(msg)
    }

    #[inline]
    fn flush(&mut self) -> Result<(), IOError> {
        (**self).flush()
    }

    #[inline]
    fn close(&mut self) -> Result<(), IOError> {
        (**self).close()
    }
}

impl<T, P: ?Sized + Pull<T>> Pull<T> for Box<P> {
    #[inline]
    fn pull_next(&mut self) -> Result<Option<T>, IOError> {
        (**self).pull_next()
    }

    fn has_next(&mut self) -> Result<bool, IOError> {
        (**self).has_next()
    }
}

pub mod abort;
pub mod alloc;
pub mod base;
pub mod block;
pub mod buffer;
pub mod data;
pub mod eos;
pub mod error;
pub mod event;
pub mod input;
pub mod output;

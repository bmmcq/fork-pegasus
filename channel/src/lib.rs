#[macro_use]
extern crate enum_dispatch;
#[macro_use]
extern crate log;

use std::fmt::{Display, Formatter};

use crate::error::{IOError, PullError, PushError};

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

impl From<(u64, ChannelIndex)> for ChannelId {
    fn from(v: (u64, ChannelIndex)) -> Self {
        ChannelId {
            job_seq: v.0,
            index: v.1
        }
    }
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
pub trait Push<T> {
    /// Push message into underlying channel, returns [`Err(IOError)`] if failed;
    /// Check the error to get more information;
    fn push(&mut self, msg: T) -> Result<(), PushError>;

    /// Since some implementation may buffer messages, override this method
    /// to do flush;
    /// For the no-buffer communication_old implementations, invoke this should have no side-effect;
    fn flush(&mut self) -> Result<(), PushError> {
        Ok(())
    }

    /// Close the current [`Push`], it can't push messages any more;
    fn close(&mut self) -> Result<(), PushError>;
}

/// Abstraction of the receive side of a channel which transforms messages of type [`T`];
#[enum_dispatch]
pub trait Pull<T> {
    /// Pull message out of the underlying channel;
    ///
    /// This function won't block;
    ///
    /// Returns [`Ok(Some(T))`] immediately if any message is available in the channel, otherwise
    /// returns [`Ok(None)`].
    ///
    /// Error([`Err(IOError)`]) occurs if the channel is in exception; Check the returned [`IOError`]
    /// for more details about the error;
    fn pull_next(&mut self) -> Result<Option<T>, PullError>;

    /// Check if there is any message in the channel;
    fn has_next(&mut self) -> Result<bool, PullError>;
}

impl<T, P: ?Sized + Push<T>> Push<T> for Box<P> {
    #[inline]
    fn push(&mut self, msg: T) -> Result<(), PushError> {
        (**self).push(msg)
    }

    #[inline]
    fn flush(&mut self) -> Result<(), PushError> {
        (**self).flush()
    }

    #[inline]
    fn close(&mut self) -> Result<(), PushError> {
        (**self).close()
    }
}

impl<T, P: ?Sized + Pull<T>> Pull<T> for Box<P> {
    #[inline]
    fn pull_next(&mut self) -> Result<Option<T>, PullError> {
        (**self).pull_next()
    }

    fn has_next(&mut self) -> Result<bool, PullError> {
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

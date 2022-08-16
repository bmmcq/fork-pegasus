use pegasus_common::downcast::AsAny;
use pegasus_common::tag::Tag;

use crate::eos::Eos;
use crate::error::PullError;
use crate::ChannelInfo;

pub trait Input: Send + 'static {
    fn info(&self) -> ChannelInfo;

    fn check_ready(&self) -> Result<bool, PullError>;

    fn abort(&self, tag: &Tag);

    fn notify_eos(&self, src: u16, eos: Eos) -> Result<(), PullError>;

    fn is_exhaust(&self) -> bool;
}

pub trait AnyInput: AsAny + Input {}

impl<T> AnyInput for T where T: AsAny + Input {}

pub mod handle;
pub mod proxy;

use pegasus_common::downcast::AsAny;

use crate::eos::Eos;
use crate::error::PullError;
use crate::Port;

#[derive(Copy, Clone, Debug, Hash, Eq, PartialEq)]
pub struct InputInfo {
    scope_level: u8,
    port: Port,
}

pub trait Input: Send {
    fn info(&self) -> InputInfo;

    fn check_ready(&self) -> Result<bool, PullError>;

    fn notify_eos(&self, eos: Eos) -> Result<(), PullError>;

    fn is_exhaust(&self) -> bool;
}

pub trait AnyInput: AsAny + Input {}

impl<T> AnyInput for T where T: AsAny + Input {}

pub mod handle;
pub mod proxy;

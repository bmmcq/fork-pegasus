use pegasus_common::downcast::AsAny;
use crate::error::IOResult;
use crate::Port;

#[derive(Copy, Clone, Debug, Hash, Eq, PartialEq)]
pub struct InputInfo {
    scope_level: u8, 
    port: Port, 
}

pub trait Input : Send {
    
    fn info(&self) -> InputInfo;
    
    fn check_ready(&self) -> IOResult<bool>;
    
    fn is_exhaust(&self) -> bool;
}

pub trait AnyInput: AsAny + Input { }

impl <T> AnyInput for T where T: AsAny + Input { }

pub mod handle;
pub mod proxy; 
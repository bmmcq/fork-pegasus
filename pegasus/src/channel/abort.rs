use ahash::AHashMap;
use pegasus_common::rc::UnsafeRcPtr;
use crate::Tag;

pub trait AbortHandle: Send + 'static {
    /// stop and abort producing or consuming or processing any data of that scope;
    ///
    fn abort(&mut self, tag: Tag, worker: u16) -> Option<Tag>;
}

use std::any::Any;
use std::cell::RefCell;
use std::collections::LinkedList;
use std::rc::Rc;
use nohash_hasher::IntMap;
use pegasus_channel::{ChannelId, ChannelIndex, ChannelInfo};

pub struct ChannelAllocator {
    channel: Rc<RefCell<IntMap<ChannelIndex, LinkedList<Box<dyn Any>>>>>
}

impl ChannelAllocator {
    pub fn alloc_pipeline(&self, ch_info: ChannelInfo) {
        
    }
}

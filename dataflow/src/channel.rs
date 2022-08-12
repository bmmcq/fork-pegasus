use std::any::Any;
use std::collections::VecDeque;
use std::sync::Arc;
use nohash_hasher::IntMap;
use pegasus_channel::alloc::{Channel, ChannelKind};
use pegasus_channel::base::BasePull;
use pegasus_channel::ChannelInfo;
use pegasus_channel::data::{Data, MiniScopeBatch};
use pegasus_channel::error::IOError;
use pegasus_channel::event::emitter::BaseEventEmitter;
use pegasus_channel::input::{AnyInput, InputInfo};
use pegasus_channel::input::proxy::InputProxy;
use pegasus_channel::output::unify::EnumStreamBufPush;
use pegasus_common::config::{JobConfig, JobServerConfig};
use pegasus_common::tag::Tag;

pub struct ChannelAllocator {
    config: Arc<JobConfig>,
    event_emitters: Vec<BaseEventEmitter>, 
    ch_resources: IntMap<u16, Box<dyn Any>>
}

impl ChannelAllocator {
    pub async fn alloc<T>(&mut self, tag: Tag, ch_info: ChannelInfo) -> Result<(), IOError> where T: Data {
        let config = self.config.server_config();
        let reses: VecDeque<Channel<T>> = pegasus_channel::alloc::alloc_buf_exchange::<T>(tag.clone(), ch_info, config, &self.event_emitters).await?;
        self.ch_resources.insert(ch_info.ch_id.index, Box::new(reses));
        Ok(())
    }
    
    pub fn get<T>(&mut self, tag: Tag, worker_index: u16, ch_info: ChannelInfo, kind: ChannelKind<T>) -> (EnumStreamBufPush<T>, Box<dyn AnyInput>) where T: Data {
       match kind {
           ChannelKind::Pipeline => {
               let (push, pull) = pegasus_channel::alloc::alloc_buf_pipeline::<T>(worker_index, tag.clone(), ch_info);
               let input = Box::new(InputProxy::new(worker_index, tag, ch_info.get_input_info(), pull));
               (push, input)
           }
           ChannelKind::Exchange(router) => {
               if let Some(res) = self.ch_resources.get_mut(&ch_info.ch_id.index) {
                   if let Some(ch_res) = res.downcast_mut::<VecDeque<Channel<T>>>() {
                       let ch = ch_res.pop_front().expect("channel lost after allocated");
                       assert_eq!(ch_info, ch.ch_info);
                       ch.into_exchange(worker_index, router)
                   } else { 
                       panic!("channel({}) type cast fail;", ch_info.ch_id.index)
                   }
               } else { 
                   panic!("channel({}) not allocated;", ch_info.ch_id.index)
               }
           }
           ChannelKind::Aggregate => {}
           ChannelKind::Broadcast => {}
       } 
    }
}

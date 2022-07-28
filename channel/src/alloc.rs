use std::collections::LinkedList;
use pegasus_common::config::JobServerConfig;
use pegasus_common::tag::Tag;
use pegasus_server::ServerInstance;
use crate::base::{BasePull};
use crate::{ChannelId, ChannelInfo};
use crate::data::{Data, MiniScopeBatch};
use crate::event::emitter::{BaseEventCollector, BaseEventEmitter, EventCollector};
use crate::output::streaming::partition::PartitionRoute;
use crate::output::unify::{EnumStreamBufPush};

pub enum ChannelKind<T: Data> {
    Pipeline,
    Exchange(Box<dyn PartitionRoute<Item = T> + Send + 'static>),
    Aggregate,
    Broadcast,
}

impl<T: Data> ChannelKind<T> {
    pub fn is_pipeline(&self) -> bool {
        matches!(self, Self::Pipeline)
    }
}


pub fn alloc_buf_pipeline<T: Data>(worker_index: u16, tag: Tag, ch_info: ChannelInfo) -> (EnumStreamBufPush<T>, BasePull<MiniScopeBatch<T>>) {
    let (push, pull) = crate::base::alloc_pipeline::<MiniScopeBatch<T>>(ch_info.ch_id);
    let push = EnumStreamBufPush::pipeline(worker_index, ch_info, tag, push);
    (push, pull)
}

pub fn alloc_multi_scope_buf_pipeline<T: Data>(worker_index: u16, ch_info: ChannelInfo) -> (EnumStreamBufPush<T>, BasePull<MiniScopeBatch<T>>) {
    let (push, pull) = crate::base::alloc_pipeline::<MiniScopeBatch<T>>(ch_info.ch_id);
    let push = EnumStreamBufPush::multi_scope_pipeline(worker_index, ch_info, push);
    (push, pull)
}

pub async fn alloc_event_channel(ch_id: ChannelId, config: JobServerConfig) -> LinkedList<(BaseEventEmitter, BaseEventCollector)> {
    let this_server_id = ServerInstance::global().get_id();
    let mut chs = LinkedList::new();
    if let Some(range) = config.get_peers_on_server(this_server_id) {
        let list = if config.include_servers() == 1 {
            let peers = range.len() as u16;
            crate::base::alloc_local_exchange(peers, ch_id)
        } else {
            crate::base::alloc_cluster_exchange(ch_id, config).await.expect("")
        };
        for (push, pull) in list {
            let push = BaseEventEmitter::new(push);
            let pull = BaseEventCollector::new(pull);
            chs.push_back((push, pull));
        }
    }
    chs
}
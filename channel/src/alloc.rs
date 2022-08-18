use std::collections::{VecDeque};
use std::sync::Arc;

use pegasus_common::config::JobServerConfig;
use pegasus_common::tag::Tag;
use pegasus_server::{Encode, ServerInstance};

use crate::base::{BasePull, SimpleDecoder};
use crate::buffer::decoder::{BatchDecoder, MultiScopeBatchDecoder};
use crate::buffer::pool::{BufferPool, SharedScopedBufferPool};
use crate::data::{Data, MiniScopeBatch};
use crate::event::emitter::{BaseEventCollector, BaseEventEmitter};
use crate::input::proxy::{InputProxy, MultiScopeInputProxy};
use crate::input::AnyInput;
use crate::output::batched::evented::EventEosBatchPush;
use crate::output::streaming::batching::{BufStreamPush, MultiScopeBufStreamPush};
use crate::output::streaming::partition::{PartitionRoute, PartitionStreamPush};
use crate::output::unify::{BaseBatchPull, BuEeBaseBatchPush, EnumStreamBufPush, MsBuEeBaseBatchPush};
use crate::{ChannelId, ChannelInfo, ChannelType, IOError};

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

    pub fn get_type(&self) -> ChannelType {
        match self {
            ChannelKind::Pipeline => ChannelType::SPSC,
            ChannelKind::Exchange(_) => ChannelType::MPMC,
            ChannelKind::Aggregate => ChannelType::MPSC,
            ChannelKind::Broadcast => ChannelType::MPMC,
        }
    }
}

pub struct Channel<T: Data> {
    pub ch_info: ChannelInfo,
    pub tag: Tag,
    pushes: Vec<BuEeBaseBatchPush<T>>,
    pull: BaseBatchPull<T>,
}

impl<T: Data> Channel<T> {
    pub fn into_exchange(
        self, worker_index: u16, router: Box<dyn PartitionRoute<Item = T> + Send + 'static>,
    ) -> (EnumStreamBufPush<T>, Box<dyn AnyInput>) {
        let push = EnumStreamBufPush::Exchange(PartitionStreamPush::new(
            self.ch_info,
            worker_index,
            router,
            self.pushes,
        ));
        let pull = Box::new(InputProxy::new(worker_index, self.tag, self.ch_info, self.pull));
        (push, pull)
    }
}

pub struct MultiScopeChannel<T: Data> {
    pub ch_info: ChannelInfo,
    pushes: Vec<MsBuEeBaseBatchPush<T>>,
    pull: BaseBatchPull<T>,
}

impl<T: Data> MultiScopeChannel<T> {
    pub fn into_exchange(
        self, worker_index: u16, router: Box<dyn PartitionRoute<Item = T> + Send + 'static>,
    ) -> (EnumStreamBufPush<T>, Box<dyn AnyInput>) {
        let push = EnumStreamBufPush::MultiScopeExchange(PartitionStreamPush::new(
            self.ch_info,
            worker_index,
            router,
            self.pushes,
        ));
        let pull = Box::new(MultiScopeInputProxy::new(worker_index, self.ch_info, self.pull));
        (push, pull)
    }
}

pub struct BinaryChannel<T: Data> {
    pub ch_info: ChannelInfo,
    pub tag: Tag,
    left_pushes: Vec<BuEeBaseBatchPush<T>>,
    right_pushes: Vec<BuEeBaseBatchPush<T>>,
    pull: BaseBatchPull<T>,
}

pub fn alloc_buf_pipeline<T>(
    worker_index: u16, tag: Tag, ch_info: ChannelInfo,
) -> (EnumStreamBufPush<T>, BasePull<MiniScopeBatch<T>>)
where
    T: Data + Encode,
{
    let (push, pull) = crate::base::alloc_pipeline::<MiniScopeBatch<T>>(ch_info.ch_id);
    let push = EnumStreamBufPush::pipeline(worker_index, ch_info, tag, push);
    (push, pull)
}

pub fn alloc_binary_buf_pipeline<T>(
    worker_index: u16, tag: Tag, ch_info: ChannelInfo,
) -> (EnumStreamBufPush<T>, BasePull<MiniScopeBatch<T>>)
    where
        T: Data + Encode,
{
    let (push, pull) = crate::base::alloc_pipeline::<MiniScopeBatch<T>>(ch_info.ch_id);
    let push = EnumStreamBufPush::pipeline(worker_index, ch_info, tag, push);
    todo!()
}


pub fn alloc_multi_scope_buf_pipeline<T>(
    worker_index: u16, ch_info: ChannelInfo,
) -> (EnumStreamBufPush<T>, BasePull<MiniScopeBatch<T>>)
where
    T: Data,
{
    let (push, pull) = crate::base::alloc_pipeline::<MiniScopeBatch<T>>(ch_info.ch_id);
    let push = EnumStreamBufPush::multi_scope_pipeline(worker_index, ch_info, push);
    (push, pull)
}

pub async fn alloc_buf_exchange<T>(
    tag: Tag, ch_info: ChannelInfo, config: &JobServerConfig, event_emitters: &Vec<BaseEventEmitter>,
) -> Result<VecDeque<Channel<T>>, IOError>
where
    T: Data,
{
    let this_server_id = ServerInstance::global().get_id();
    let mut chs = VecDeque::new();
    if let Some(range) = config.get_peers_on_server(this_server_id) {
        let local_peers = range.len() as u16;
        chs = VecDeque::with_capacity(local_peers as usize);
        assert_eq!(local_peers as usize, event_emitters.len());
        let total_peers = config.total_peers();

        let mut recv_buffers = VecDeque::with_capacity(local_peers as usize);
        for _ in 0..local_peers {
            recv_buffers.push_back(BufferPool::new(ch_info.batch_size, ch_info.batch_capacity));
        }

        let mut list = if config.include_servers() == 1 {
            let list = crate::base::alloc_local_exchange::<MiniScopeBatch<T>>(local_peers, ch_info.ch_id);
            assert_eq!(list.len(), local_peers as usize);
            list
        } else {
            let mut decoders = Vec::with_capacity(local_peers as usize);
            for recv_buf in recv_buffers.clone() {
                decoders.push(BatchDecoder::new(recv_buf));
            }

            let list = crate::base::alloc_cluster_exchange(ch_info.ch_id, config, decoders).await?;
            let mut to_local_recv_bufs =
                std::mem::replace(&mut recv_buffers, VecDeque::with_capacity(local_peers as usize));

            // for each target at remote, alloc recv_buffer.
            for i in 0..total_peers {
                if range.contains(&i) {
                    recv_buffers.push_back(to_local_recv_bufs.pop_front().expect(""));
                } else {
                    recv_buffers.push_back(BufferPool::new(ch_info.batch_size, ch_info.batch_capacity));
                }
            }
            list
        };

        let (last_pushes, last_pull) = list.pop().expect("unreachable ...");
        let mut worker_index = range.start;
        let mut i = 0;
        for (pushes, pull) in list {
            let mut recv_buffers = recv_buffers.clone();
            let mut buf_pushes = Vec::with_capacity(pushes.len());
            for (target, p) in pushes.into_iter().enumerate() {
                let rbuf = recv_buffers
                    .pop_front()
                    .expect("recv buffer unexpected eof;");
                let p = EventEosBatchPush::new(
                    worker_index,
                    target as u16,
                    ch_info.target_port,
                    event_emitters[i].clone(),
                    p,
                );
                let push = BufStreamPush::with_pool(ch_info, worker_index, tag.clone(), rbuf, p);
                buf_pushes.push(push);
            }

            chs.push_back(Channel { ch_info, tag: tag.clone(), pushes: buf_pushes, pull });
            worker_index += 1;
            i += 1;
        }

        let mut buf_pushes = Vec::with_capacity(last_pushes.len());
        for (target, p) in last_pushes.into_iter().enumerate() {
            let rbuf = recv_buffers
                .pop_front()
                .expect("recv buffer unexpected eof;");
            let p = EventEosBatchPush::new(
                worker_index,
                target as u16,
                ch_info.target_port,
                event_emitters[i].clone(),
                p,
            );
            let push = BufStreamPush::with_pool(ch_info, worker_index, tag.clone(), rbuf, p);
            buf_pushes.push(push);
        }
        chs.push_back(Channel { ch_info, tag: tag.clone(), pushes: buf_pushes, pull: last_pull });
    }
    Ok(chs)
}

pub async fn alloc_multi_scope_buf_exchange<T>(
    ch_info: ChannelInfo, config: &JobServerConfig, event_emitters: &Vec<BaseEventEmitter>,
) -> Result<VecDeque<MultiScopeChannel<T>>, IOError>
where
    T: Data,
{
    let this_server_id = ServerInstance::global().get_id();
    let mut chs = VecDeque::new();
    if let Some(range) = config.get_peers_on_server(this_server_id) {
        chs = VecDeque::with_capacity(range.len());
        let local_peers = range.len() as u16;
        assert_eq!(local_peers as usize, event_emitters.len());
        let total_peers = config.total_peers();
        let mut recv_buffers = VecDeque::with_capacity(local_peers as usize);
        for _ in 0..local_peers {
            recv_buffers.push_back(Arc::new(SharedScopedBufferPool::new(
                ch_info.batch_size,
                ch_info.batch_capacity,
                ch_info.max_scope_slots,
            )));
        }
        let mut list = if config.include_servers() == 1 {
            let list = crate::base::alloc_local_exchange::<MiniScopeBatch<T>>(local_peers, ch_info.ch_id);
            assert_eq!(list.len(), local_peers as usize);
            list
        } else {
            let mut decoders = Vec::with_capacity(local_peers as usize);
            for recv_buf in recv_buffers.clone() {
                decoders.push(MultiScopeBatchDecoder::new(recv_buf));
            }
            let list = crate::base::alloc_cluster_exchange(ch_info.ch_id, config, decoders).await?;
            let mut to_local_recv_bufs =
                std::mem::replace(&mut recv_buffers, VecDeque::with_capacity(local_peers as usize));

            // for each target at remote, alloc recv_buffer.
            for i in 0..total_peers {
                if range.contains(&i) {
                    recv_buffers.push_back(to_local_recv_bufs.pop_front().expect(""));
                } else {
                    recv_buffers.push_back(Arc::new(SharedScopedBufferPool::new(
                        ch_info.batch_size,
                        ch_info.batch_capacity,
                        ch_info.max_scope_slots,
                    )));
                }
            }
            list
        };

        let (last_pushes, last_pull) = list.pop().expect("unreachable ...");
        let mut worker_index = range.start;
        let mut i = 0;
        for (pushes, pull) in list {
            let mut recv_buffers = recv_buffers.clone();
            let mut buf_pushes = Vec::with_capacity(pushes.len());
            for (target, p) in pushes.into_iter().enumerate() {
                let rbuf = recv_buffers
                    .pop_front()
                    .expect("recv buffer unexpected eof;");
                let p = EventEosBatchPush::new(
                    worker_index,
                    target as u16,
                    ch_info.target_port,
                    event_emitters[i].clone(),
                    p,
                );
                let push = MultiScopeBufStreamPush::with_pool(ch_info, worker_index, rbuf, p);
                buf_pushes.push(push);
            }
            worker_index += 1;
            i += 1;
            chs.push_back(MultiScopeChannel { ch_info, pushes: buf_pushes, pull });
        }
        let mut buf_pushes = Vec::with_capacity(last_pushes.len());
        for (target, p) in last_pushes.into_iter().enumerate() {
            let rbuf = recv_buffers
                .pop_front()
                .expect("recv buffer unexpected eof;");
            let p = EventEosBatchPush::new(
                worker_index,
                target as u16,
                ch_info.target_port,
                event_emitters[i].clone(),
                p,
            );
            let push = MultiScopeBufStreamPush::with_pool(ch_info, worker_index, rbuf, p);
            buf_pushes.push(push);
        }
        chs.push_back(MultiScopeChannel { ch_info, pushes: buf_pushes, pull: last_pull });
    }
    Ok(chs)
}

pub async fn alloc_event_channel(
    ch_id: ChannelId, config: &JobServerConfig,
) -> Result<Vec<(BaseEventEmitter, BaseEventCollector)>, IOError> {
    let this_server_id = ServerInstance::global().get_id();
    let mut chs = Vec::new();
    if let Some(range) = config.get_peers_on_server(this_server_id) {
        let peers = range.len() as u16;
        chs = Vec::with_capacity(peers as usize);
        let list = if config.include_servers() == 1 {
            crate::base::alloc_local_exchange(peers, ch_id)
        } else {
            let mut decoders = Vec::with_capacity(peers as usize);
            for _ in 0..peers {
                decoders.push(SimpleDecoder::new())
            }
            crate::base::alloc_cluster_exchange(ch_id, config, decoders).await?
        };
        for (push, pull) in list {
            let push = BaseEventEmitter::new(push);
            let pull = BaseEventCollector::new(pull);
            chs.push((push, pull));
        }
    }
    Ok(chs)
}

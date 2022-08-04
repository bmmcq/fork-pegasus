use std::collections::{LinkedList, VecDeque};
use std::sync::Arc;

use pegasus_common::config::JobServerConfig;
use pegasus_common::tag::Tag;
use pegasus_server::{Encode, ServerInstance};

use crate::base::{BasePull, SimpleDecoder};
use crate::buffer::decoder::{BatchDecoder, MultiScopeBatchDecoder};
use crate::buffer::pool::{BufferPool, SharedScopedBufferPool};
use crate::data::{Data, MiniScopeBatch};
use crate::event::emitter::{BaseEventCollector, BaseEventEmitter};
use crate::output::batched::evented::EventEosBatchPush;
use crate::output::streaming::batching::{BufStreamPush, MultiScopeBufStreamPush};
use crate::output::streaming::partition::PartitionRoute;
use crate::output::unify::{BaseBatchPull, BuEeBaseBatchPush, EnumStreamBufPush, MsBuEeBaseBatchPush};
use crate::{ChannelId, ChannelInfo, IOError};

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

pub fn alloc_multi_scope_buf_pipeline<T>(
    worker_index: u16, max_scope_slots: u16, ch_info: ChannelInfo,
) -> (EnumStreamBufPush<T>, BasePull<MiniScopeBatch<T>>)
where
    T: Data,
{
    let (push, pull) = crate::base::alloc_pipeline::<MiniScopeBatch<T>>(ch_info.ch_id);
    let push = EnumStreamBufPush::multi_scope_pipeline(worker_index, max_scope_slots, ch_info, push);
    (push, pull)
}

pub async fn alloc_buf_exchange<T>(
    worker_index: u16, tag: Tag, ch_info: ChannelInfo, config: JobServerConfig,
    event_emitter: BaseEventEmitter,
) -> Result<LinkedList<(Vec<BuEeBaseBatchPush<T>>, BaseBatchPull<T>)>, IOError>
where
    T: Data,
{
    let this_server_id = ServerInstance::global().get_id();
    let mut chs = LinkedList::new();
    if let Some(range) = config.get_peers_on_server(this_server_id) {
        let local_peers = range.len() as u16;
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
                    event_emitter.clone(),
                    p,
                );
                let push = BufStreamPush::with_pool(ch_info, worker_index, tag.clone(), rbuf, p);
                buf_pushes.push(push);
            }
            chs.push_back((buf_pushes, pull));
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
                event_emitter.clone(),
                p,
            );
            let push = BufStreamPush::with_pool(ch_info, worker_index, tag.clone(), rbuf, p);
            buf_pushes.push(push);
        }
        chs.push_back((buf_pushes, last_pull));
    }
    Ok(chs)
}

pub async fn alloc_multi_scope_buf_exchange<T>(
    worker_index: u16, max_scope_slots: u16, ch_info: ChannelInfo, config: JobServerConfig,
    event_emitter: BaseEventEmitter,
) -> Result<LinkedList<(Vec<MsBuEeBaseBatchPush<T>>, BaseBatchPull<T>)>, IOError>
where
    T: Data,
{
    let this_server_id = ServerInstance::global().get_id();
    let mut chs = LinkedList::new();
    if let Some(range) = config.get_peers_on_server(this_server_id) {
        let local_peers = range.len() as u16;
        let total_peers = config.total_peers();
        let mut recv_buffers = VecDeque::with_capacity(local_peers as usize);
        for _ in 0..local_peers {
            recv_buffers.push_back(Arc::new(SharedScopedBufferPool::new(
                ch_info.batch_size,
                ch_info.batch_capacity,
                max_scope_slots,
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
                        max_scope_slots,
                    )));
                }
            }
            list
        };

        let (last_pushes, last_pull) = list.pop().expect("unreachable ...");

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
                    event_emitter.clone(),
                    p,
                );
                let push = MultiScopeBufStreamPush::with_pool(ch_info, worker_index, rbuf, p);
                buf_pushes.push(push);
            }
            chs.push_back((buf_pushes, pull));
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
                event_emitter.clone(),
                p,
            );
            let push = MultiScopeBufStreamPush::with_pool(ch_info, worker_index, rbuf, p);
            buf_pushes.push(push);
        }
        chs.push_back((buf_pushes, last_pull));
    }
    Ok(chs)
}

pub async fn alloc_event_channel(
    ch_id: ChannelId, config: JobServerConfig,
) -> LinkedList<(BaseEventEmitter, BaseEventCollector)> {
    let this_server_id = ServerInstance::global().get_id();
    let mut chs = LinkedList::new();
    if let Some(range) = config.get_peers_on_server(this_server_id) {
        let peers = range.len() as u16;
        let list = if config.include_servers() == 1 {
            crate::base::alloc_local_exchange(peers, ch_id)
        } else {
            let mut decoders = Vec::with_capacity(peers as usize);
            for _ in 0..peers {
                decoders.push(SimpleDecoder::new())
            }
            crate::base::alloc_cluster_exchange(ch_id, config, decoders)
                .await
                .expect("")
        };
        for (push, pull) in list {
            let push = BaseEventEmitter::new(push);
            let pull = BaseEventCollector::new(pull);
            chs.push_back((push, pull));
        }
    }
    chs
}

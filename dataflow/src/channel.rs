use std::any::Any;
use std::collections::VecDeque;
use std::sync::Arc;

use nohash_hasher::IntMap;
use pegasus_channel::alloc::{Channel, ChannelKind, MultiScopeChannel};
use pegasus_channel::data::Data;
use pegasus_channel::error::IOError;
use pegasus_channel::event::emitter::{BaseEventCollector, BaseEventEmitter};
use pegasus_channel::input::proxy::{InputProxy, MultiScopeInputProxy};
use pegasus_channel::input::AnyInput;
use pegasus_channel::output::unify::EnumStreamBufPush;
use pegasus_channel::ChannelInfo;
use pegasus_common::config::JobConfig;
use pegasus_common::tag::Tag;

use crate::errors::JobBuildError;

pub struct ChannelAllocator {
    config: Arc<JobConfig>,
    event_emitters: Vec<BaseEventEmitter>,
    event_collectors: VecDeque<BaseEventCollector>,
    ch_resources: IntMap<u16, Box<dyn Any>>,
}

impl ChannelAllocator {
    pub async fn new(config: Arc<JobConfig>) -> Result<Self, JobBuildError> {
        match pegasus_channel::alloc::alloc_event_channel(
            (config.job_id(), 0).into(),
            config.server_config(),
        )
        .await
        {
            Ok(tmp) => {
                let mut event_emitters = Vec::with_capacity(tmp.len());
                let mut event_collectors = VecDeque::with_capacity(tmp.len());
                for (e, c) in tmp {
                    event_emitters.push(e);
                    event_collectors.push_back(c);
                }

                Ok(Self { config, event_emitters, event_collectors, ch_resources: IntMap::default() })
            }
            Err(e) => {
                error!("job({}): alloc channel for event fail: {}", config.job_id(), e);
                Err(e)?
            }
        }
    }

    pub fn get_event_emitter(&self, worker_index: u16) -> &BaseEventEmitter {
        assert!((worker_index as usize) < self.event_emitters.len());
        &self.event_emitters[worker_index as usize]
    }

    pub fn take_event_collector(&mut self) -> Option<BaseEventCollector> {
        self.event_collectors.pop_front()
    }

    pub async fn alloc<T>(&mut self, tag: Tag, ch_info: ChannelInfo) -> Result<(), IOError>
    where
        T: Data,
    {
        let config = self.config.server_config();
        let reses: VecDeque<Channel<T>> = pegasus_channel::alloc::alloc_buf_exchange::<T>(
            tag.clone(),
            ch_info,
            config,
            &self.event_emitters,
        )
        .await?;
        self.ch_resources
            .insert(ch_info.ch_id.index, Box::new(reses));
        Ok(())
    }

    pub async fn alloc_multi_scope<T>(&mut self, ch_info: ChannelInfo) -> Result<(), IOError> where T: Data {
        let config = self.config.server_config();
        let res: VecDeque<MultiScopeChannel<T>> = pegasus_channel::alloc::alloc_multi_scope_buf_exchange::<T>(ch_info, config, &self.event_emitters).await?;
        self.ch_resources.insert(ch_info.ch_id.index, Box::new(res));
        Ok(())
    }

    pub fn get<T>(
        &mut self, tag: Tag, worker_index: u16, ch_info: ChannelInfo, kind: ChannelKind<T>,
    ) -> Result<(EnumStreamBufPush<T>, Box<dyn AnyInput>), JobBuildError>
    where
        T: Data,
    {
        match kind {
            ChannelKind::Pipeline => {
                let (push, pull) =
                    pegasus_channel::alloc::alloc_buf_pipeline::<T>(worker_index, tag.clone(), ch_info);
                let input = Box::new(InputProxy::new(worker_index, tag, ch_info, pull));
                Ok((push, input))
            }
            ChannelKind::Exchange(router) => {
                if let Some(res) = self.ch_resources.get_mut(&ch_info.ch_id.index) {
                    if let Some(ch_res) = res.downcast_mut::<VecDeque<Channel<T>>>() {
                        let ch = ch_res
                            .pop_front()
                            .expect("channel lost after allocated");
                        assert_eq!(ch_info, ch.ch_info);
                        Ok(ch.into_exchange(worker_index, router))
                    } else {
                        Err(JobBuildError::TypeCastError(format!(
                            "channel({}) type cast to {} fail;",
                            ch_info.ch_id.index,
                            std::any::type_name::<T>()
                        )))
                    }
                } else {
                    Err(JobBuildError::ChannelNotAlloc(format!("channel({})", ch_info.ch_id.index)))
                }
            }
            ChannelKind::Aggregate => {
                todo!()
            }
            ChannelKind::Broadcast => {
                todo!()
            }
        }
    }

    pub fn get_multi_scope<T>(&mut self, worker_index: u16, ch_info: ChannelInfo, kind: ChannelKind<T>) -> Result<(EnumStreamBufPush<T>, Box<dyn AnyInput>), JobBuildError> where T: Data {
        match kind {
            ChannelKind::Pipeline => {
                let (push, pull) = pegasus_channel::alloc::alloc_multi_scope_buf_pipeline(worker_index, ch_info);
                let input = Box::new(MultiScopeInputProxy::new(worker_index, ch_info, pull));
                Ok((push, input))
            }
            ChannelKind::Exchange(router) => {
                if let Some(res) = self.ch_resources.get_mut(&ch_info.ch_id.index) {
                    if let Some(ch_res) = res.downcast_mut::<VecDeque<MultiScopeChannel<T>>>() {
                        let ch = ch_res
                            .pop_front()
                            .expect("channel lost after allocated");
                        assert_eq!(ch_info, ch.ch_info);
                        Ok(ch.into_exchange(worker_index, router))
                    } else {
                        Err(JobBuildError::TypeCastError(format!(
                            "channel({}) type cast to {} fail;",
                            ch_info.ch_id.index,
                            std::any::type_name::<T>()
                        )))
                    }
                } else {
                    Err(JobBuildError::ChannelNotAlloc(format!("channel({})", ch_info.ch_id.index)))
                }

            }
            ChannelKind::Aggregate => {
                todo!()
            }
            ChannelKind::Broadcast => {
                todo!()
            }
        }
    }
}

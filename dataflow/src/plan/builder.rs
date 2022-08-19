use std::cell::RefCell;
use std::rc::Rc;
use std::sync::Arc;

use nohash_hasher::IntMap;
use pegasus_channel::alloc::ChannelKind;
use pegasus_channel::data::Data;
use pegasus_channel::event::emitter::BaseEventEmitter;
use pegasus_channel::input::AnyInput;
use pegasus_channel::output::unify::EnumStreamBufPush;
use pegasus_channel::{ChannelId, ChannelInfo};
use pegasus_common::config::JobConfig;
use pegasus_common::tag::Tag;

use crate::channel::ChannelAllocator;
use crate::context::{ScopeContext, ScopeContextWithOps};
use crate::errors::JobBuildError;
use crate::operators::builder::{Builder, OperatorBuilder};
use crate::operators::OperatorInfo;
use crate::plan::DataFlowPlan;

pub struct DataflowBuilder {
    pub(crate) index: u16,
    pub(crate) worker_index: u16,
    peers: u16,
    pub(crate) config: Arc<JobConfig>,
    scope_ctxes: Rc<RefCell<IntMap<u16, ScopeContextWithOps>>>,
    channel_index: Rc<RefCell<usize>>,
    operators: Rc<RefCell<Vec<OperatorBuilder>>>,
    channel_alloc: Rc<RefCell<ChannelAllocator>>,
}

impl Clone for DataflowBuilder {
    fn clone(&self) -> Self {
        Self {
            index: self.index,
            worker_index: self.worker_index,
            peers: self.peers,
            config: self.config.clone(),
            scope_ctxes: self.scope_ctxes.clone(),
            channel_index: self.channel_index.clone(),
            operators: self.operators.clone(),
            channel_alloc: self.channel_alloc.clone(),
        }
    }
}

impl DataflowBuilder {
    pub fn fork(&mut self) -> Self {
        if self.index == 0 {
            self.peers += 1;
            Self {
                index: self.index + self.peers,
                worker_index: self.worker_index + self.peers,
                peers: self.peers,
                config: self.config.clone(),
                scope_ctxes: self.scope_ctxes.clone(),
                channel_index: Rc::new(RefCell::new(0)),
                operators: Rc::new(RefCell::new(Vec::with_capacity(self.operators.borrow().len()))),
                channel_alloc: self.channel_alloc.clone(),
            }
        } else {
            panic!("can't fork dataflow builder from mirror;");
        }
    }

    pub(crate) fn add_operator<OB>(&self, origin_op_index: usize, info: OperatorInfo, op: OB)
    where
        OB: Builder,
    {
        let mut operators_br = self.operators.borrow_mut();
        assert!(origin_op_index < operators_br.len());
        operators_br[origin_op_index].add_dependency(info.index as usize);
        // only need in primary builder;
        if self.index == 0 {
            let mut ctxes = self.scope_ctxes.borrow_mut();
            ctxes
                .get_mut(&info.scope_ctx.id())
                .expect("scope context not found")
                .add_op(info.index);
        }
        let mut op_builder = OperatorBuilder::new(info, op);
        op_builder.dependent_on(origin_op_index);
        operators_br.push(op_builder);
    }

    pub(crate) fn op_size(&self) -> usize {
        self.operators.borrow().len()
    }

    pub(crate) fn add_scope_cxt(&self, ctx: ScopeContext) {
        self.scope_ctxes
            .borrow_mut()
            .insert(ctx.id(), ScopeContextWithOps::new(ctx));
    }

    pub(crate) fn new_channel_id(&self) -> ChannelId {
        let mut index = self.channel_index.borrow_mut();
        *index += 1;
        assert!(*index < u16::MAX as usize, "too many channels;");
        (self.config.job_id(), *index as u16 - 1).into()
    }

    pub(crate) fn get_event_emitter(&self) -> BaseEventEmitter {
        let ch_br = self.channel_alloc.borrow_mut();
        ch_br
            .get_event_emitter(self.worker_index)
            .clone()
    }

    pub(crate) async fn alloc<T>(
        &self, tag: Tag, ch_info: ChannelInfo, kind: ChannelKind<T>,
    ) -> Result<(EnumStreamBufPush<T>, Box<dyn AnyInput>), JobBuildError>
    where
        T: Data,
    {
        if !kind.is_pipeline() && self.index == 0 {
            self.channel_alloc
                .borrow_mut()
                .alloc::<T>(tag.clone(), ch_info)
                .await?;
        }

        self.channel_alloc
            .borrow_mut()
            .get(tag, self.worker_index, ch_info, kind)
    }

    pub(crate) async fn alloc_multi_scope<T>(
        &self, ch_info: ChannelInfo, kind: ChannelKind<T>,
    ) -> Result<(EnumStreamBufPush<T>, Box<dyn AnyInput>), JobBuildError>
    where
        T: Data,
    {
        if !kind.is_pipeline() && self.index == 0 {
            self.channel_alloc
                .borrow_mut()
                .alloc_multi_scope::<T>(ch_info)
                .await?;
        }

        self.channel_alloc
            .borrow_mut()
            .get_multi_scope(self.worker_index, ch_info, kind)
    }
}

impl DataflowBuilder {
    pub fn build(self) -> DataFlowPlan {
        let mut ops = self.operators.borrow_mut();
        ops.sort_by(|a, b| a.info().index.cmp(&b.info().index));
        let mut operators = Vec::with_capacity(ops.len());
        for o in ops.drain(..) {
            let op = o.build();
            assert_eq!(operators.len(), op.info().index as usize);
            operators.push(op);
        }
        let event_collector = self
            .channel_alloc
            .borrow_mut()
            .take_event_collector()
            .expect("event collector not found;");
        let scope_ctxes = std::mem::replace(&mut *self.scope_ctxes.borrow_mut(), IntMap::default());
        DataFlowPlan::new(self.index, self.config, scope_ctxes, event_collector, operators)
    }
}

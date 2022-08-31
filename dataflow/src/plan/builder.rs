use std::cell::RefCell;
use std::rc::Rc;
use std::sync::Arc;

use pegasus_channel::alloc::ChannelKind;
use pegasus_channel::data::Data;
use pegasus_channel::event::emitter::EventEmitter;
use pegasus_channel::input::AnyInput;
use pegasus_channel::output::unify::EnumStreamBufPush;
use pegasus_channel::{ChannelId, ChannelInfo};
use pegasus_common::config::JobConfig;
use pegasus_common::tag::Tag;

use crate::channel::ChannelAllocator;
use crate::context::{ContextKind, ScopeContext};
use crate::errors::JobBuildError;
use crate::operators::builder::{Builder, OperatorInBuild};
use crate::operators::OperatorInfo;
use crate::plan::DataFlowTask;

pub struct DataflowBuilder {
    pub(crate) index: u16,
    pub(crate) local_peers: u16,
    pub(crate) cluster_peer_index: u16,
    pub(crate) config: Arc<JobConfig>,
    channel_index: Rc<RefCell<usize>>,
    scope_id: Rc<RefCell<u16>>,
    operators: Rc<RefCell<Vec<OperatorInBuild>>>,
    channel_alloc: Rc<RefCell<ChannelAllocator>>,
}

impl Clone for DataflowBuilder {
    fn clone(&self) -> Self {
        Self {
            index: self.index,
            cluster_peer_index: self.cluster_peer_index,
            local_peers: self.local_peers,
            config: self.config.clone(),
            channel_index: self.channel_index.clone(),
            scope_id: self.scope_id.clone(),
            operators: self.operators.clone(),
            channel_alloc: self.channel_alloc.clone(),
        }
    }
}

impl DataflowBuilder {
    pub fn fork(&mut self) -> Self {
        if self.index == 0 {
            self.local_peers += 1;
            Self {
                index: self.index + self.local_peers,
                cluster_peer_index: self.cluster_peer_index + self.local_peers,
                local_peers: self.local_peers,
                config: self.config.clone(),
                channel_index: Rc::new(RefCell::new(0)),
                scope_id: Rc::new(RefCell::new(1)),
                operators: Rc::new(RefCell::new(Vec::with_capacity(self.operators.borrow().len()))),
                channel_alloc: self.channel_alloc.clone(),
            }
        } else {
            panic!("can't fork dataflow builder from mirror;");
        }
    }

    pub(crate) fn add_source<OB>(&self, op: OB) where OB: Builder {
        let mut operators_br = self.operators.borrow_mut();
        assert_eq!(operators_br.len(), 0);
        let scope_ctx = ScopeContext::new(0, 0, ContextKind::Flat);
        let info = OperatorInfo::new("source", 0, scope_ctx);
        let op_builder = OperatorInBuild::new(info, op);
        operators_br.push(op_builder);
    }

    pub(crate) fn add_operator<OB>(&self, pre_op_index: usize, info: OperatorInfo, op: OB)
    where
        OB: Builder,
    {
        let mut operators_br = self.operators.borrow_mut();
        assert!(pre_op_index < operators_br.len());
        let pre = &mut operators_br[pre_op_index];
        if pre.info().scope_ctx.is_parent_of(&info.scope_ctx) {
            pre.add_sub(info.index as usize);
            let mut op_builder = OperatorInBuild::new(info, op);
            op_builder.add_parent(pre_op_index);
            operators_br.push(op_builder);
        } else {
            pre.add_dependency(info.index as usize);
            let mut op_builder = OperatorInBuild::new(info, op);
            op_builder.dependent_on(pre_op_index);
            operators_br.push(op_builder);
        }
    }

    pub(crate) fn feedback_to(&self, op_index: usize, feedback: Box<dyn AnyInput>) {
        let mut op_br = self.operators.borrow_mut();
        op_br[op_index].add_feedback(feedback)
    }

    pub(crate) fn op_size(&self) -> usize {
        self.operators.borrow().len()
    }

    pub(crate) fn new_channel_id(&self) -> ChannelId {
        let mut index = self.channel_index.borrow_mut();
        *index += 1;
        assert!(*index < u16::MAX as usize, "too many channels;");
        (self.config.job_id(), *index as u16 - 1).into()
    }

    pub(crate) fn new_scope_id(&self) -> u16 {
        let mut scope_id = self.scope_id.borrow_mut();
        *scope_id += 1;
        *scope_id - 1
    }

    pub(crate) fn get_event_emitter(&self) -> EventEmitter {
        let ch_br = self.channel_alloc.borrow_mut();
        ch_br
            .get_event_emitter(self.cluster_peer_index)
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
            .get(tag, self.cluster_peer_index, ch_info, kind)
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
            .get_multi_scope(self.cluster_peer_index, ch_info, kind)
    }
}

impl DataflowBuilder {
    pub fn build(self) -> DataFlowTask {
        let mut ops = self.operators.borrow_mut();
        ops.sort_by(|a, b| a.info().index.cmp(&b.info().index));
        let mut operators = Vec::with_capacity(ops.len());
        let event_emitter = self.get_event_emitter();
        for o in ops.drain(..) {
            let op = o.build(event_emitter.clone());
            let offset = op.info().index as usize;
            assert_eq!(operators.len(), offset);

            operators.push(op);

        }
        let event_collector = self
            .channel_alloc
            .borrow_mut()
            .take_event_collector()
            .expect("event collector not found;");
        DataFlowTask::new(self.index, self.config, event_collector, operators)
    }
}

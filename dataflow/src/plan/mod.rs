use std::sync::Arc;

use nohash_hasher::IntMap;
use pegasus_channel::event::emitter::BaseEventCollector;
use pegasus_common::config::JobConfig;

use crate::context::ScopeContextWithOps;
use crate::operators::OperatorFlow;

pub mod builder;
mod streams;

#[allow(dead_code)]
pub struct DataFlowPlan {
    index: u16,
    config: Arc<JobConfig>,
    scope_ctxes: IntMap<u16, ScopeContextWithOps>,
    event_collector: BaseEventCollector,
    operators: Vec<OperatorFlow>,
}

impl DataFlowPlan {
    pub fn new(
        index: u16, config: Arc<JobConfig>, scope_ctxes: IntMap<u16, ScopeContextWithOps>,
        event_collector: BaseEventCollector, operators: Vec<OperatorFlow>,
    ) -> Self {
        Self { index, config, scope_ctxes, event_collector, operators }
    }
}

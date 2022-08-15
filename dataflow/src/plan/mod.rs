use std::sync::Arc;

use pegasus_channel::event::emitter::BaseEventCollector;
use pegasus_common::config::JobConfig;

use crate::operators::OperatorFlow;

pub mod builder;

#[allow(dead_code)]
pub struct DataFlowPlan {
    index: u16,
    config: Arc<JobConfig>,
    event_collector: BaseEventCollector,
    operators: Vec<OperatorFlow>,
}

impl DataFlowPlan {
    pub fn new(index: u16,
        config: Arc<JobConfig>, event_collector: BaseEventCollector, operators: Vec<OperatorFlow>,
    ) -> Self {
        Self { index, config, event_collector, operators }
    }
}

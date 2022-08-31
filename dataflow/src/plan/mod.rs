use std::sync::Arc;

use nohash_hasher::IntMap;
use pegasus_channel::event::emitter::EventCollector;
use pegasus_common::config::JobConfig;
use smallvec::SmallVec;

use crate::context::ScopeContext;
use crate::operators::Operator;

pub mod builder;
mod streams;

pub struct SourceNode {
    op_index: usize,
    next: SmallVec<[usize; 2]>,
}

pub struct OpNode {
    op_index: usize,
    prev: SmallVec<[usize; 2]>,
    next: SmallVec<[usize; 2]>,
    nested: Option<Box<Plan>>,
}

pub struct Plan {
    scope_ctx: ScopeContext,
    source: SourceNode,
    nodes: IntMap<usize, OpNode>,
}

impl SourceNode {
    fn new(op_index: usize) -> Self {
        Self { op_index, next: SmallVec::new() }
    }
}

impl OpNode {
    fn new(op_index: usize) -> Self {
        Self { op_index, prev: SmallVec::new(), next: SmallVec::new(), nested: None }
    }
}

impl Plan {
    fn new(source_index: usize, operators: &[Operator]) -> Self {
        let source = &operators[source_index];
        let mut src_node = SourceNode::new(source_index);
        let scope_ctx = source.info().scope_ctx;
        for next in source.dependencies() {
            src_node.next.push(*next);
        }
        let mut plan = Self { source: src_node, nodes: IntMap::default(), scope_ctx };
        for next in source.dependencies() {
            plan.add_op(*next, operators);
        }
        plan
    }

    fn add_op(&mut self, op_index: usize, operators: &[Operator]) {
        let mut op_node = OpNode::new(op_index);
        let op = &operators[op_index];
        for prev in op.dependents() {
            op_node.prev.push(*prev);
        }

        for next in op.dependencies() {
            op_node.next.push(*next);
        }

        if let Some(sub_index) = op.get_sub() {
            let sub_plan = Plan::new(sub_index, operators);
            op_node.nested = Some(Box::new(sub_plan));
        }

        self.nodes.insert(op_index, op_node);

        for next in op.dependencies() {
            self.add_op(*next, operators);
        }
    }

    fn get_sinks(&self) -> SmallVec<[usize; 2]> {
        let mut sinks = SmallVec::new();
        for (index, node) in self.nodes.iter() {
            if node.next.is_empty() {
                sinks.push(*index);
            }
        }
        sinks
    }
}

#[allow(dead_code)]
pub struct DataFlowTask {
    index: u16,
    config: Arc<JobConfig>,
    event_collector: EventCollector,
    operators: Vec<Operator>,
    plan: Plan,
    sinks: SmallVec<[usize; 2]>,
}

impl DataFlowTask {
    pub(crate) fn new(
        index: u16, config: Arc<JobConfig>, event_collector: EventCollector, operators: Vec<Operator>,
    ) -> Self {
        let plan = Plan::new(0, &operators);
        let sinks = plan.get_sinks();
        Self { index, config, event_collector, operators, plan, sinks }
    }

    pub fn poll(&mut self) {}
}

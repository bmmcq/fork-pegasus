use std::sync::Arc;

use nohash_hasher::IntMap;
use pegasus_channel::event::emitter::EventCollector;
use pegasus_common::config::JobConfig;
use smallvec::SmallVec;

use crate::context::ScopeContext;
use crate::errors::JobExecError;
use crate::operators::{Operator, State};

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

    fn get_leafs(&self) -> SmallVec<[usize; 2]> {
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
    leafs: SmallVec<[usize; 2]>,
}

impl DataFlowTask {
    pub(crate) fn new(
        index: u16, config: Arc<JobConfig>, event_collector: EventCollector, operators: Vec<Operator>,
    ) -> Self {
        let plan = Plan::new(0, &operators);
        let leafs = plan.get_leafs();
        Self { index, config, event_collector, operators, plan, leafs }
    }

    pub fn poll(&mut self) -> Result<(), JobExecError> {
        // 1. 从叶子节点还是开始poll执行：
        //     1.1. 如果所有叶子节点都返回Finished状态，返回 Task Finished；
        //     1.2. 如果有一个叶子节点返回blocking状态，返回 Task Block;
        //     1.3. 如果所有叶子节点返回状态部分Idle和部分Finished, 但不是1.1的情况， 进入2 poll上游依赖算子；
        // 2.
        todo!()
    }
}

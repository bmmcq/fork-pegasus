use crate::operators::{OperatorFlow};

pub mod builder;

#[allow(dead_code)]
pub struct DataFlowPlan {
    operators: Vec<OperatorFlow>
}
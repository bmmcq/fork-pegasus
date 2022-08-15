use pegasus_channel::input::AnyInput;
use pegasus_channel::output::AnyOutput;

use crate::errors::JobExecError;

pub trait BranchFunction: Send + 'static {
    fn on_receive(
        &mut self, input: &Box<dyn AnyInput>, left_out: &Box<dyn AnyOutput>, right_out: &Box<dyn AnyOutput>,
    ) -> Result<(), JobExecError>;
}

#[allow(dead_code)]
pub struct BranchOperator {
    input: [Box<dyn AnyInput>; 1],
    outputs: [Box<dyn AnyOutput>; 2],
    func: Box<dyn BranchFunction>,
}

impl BranchOperator {
    pub fn new(
        input: Box<dyn AnyInput>, left_out: Box<dyn AnyOutput>, right_out: Box<dyn AnyOutput>,
        func: Box<dyn BranchFunction>,
    ) -> Self {
        Self { input: [input], outputs: [left_out, right_out], func }
    }
}

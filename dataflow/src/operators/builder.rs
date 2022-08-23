use pegasus_channel::event::emitter::EventEmitter;
use pegasus_channel::input::AnyInput;
use pegasus_channel::output::builder::OutputBuilder;
use pegasus_common::downcast::AsAny;
use smallvec::SmallVec;

use crate::operators::{Operator, OperatorFlow, OperatorInfo};

pub trait Builder: AsAny {
    fn add_feedback(&mut self, _feedback: Box<dyn AnyInput>) {
        panic!("feedback not supported")
    }

    fn build(self: Box<Self>, event_emitter: EventEmitter) -> Box<dyn Operator>;
}

pub struct OperatorInBuild {
    info: OperatorInfo,
    builder: Box<dyn Builder>,
    dependencies: SmallVec<[usize; 2]>,
    dependents: SmallVec<[usize; 2]>,
}

impl OperatorInBuild {
    pub fn new<B>(info: OperatorInfo, builder: B) -> Self
    where
        B: Builder,
    {
        Self {
            info,
            builder: Box::new(builder),
            dependencies: SmallVec::new(),
            dependents: SmallVec::new(),
        }
    }

    pub fn info(&self) -> &OperatorInfo {
        &self.info
    }

    pub fn add_dependency(&mut self, index: usize) {
        self.dependencies.push(index)
    }

    pub fn dependent_on(&mut self, index: usize) {
        self.dependents.push(index)
    }

    pub fn add_feedback(&mut self, feedback: Box<dyn AnyInput>) {
        self.builder.add_feedback(feedback);
    }

    pub fn build(self, event_emitter: EventEmitter) -> OperatorFlow {
        let op = self.builder.build(event_emitter);
        OperatorFlow::new(self.info, op, self.dependencies, self.dependents)
    }
}

pub struct BuildCommon {
    pub(crate) worker_index: u16,
    pub(crate) inputs: SmallVec<[Box<dyn AnyInput>; 2]>,
    pub(crate) outputs: SmallVec<[Box<dyn OutputBuilder>; 2]>,
}

impl BuildCommon {
    pub fn new_unary<O>(worker_index: u16, input: Box<dyn AnyInput>, output: O) -> Self
    where
        O: OutputBuilder + 'static,
    {
        let mut inputs = SmallVec::new();
        inputs.push(input);
        let mut outputs = SmallVec::new();
        outputs.push(Box::new(output) as Box<dyn OutputBuilder>);

        Self { worker_index, inputs, outputs }
    }
}

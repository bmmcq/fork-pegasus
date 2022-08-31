use pegasus_channel::event::emitter::EventEmitter;
use pegasus_channel::input::AnyInput;
use pegasus_channel::output::builder::OutputBuilder;
use pegasus_common::downcast::AsAny;
use smallvec::SmallVec;

use crate::operators::{OperatorTrait, Operator, OperatorInfo};

pub trait Builder: AsAny {
    fn add_feedback(&mut self, _feedback: Box<dyn AnyInput>) {
        panic!("feedback not supported")
    }

    fn build(self: Box<Self>, event_emitter: EventEmitter) -> Box<dyn OperatorTrait>;
}

pub struct OperatorInBuild {
    info: OperatorInfo,
    builder: Box<dyn Builder>,
    dependencies: SmallVec<[usize; 2]>,
    dependents: SmallVec<[usize; 2]>,
    sub_index: Option<usize>,
    parent_index: Option<usize>,
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
            sub_index: None,
            parent_index: None,
        }
    }

    pub fn info(&self) -> &OperatorInfo {
        &self.info
    }

    pub fn add_dependency(&mut self, index: usize) {
        self.dependencies.push(index)
    }

    pub fn add_sub(&mut self, index: usize) {
        self.sub_index = Some(index);
    }

    pub fn dependent_on(&mut self, index: usize) {
        self.dependents.push(index)
    }

    pub fn add_parent(&mut self, index: usize) {
        self.parent_index = Some(index);
    }

    pub fn add_feedback(&mut self, feedback: Box<dyn AnyInput>) {
        self.builder.add_feedback(feedback);
    }

    pub fn build(self, event_emitter: EventEmitter) -> Operator {
        let op = self.builder.build(event_emitter);
        let mut of = Operator::new(self.info, op, self.dependencies, self.dependents);
        if let Some(index) = self.sub_index {
            of.add_sub(index);
        }

        if let Some(index) = self.parent_index {
            of.add_parent(index)
        }
        of
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

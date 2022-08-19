use pegasus_common::downcast::AsAny;
use smallvec::SmallVec;

use crate::operators::{Operator, OperatorFlow, OperatorInfo};

pub trait Builder: AsAny {
    fn build(self: Box<Self>) -> Box<dyn Operator>;
}

pub struct OperatorBuilder {
    info: OperatorInfo,
    builder: Box<dyn Builder>,
    dependencies: SmallVec<[usize; 2]>,
    dependents: SmallVec<[usize; 2]>,
}

impl OperatorBuilder {
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

    pub fn build(self) -> OperatorFlow {
        let op = self.builder.build();
        OperatorFlow::new(self.info, op, self.dependencies, self.dependents)
    }
}

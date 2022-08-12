use smallvec::SmallVec;
use pegasus_common::downcast::AsAny;
use crate::operators::{Operator, OperatorInfo};

pub trait Builder: AsAny {
    fn build(self: Box<Self>) -> Box<dyn Operator>;
}

pub struct OperatorBuilder {
    info: OperatorInfo,
    builder: Box<dyn Builder>,
    dependencies: SmallVec<[usize;2]>,
    dependents: SmallVec<[usize;2]>
}

impl OperatorBuilder {
    pub fn new<B>(info: OperatorInfo, builder: B) -> Self where B: Builder {
        Self {
            info, 
            builder: Box::new(builder), 
            dependencies: SmallVec::new(), 
            dependents: SmallVec::new(), 
        }
    }
}




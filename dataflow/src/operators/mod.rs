use smallvec::SmallVec;
use pegasus_channel::base::BasePull;
use pegasus_channel::data::MiniScopeBatch;
use pegasus_channel::input::handle::{InputHandle, MultiScopeInputHandle};
use pegasus_channel::input::{AnyInput};
use pegasus_channel::output::handle::{MiniScopeStreamSink, MultiScopeOutputHandle, OutputHandle};
use pegasus_channel::output::unify::EnumStreamBufPush;
use pegasus_channel::output::AnyOutput;

use crate::error::JobExecError;

pub type BatchInput<T> = InputHandle<T, BasePull<MiniScopeBatch<T>>>;
pub type Output<T> = OutputHandle<T, EnumStreamBufPush<T>>;
pub type MultiScopeBatchInput<T> = MultiScopeInputHandle<T, BasePull<MiniScopeBatch<T>>>;
pub type MultiScopeOutput<T> = MultiScopeOutputHandle<T, EnumStreamBufPush<T>>;
pub type StreamSink<'a, T> = MiniScopeStreamSink<'a, T, Output<T>>;
pub type MultiScopeStreamSink<'a, T> = MiniScopeStreamSink<'a, T, MultiScopeOutput<T>>;

#[derive(Clone, Debug)]
pub struct OperatorInfo {
    pub name: String,
    pub index: usize,
    pub scope_level: u8,
}

impl std::fmt::Display for OperatorInfo {
    fn fmt(&self, f: &mut std::fmt::Formatter) -> std::fmt::Result {
        write!(f, "[{}_{}]", self.name, self.index)
    }
}

impl OperatorInfo {
    pub fn new(name: &str, index: usize, scope_level: u8) -> Self {
        OperatorInfo { name: name.to_owned(), index, scope_level }
    }
}

pub trait Operator {
    fn inputs(&self) -> &[Box<dyn AnyInput>];

    fn outputs(&self) -> &[Box<dyn AnyOutput>];

    fn fire(&mut self) -> Result<bool, JobExecError>;

    fn close(&mut self);
}

#[allow(dead_code)]
pub struct OperatorFlow {
    is_active: bool,
    info: OperatorInfo,
    core: Option<Box<dyn Operator + Send + 'static>>,
    dependencies: SmallVec<[usize;2]>,
    dependents: SmallVec<[usize;2]>
}

pub mod builder; 
pub mod source;
pub mod binary;
pub mod branch;
pub mod consume;
pub mod unary;

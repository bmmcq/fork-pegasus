use pegasus_channel::base::BasePull;
use pegasus_channel::data::MiniScopeBatch;
use pegasus_channel::input::handle::{InputHandle, MultiScopeInputHandle};
use pegasus_channel::input::AnyInput;
use pegasus_channel::output::handle::{MiniScopeStreamSink, MultiScopeOutputHandle, OutputHandle};
use pegasus_channel::output::unify::EnumStreamBufPush;
use pegasus_channel::output::AnyOutput;
use smallvec::SmallVec;

use crate::errors::JobExecError;

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
    pub is_multi_scope: bool,
}

impl std::fmt::Display for OperatorInfo {
    fn fmt(&self, f: &mut std::fmt::Formatter) -> std::fmt::Result {
        write!(f, "[{}_{}]", self.name, self.index)
    }
}

impl OperatorInfo {
    pub fn new(name: &str, index: usize, scope_level: u8) -> Self {
        OperatorInfo { name: name.to_owned(), index, scope_level, is_multi_scope: false }
    }
}

pub enum State {
    Idle,
    Blocking(usize),
    Finished,
}

pub trait Operator: Send + 'static {
    fn inputs(&self) -> &[Box<dyn AnyInput>];

    fn outputs(&self) -> &[Box<dyn AnyOutput>];

    fn fire(&mut self) -> Result<State, JobExecError>;

    fn close(&mut self);
}

#[allow(dead_code)]
pub struct OperatorFlow {
    is_finished: bool,
    info: OperatorInfo,
    core: Option<Box<dyn Operator>>,
    dependencies: SmallVec<[usize; 2]>,
    dependents: SmallVec<[usize; 2]>,
}

impl OperatorFlow {
    pub fn new(
        info: OperatorInfo, op: Box<dyn Operator>, dependencies: SmallVec<[usize; 2]>,
        dependents: SmallVec<[usize; 2]>,
    ) -> Self {
        Self { is_finished: true, info, core: Some(op), dependencies, dependents }
    }

    pub fn info(&self) -> &OperatorInfo {
        &self.info
    }

    pub fn dependents(&self) -> &[usize] {
        self.dependents.as_slice()
    }

    pub fn dependencies(&self) -> &[usize] {
        self.dependencies.as_slice()
    }

    pub fn fire(&mut self) -> Result<State, JobExecError> {
        if let Some(mut op) = self.core.take() {
            let state = op.fire()?;
            if matches!(state, State::Finished) {
                self.is_finished = false;
                for output in op.outputs() {
                    output.close()?;
                }
            }
            Ok(state)
        } else {
            Ok(State::Finished)
        }
    }
}

pub mod binary;
pub mod branch;
pub mod builder;
pub mod consume;
pub mod source;
pub mod unary;

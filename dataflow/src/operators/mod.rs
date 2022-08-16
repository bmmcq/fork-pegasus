use pegasus_channel::base::BasePull;
use pegasus_channel::data::MiniScopeBatch;
use pegasus_channel::event::{Event, EventKind};
use pegasus_channel::input::handle::{InputHandle, MultiScopeInputHandle};
use pegasus_channel::input::AnyInput;
use pegasus_channel::output::handle::{MiniScopeStreamSink, MultiScopeOutputHandle, OutputHandle};
use pegasus_channel::output::unify::EnumStreamBufPush;
use pegasus_channel::output::AnyOutput;
use pegasus_common::tag::Tag;
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
    pub index: u16,
    pub scope_level: u8,
    pub is_multi_scope: bool,
}

impl std::fmt::Display for OperatorInfo {
    fn fmt(&self, f: &mut std::fmt::Formatter) -> std::fmt::Result {
        write!(f, "[{}_{}]", self.name, self.index)
    }
}

impl OperatorInfo {
    pub fn new(name: &str, index: u16, scope_level: u8) -> Self {
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

    fn abort(&mut self, output_port: u8, tag: Tag) -> Result<(), JobExecError>;

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

    pub fn accept_event(&mut self, event: Event) -> Result<(), JobExecError> {
        assert_eq!(event.target_port.index, self.info.index);
        let src = event.from_worker;
        let to_port = event.target_port;
        match event.take_kind() {
            EventKind::Eos(end) => {
                let offset = to_port.port as usize;
                if let Some(ref op) = self.core {
                    let inputs = op.inputs();
                    assert!(offset < inputs.len());
                    inputs[offset].notify_eos(src, end)?;
                } else {
                    panic!("accept event after operator finished;");
                }
            }
            EventKind::Abort(tag) => {
                let offset = to_port.port as usize;
                if let Some(ref mut op) = self.core {
                    let outputs = op.outputs();
                    assert!(offset < outputs.len());
                    if let Some(tag) = outputs[offset].abort(tag, src) {
                        op.abort(to_port.port, tag)?;
                    }
                }
            }
        }
        Ok(())
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
            assert!(self.is_finished);
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

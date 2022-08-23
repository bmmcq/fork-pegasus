use std::any::Any;

use pegasus_channel::block::BlockHandle;
use pegasus_channel::data::Data;
use pegasus_channel::error::PushError;
use pegasus_channel::event::emitter::EventEmitter;
use pegasus_channel::input::handle::{MiniScopeBatchQueue, PopEntry};
use pegasus_channel::input::proxy::{InputProxy, MultiScopeInputProxy};
use pegasus_channel::input::AnyInput;
use pegasus_channel::output::builder::OutputBuilder;
use pegasus_channel::output::handle::MiniScopeStreamSinkFactory;
use pegasus_channel::output::proxy::{MultiScopeOutputProxy, OutputProxy};
use pegasus_channel::output::AnyOutput;
use pegasus_common::downcast::AsAny;
use pegasus_common::tag::Tag;

use crate::errors::JobExecError;
use crate::operators::builder::{BuildCommon, Builder};
use crate::operators::{MultiScopeOutput, Operator, Output, State};

pub struct RepeatSwitchOperator<D> {
    #[allow(dead_code)]
    worker_index: u16,
    times: u32,
    #[allow(dead_code)]
    event_emitter: EventEmitter,
    inputs: [Box<dyn AnyInput>; 2],
    outputs: [Box<dyn AnyOutput>; 2],
    _ph: std::marker::PhantomData<D>,
}

impl<D> Operator for RepeatSwitchOperator<D>
where
    D: Data,
{
    fn inputs(&self) -> &[Box<dyn AnyInput>] {
        &self.inputs.as_slice()
    }

    fn outputs(&self) -> &[Box<dyn AnyOutput>] {
        &self.outputs.as_slice()
    }

    fn fire(&mut self) -> Result<State, JobExecError> {
        let mut leave = OutputProxy::<D>::downcast(&self.outputs[0]).expect("output type cast fail;");
        if leave.has_blocks() {
            leave.try_unblock()?;
        }

        let mut repeat =
            MultiScopeOutputProxy::<D>::downcast(&self.outputs[1]).expect("output type cast fail;");
        if repeat.has_blocks() {
            repeat.try_unblock()?;
        }

        let mut feedback =
            MultiScopeInputProxy::<D>::downcast(&self.inputs[1]).expect("input type cast fail;");
        if feedback.check_ready()? {
            for stream in feedback.streams() {
                let loops = stream.tag().current_uncheck();
                if loops >= self.times {
                    // send to output 0 which leave loop;
                    if leave_repeat(stream, &mut leave)? {
                        repeat.close()?;
                        return Ok(State::Finished);
                    }
                } else {
                    // send into loop;
                    next_repeat(stream, &mut repeat)?;
                }
            }
        }

        let mut enter = InputProxy::<D>::downcast(&self.inputs[0]).expect("input type case fal;");
        if enter.check_ready()? {
            let stream = enter
                .streams()
                .next()
                .expect("at least one stream");
            next_repeat(stream, &mut repeat)?;
        }

        if leave.has_blocks() {
            Ok(State::Blocking(1))
        } else {
            Ok(State::Idle)
        }
    }

    fn abort(&mut self, _output_port: u8, _tag: Tag) -> Result<(), JobExecError> {
        todo!()
    }

    fn close(&mut self) {
        if let Err(e) = self.outputs[0].close() {
            error!("repeat switch operator close fail: {}", e);
        }
    }
}

fn leave_repeat<D>(stream: &mut MiniScopeBatchQueue<D>, leave: &mut Output<D>) -> Result<bool, PushError>
where
    D: Data,
{
    let mut session = leave.new_session(stream.tag())?.expect("");
    loop {
        match stream.front() {
            PopEntry::Ready(batch) => match session.give_iterator(batch.take_data()) {
                Ok(()) => {
                    if let Some(end) = batch.take_end() {
                        session.notify_end(end)?;
                        return Ok(true);
                    }
                }
                Err(PushError::WouldBlock(b)) => {
                    if let Some(guard) = b {
                        stream.block(guard.into());
                    }
                    return Ok(false);
                }
                Err(e) => return Err(e),
            },
            _ => break,
        }
        let _discard = stream.pop();
    }
    Ok(false)
}

fn next_repeat<D>(
    stream: &mut MiniScopeBatchQueue<D>, enter: &mut MultiScopeOutput<D>,
) -> Result<(), PushError>
where
    D: Data,
{
    let mut session = enter.new_session(stream.tag())?.expect("");
    loop {
        match stream.front() {
            PopEntry::End | PopEntry::NotReady => break,
            PopEntry::Ready(batch) => match session.give_iterator(batch.take_data()) {
                Ok(()) => {
                    if let Some(end) = batch.take_end() {
                        session.notify_end(end)?;
                    }
                }
                Err(PushError::WouldBlock(b)) => {
                    if let Some(guard) = b {
                        stream.block(guard.into());
                    }
                    return Ok(());
                }
                Err(e) => return Err(e),
            },
        }
        let _discard = stream.pop();
    }
    Ok(())
}

pub struct RepeatSwitchOperatorBuilder<D> {
    worker_index: u16,
    times: u32,
    enter_loop: Box<dyn AnyInput>,
    feedback: Option<Box<dyn AnyInput>>,
    leave: Box<dyn OutputBuilder>,
    reenter: Box<dyn OutputBuilder>,
    _ph: std::marker::PhantomData<D>,
}

impl<D> RepeatSwitchOperatorBuilder<D> {
    pub fn new(mut common: BuildCommon, times: u32) -> Self {
        let enter_loop = common
            .inputs
            .pop()
            .expect("enter loop input not found;");
        let reenter = common
            .outputs
            .pop()
            .expect("reenter loop output not found;");
        let leave = common
            .outputs
            .pop()
            .expect("leave loop output not found;");

        Self {
            worker_index: common.worker_index,
            times,
            enter_loop,
            feedback: None,
            leave,
            reenter,
            _ph: std::marker::PhantomData,
        }
    }
}

impl<D> AsAny for RepeatSwitchOperatorBuilder<D>
where
    D: Data,
{
    fn as_any_mut(&mut self) -> &mut dyn Any {
        self
    }

    fn as_any_ref(&self) -> &dyn Any {
        self
    }
}

impl<D> Builder for RepeatSwitchOperatorBuilder<D>
where
    D: Data,
{
    fn add_feedback(&mut self, feedback: Box<dyn AnyInput>) {
        self.feedback = Some(feedback);
    }

    fn build(mut self: Box<Self>, event_emitter: EventEmitter) -> Box<dyn Operator> {
        let feedback = self
            .feedback
            .take()
            .expect("repeat feedback not found;");
        Box::new(RepeatSwitchOperator {
            worker_index: self.worker_index,
            times: self.times,
            event_emitter,
            inputs: [self.enter_loop, feedback],
            outputs: [self.leave.build(), self.reenter.build()],
            _ph: self._ph,
        })
    }
}

pub mod switch_unary;

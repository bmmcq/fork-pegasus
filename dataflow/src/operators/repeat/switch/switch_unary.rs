use std::any::Any;

use pegasus_channel::block::BlockHandle;
use pegasus_channel::data::Data;
use pegasus_channel::event::emitter::EventEmitter;
use pegasus_channel::input::proxy::{InputProxy, MultiScopeInputProxy};
use pegasus_channel::input::AnyInput;
use pegasus_channel::output::handle::MiniScopeStreamSink;
use pegasus_channel::output::proxy::{MultiScopeOutputProxy, OutputProxy};
use pegasus_channel::output::AnyOutput;
use pegasus_common::downcast::AsAny;
use pegasus_common::tag::Tag;

use crate::errors::JobExecError;
use crate::operators::builder::{BuildCommon, Builder};
use crate::operators::consume::MiniScopeBatchStream;
use crate::operators::repeat::switch::RepeatSwitchOperatorBuilder;
use crate::operators::unary::unary_consume;
use crate::operators::{MultiScopeOutput, Operator, State};

pub struct RepeatSwitchUnaryOperator<I, O, F> {
    #[allow(dead_code)]
    worker_index: u16,
    times: u32,
    #[allow(dead_code)]
    event_emitter: EventEmitter,
    inputs: [Box<dyn AnyInput>; 2],
    outputs: [Box<dyn AnyOutput>; 2],
    unary: F,
    _ph: std::marker::PhantomData<(I, O)>,
}

impl<I, O, F> Operator for RepeatSwitchUnaryOperator<I, O, F>
where
    I: Data,
    O: Data,
    F: FnMut(
            &mut MiniScopeBatchStream<I>,
            &mut MiniScopeStreamSink<O, MultiScopeOutput<O>>,
        ) -> Result<(), JobExecError>
        + Send
        + 'static,
{
    fn inputs(&self) -> &[Box<dyn AnyInput>] {
        todo!()
    }

    fn outputs(&self) -> &[Box<dyn AnyOutput>] {
        todo!()
    }

    fn fire(&mut self) -> Result<State, JobExecError> {
        let mut leave = OutputProxy::<I>::downcast(&self.outputs[0]).expect("output type cast fail;");
        if leave.has_blocks() {
            leave.try_unblock()?;
        }

        let mut repeat =
            MultiScopeOutputProxy::<O>::downcast(&self.outputs[1]).expect("output type cast fail;");
        if repeat.has_blocks() {
            repeat.try_unblock()?;
        }

        let mut feedback =
            MultiScopeInputProxy::<I>::downcast(&self.inputs[1]).expect("input type cast fail;");
        if feedback.check_ready()? {
            for stream in feedback.streams() {
                let loops = stream.tag().current_uncheck();
                if loops >= self.times {
                    // send to output 0 which leave loop;
                    if super::leave_repeat(stream, &mut leave)? {
                        repeat.close()?;
                        return Ok(State::Finished);
                    }
                } else {
                    // send into loop;
                    unary_consume(stream, &mut *repeat, &mut self.unary)?;
                }
            }
        }

        let mut enter = InputProxy::<I>::downcast(&self.inputs[0]).expect("input type case fal;");
        if enter.check_ready()? {
            let stream = enter
                .streams()
                .next()
                .expect("at least one stream");
            unary_consume(stream, &mut *repeat, &mut self.unary)?;
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

pub struct RepeatSwitchUnaryOperatorBuilder<I, O, F> {
    inner: RepeatSwitchOperatorBuilder<I>,
    unary: F,
    _ph: std::marker::PhantomData<O>,
}

impl<I, O, F> RepeatSwitchUnaryOperatorBuilder<I, O, F> {
    pub fn new(times: u32, common: BuildCommon, unary: F) -> Self {
        Self {
            inner: RepeatSwitchOperatorBuilder::new(common, times),
            unary,
            _ph: std::marker::PhantomData,
        }
    }
}

impl<I, O, F> AsAny for RepeatSwitchUnaryOperatorBuilder<I, O, F>
where
    I: Data,
    O: Data,
    F: Send + 'static,
{
    fn as_any_mut(&mut self) -> &mut dyn Any {
        self
    }

    fn as_any_ref(&self) -> &dyn Any {
        self
    }
}

impl<I, O, F> Builder for RepeatSwitchUnaryOperatorBuilder<I, O, F>
where
    I: Data,
    O: Data,
    F: FnMut(
            &mut MiniScopeBatchStream<I>,
            &mut MiniScopeStreamSink<O, MultiScopeOutput<O>>,
        ) -> Result<(), JobExecError>
        + Send
        + 'static,
{
    fn build(mut self: Box<Self>, event_emitter: EventEmitter) -> Box<dyn Operator> {
        let feedback = self
            .inner
            .feedback
            .take()
            .expect("feedback not found;");
        let inputs = [self.inner.enter_loop, feedback];
        let outputs = [self.inner.leave.build(), self.inner.reenter.build()];
        Box::new(RepeatSwitchUnaryOperator {
            worker_index: self.inner.worker_index,
            times: self.inner.times,
            event_emitter: event_emitter,
            inputs,
            outputs,
            unary: self.unary,
            _ph: std::marker::PhantomData,
        })
    }
}

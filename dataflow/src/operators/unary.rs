use std::any::Any;

use pegasus_channel::block::BlockHandle;
use pegasus_channel::data::Data;
use pegasus_channel::event::emitter::EventEmitter;
use pegasus_channel::event::Event;
use pegasus_channel::input::handle::{MiniScopeBatchQueue, PopEntry};
use pegasus_channel::input::proxy::{InputProxy, MultiScopeInputProxy};
use pegasus_channel::input::AnyInput;
use pegasus_channel::output::builder::OutputBuilder;
use pegasus_channel::output::handle::{MiniScopeStreamSink, MiniScopeStreamSinkFactory};
use pegasus_channel::output::proxy::{MultiScopeOutputProxy, OutputProxy};
use pegasus_channel::output::streaming::{Pinnable, StreamPush};
use pegasus_channel::output::AnyOutput;
use pegasus_channel::ChannelType;
use pegasus_common::downcast::AsAny;
use pegasus_common::tag::Tag;

use super::{MultiScopeOutput, OperatorTrait};
use crate::errors::JobExecError;
use crate::operators::builder::{BuildCommon, Builder};
use crate::operators::consume::MiniScopeBatchStream;
use crate::operators::{Output, State};

trait UnaryShape: Send + 'static {
    fn on_fire(
        &mut self, worker_index: u16, input: &Box<dyn AnyInput>, output: &Box<dyn AnyOutput>,
        event_emitter: &mut EventEmitter,
    ) -> Result<State, JobExecError>;
}

pub struct UnaryImpl<I, O, F> {
    func: F,
    _ph: std::marker::PhantomData<(I, O)>,
}

impl<I, O, F> UnaryImpl<I, O, F> {
    pub fn new(func: F) -> Self {
        Self { func, _ph: std::marker::PhantomData }
    }
}

pub struct MultiScopeUnaryImpl<I, O, F> {
    func: F,
    _ph: std::marker::PhantomData<(I, O)>,
}

impl<I, O, F> MultiScopeUnaryImpl<I, O, F> {
    pub fn new(func: F) -> Self {
        Self { func, _ph: std::marker::PhantomData }
    }
}

pub(crate) fn unary_consume<I, O, T, F, SF>(
    src: &mut MiniScopeBatchQueue<I>, sink_factory: &mut SF, func: &mut F,
) -> Result<(), JobExecError>
where
    I: Data,
    O: Data,
    T: StreamPush<O> + BlockHandle<O> + Pinnable,
    SF: MiniScopeStreamSinkFactory<O, T>,
    F: FnMut(&mut MiniScopeBatchStream<I>, &mut MiniScopeStreamSink<O, T>) -> Result<(), JobExecError>
        + Send
        + 'static,
{
    if let Some(mut sink) = sink_factory.new_session(src.tag())? {
        {
            let mut src_ext = MiniScopeBatchStream::new(src);
            match (func)(&mut src_ext, &mut sink) {
                Ok(_) => (),
                Err(JobExecError::Inner { mut source }) => {
                    if let Some(b) = source.check_data_block() {
                        src_ext.block(b);
                    } else {
                        return Err(JobExecError::Inner { source });
                    }
                }
                Err(e) => {
                    return Err(e);
                }
            }
        };

        let mut end = None;
        match src.front() {
            PopEntry::End | PopEntry::NotReady => {}
            PopEntry::Ready(b) => {
                if b.is_empty() {
                    assert!(b.is_last());
                    end = b.take_end();
                }
            }
        }

        if let Some(eos) = end {
            src.pop();
            Ok(sink.notify_end(eos)?)
        } else {
            Ok(sink.flush()?)
        }
    } else {
        Ok(())
    }
}

impl<I, O, F> UnaryShape for UnaryImpl<I, O, F>
where
    I: Data,
    O: Data,
    F: FnMut(
            &mut MiniScopeBatchStream<I>,
            &mut MiniScopeStreamSink<O, Output<O>>,
        ) -> Result<(), JobExecError>
        + Send
        + 'static,
{
    fn on_fire(
        &mut self, worker_index: u16, input: &Box<dyn AnyInput>, output: &Box<dyn AnyOutput>,
        event_emitter: &mut EventEmitter,
    ) -> Result<State, JobExecError> {
        let mut output_proxy = OutputProxy::<O>::downcast(output).expect("output type cast fail;");

        if output_proxy.has_blocks() {
            output_proxy.try_unblock()?;
            if output_proxy.has_blocks() {
                return Ok(State::Blocking(1));
            }
        }

        let mut input_proxy = InputProxy::<I>::downcast(input).expect("input type cast fail;");
        let port = input_proxy.info().source_port;
        let ch_type = input_proxy.info().ch_type;
        if input_proxy.check_ready()? {
            let mut once = input_proxy.streams();
            let stream = once
                .next()
                .expect("should be as least one stream;");

            unary_consume(stream, &mut *output_proxy, &mut self.func)?;

            if stream.is_abort() && !stream.is_exhaust() {
                let abort_event = Event::abort(worker_index, stream.tag().clone(), port);
                match ch_type {
                    ChannelType::SPSC => {
                        event_emitter.send(worker_index, abort_event)?;
                    }
                    ChannelType::MPSC | ChannelType::MPMC => {
                        event_emitter.broadcast(abort_event)?;
                    }
                }
            }
        }

        if output_proxy.has_blocks() {
            Ok(State::Blocking(1))
        } else if input_proxy.is_exhaust() {
            Ok(State::Finished)
        } else {
            Ok(State::Idle)
        }
    }
}

impl<I, O, F> UnaryShape for MultiScopeUnaryImpl<I, O, F>
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
    fn on_fire(
        &mut self, worker_index: u16, input: &Box<dyn AnyInput>, output: &Box<dyn AnyOutput>,
        event_emitter: &mut EventEmitter,
    ) -> Result<State, JobExecError> {
        let mut input_proxy = MultiScopeInputProxy::<I>::downcast(input).expect("input type cast fail;");
        let mut output_proxy =
            MultiScopeOutputProxy::<O>::downcast(output).expect("output type cast fail;");

        if output_proxy.has_blocks() {
            output_proxy.try_unblock()?;
        }

        if input_proxy.check_ready()? {
            let port = input_proxy.info().source_port;
            let ch_type = input_proxy.info().ch_type;
            for stream in input_proxy.streams() {
                unary_consume(stream, &mut *output_proxy, &mut self.func)?;

                if stream.is_abort() && !stream.is_exhaust() {
                    let abort_event = Event::abort(worker_index, stream.tag().clone(), port);
                    match ch_type {
                        ChannelType::SPSC => {
                            event_emitter.send(worker_index, abort_event)?;
                        }
                        ChannelType::MPSC | ChannelType::MPMC => {
                            event_emitter.broadcast(abort_event)?;
                        }
                    }
                }
            }
        }

        if output_proxy.has_blocks() {
            Ok(State::Blocking(output_proxy.block_scope_size()))
        } else if input_proxy.is_exhaust() {
            Ok(State::Finished)
        } else {
            Ok(State::Idle)
        }
    }
}

pub struct UnaryOperator<F> {
    worker_index: u16,
    event_emitter: EventEmitter,
    input: [Box<dyn AnyInput>; 1],
    output: [Box<dyn AnyOutput>; 1],
    func: F,
}

impl<F> UnaryOperator<F> {
    pub fn new(
        worker_index: u16, event_emitter: EventEmitter, input: Box<dyn AnyInput>,
        output: Box<dyn AnyOutput>, func: F,
    ) -> Self {
        Self { worker_index, event_emitter, input: [input], output: [output], func }
    }
}

impl<F> OperatorTrait for UnaryOperator<F>
where
    F: UnaryShape,
{
    fn inputs(&self) -> &[Box<dyn AnyInput>] {
        self.input.as_slice()
    }

    fn outputs(&self) -> &[Box<dyn AnyOutput>] {
        self.output.as_slice()
    }

    fn fire(&mut self) -> Result<State, JobExecError> {
        self.func
            .on_fire(self.worker_index, &self.input[0], &self.output[0], &mut self.event_emitter)
    }

    fn abort(&mut self, output_port: u8, tag: Tag) -> Result<(), JobExecError> {
        assert_eq!(output_port, 0);
        self.input[0].abort(&tag);

        let port = self.input[0].info().source_port;
        let event = Event::abort(self.worker_index, tag, port);
        match self.input[0].info().ch_type {
            ChannelType::SPSC => {
                self.event_emitter
                    .send(self.worker_index, event)?;
            }
            ChannelType::MPSC | ChannelType::MPMC => {
                self.event_emitter.broadcast(event)?;
            }
        }

        Ok(())
    }

    fn close(&mut self) {
        if let Err(e) = self.output[0].close() {
            error!("close operation failed {}", e);
        }
    }
}

pub struct UnaryOperatorBuilder<F> {
    worker_index: u16,
    input: Box<dyn AnyInput>,
    output: Box<dyn OutputBuilder>,
    func: F,
}

impl<F> UnaryOperatorBuilder<F> {
    pub fn new(mut build_common: BuildCommon, func: F) -> Self {
        Self {
            worker_index: build_common.worker_index,
            input: build_common
                .inputs
                .pop()
                .expect("unary input not found;"),
            output: build_common
                .outputs
                .pop()
                .expect("unary output not found"),
            func,
        }
    }
}

impl<F> AsAny for UnaryOperatorBuilder<F>
where
    F: Send + 'static,
{
    fn as_any_mut(&mut self) -> &mut dyn Any {
        self
    }

    fn as_any_ref(&self) -> &dyn Any {
        self
    }
}

impl<F> Builder for UnaryOperatorBuilder<F>
where
    F: UnaryShape,
{
    fn build(self: Box<Self>, event_emitter: EventEmitter) -> Box<dyn OperatorTrait> {
        let output = self.output.build();
        Box::new(UnaryOperator::new(self.worker_index, event_emitter, self.input, output, self.func))
    }
}

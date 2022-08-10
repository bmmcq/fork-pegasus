use pegasus_channel::block::BlockHandle;
use pegasus_channel::data::{Data};
use pegasus_channel::input::handle::{MiniScopeBatchStream, PopEntry};
use pegasus_channel::input::proxy::{InputProxy, MultiScopeInputProxy};
use pegasus_channel::input::{AnyInput};
use pegasus_channel::output::handle::{MiniScopeStreamSink, MiniScopeStreamSinkFactory};
use pegasus_channel::output::proxy::{MultiScopeOutputProxy, OutputProxy};
use pegasus_channel::output::streaming::{Pinnable, StreamPush};
use pegasus_channel::output::AnyOutput;

use super::{MultiScopeOutput, Operator};
use crate::error::JobExecError;
use crate::operators::consume::MiniScopeBatchStreamExt;
use crate::operators::Output;

pub trait UnaryFunction : Send + 'static {
    fn on_receive(
        &mut self, input: &Box<dyn AnyInput>, output: &Box<dyn AnyOutput>,
    ) -> Result<(), JobExecError>;
}

struct UnaryImpl<I, O, F> {
    func: F,
    _ph: std::marker::PhantomData<(I, O)>,
}

impl<I, O, F> UnaryImpl<I, O, F> {
    fn _new(func: F) -> Self {
        Self { func, _ph: std::marker::PhantomData }
    }
}

struct MultiScopeUnaryImpl<I, O, F> {
    func: F,
    _ph: std::marker::PhantomData<(I, O)>,
}

impl<I, O, F> MultiScopeUnaryImpl<I, O, F> {
    fn _new(func: F) -> Self {
        Self { func, _ph: std::marker::PhantomData }
    }
}

fn unary_consume<I, O, T, F, SF>(
    src: &mut MiniScopeBatchStream<I>, sink_factory: &mut SF, func: &mut F,
) -> Result<(), JobExecError>
where
    I: Data,
    O: Data,
    T: StreamPush<O> + BlockHandle<O> + Pinnable,
    SF: MiniScopeStreamSinkFactory<O, T>,
    F: FnMut(&mut MiniScopeBatchStreamExt<I>, &mut MiniScopeStreamSink<O, T>) -> Result<(), JobExecError> + Send + 'static,
{
    if let Some(mut sink) = sink_factory.new_session(src.tag()) {
        {
            let mut src_ext = MiniScopeBatchStreamExt::new(src);
            match (func)(&mut src_ext, &mut sink) {
                Ok(_) => (),
                Err(JobExecError::Inner { mut source }) => {
                    if let Some(b) = source.check_data_block() {
                        src_ext.block(b);
                    } else if let Some(tag) = source.check_data_abort() {
                        assert_eq!(&tag, src_ext.tag());
                        src_ext.abort();
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
            src.pull();
            Ok(sink.notify_end(eos)?)
        } else {
            Ok(sink.flush()?)
        }
    } else {
        Ok(())
    }
}

impl<I, O, F> UnaryFunction for UnaryImpl<I, O, F>
where
    I: Data,
    O: Data,
    F: FnMut(
        &mut MiniScopeBatchStreamExt<I>,
        &mut MiniScopeStreamSink<O, Output<O>>,
    ) -> Result<(), JobExecError> + Send + 'static,
{
    fn on_receive(
        &mut self, input: &Box<dyn AnyInput>, output: &Box<dyn AnyOutput>,
    ) -> Result<(), JobExecError> {
        let mut input_proxy = InputProxy::<I>::downcast(input).expect("input type cast fail;");
        let mut output_proxy = OutputProxy::<O>::downcast(output).expect("output type cast fail;");
        let mut once = input_proxy.streams();
        let stream = once
            .next()
            .expect("should be as least one stream;");
        unary_consume(stream, &mut *output_proxy, &mut self.func)?;
        Ok(())
    }
}

impl<I, O, F> UnaryFunction for MultiScopeUnaryImpl<I, O, F>
where
    I: Data,
    O: Data,
    F: FnMut(
        &mut MiniScopeBatchStreamExt<I>,
        &mut MiniScopeStreamSink<O, MultiScopeOutput<O>>,
    ) -> Result<(), JobExecError> + Send + 'static,
{
    fn on_receive(
        &mut self, input: &Box<dyn AnyInput>, output: &Box<dyn AnyOutput>,
    ) -> Result<(), JobExecError> {
        let mut input_proxy = MultiScopeInputProxy::<I>::downcast(input).expect("input type cast fail;");
        let mut output_proxy =
            MultiScopeOutputProxy::<O>::downcast(output).expect("output type cast fail;");

        for stream in input_proxy.streams() {
            unary_consume(stream, &mut *output_proxy, &mut self.func)?;
        }
        Ok(())
    }
}

pub struct UnaryOperator {
    input: [Box<dyn AnyInput>; 1],
    output: [Box<dyn AnyOutput>; 1],
    func: Box<dyn UnaryFunction>,
}

impl UnaryOperator {
    pub fn new(input: Box<dyn AnyInput>, output: Box<dyn AnyOutput>,
        func: Box<dyn UnaryFunction>,
    ) -> Self {
        Self { input: [input], output: [output], func }
    }
}

impl Operator for UnaryOperator {

    fn inputs(&self) -> &[Box<dyn AnyInput>] {
        self.input.as_slice()
    }

    fn outputs(&self) -> &[Box<dyn AnyOutput>] {
        self.output.as_slice()
    }

    fn fire(&mut self) -> Result<(), JobExecError> {
        self.func
            .on_receive(&self.input[0], &self.output[0])
    }

    fn close(&mut self) {
        if let Err(e) = self.output[0].close() {
            error!("close operation failed {}", e);
        }
    }
}

use pegasus_channel::block::{BlockGuard, BlockHandle};
use pegasus_channel::data::Data;
use pegasus_channel::error::{IOErrorKind, PushError};
use pegasus_channel::input::proxy::{InputProxy, MultiScopeInputProxy};
use pegasus_channel::input::AnyInput;
use pegasus_channel::input::handle::MiniScopeBatchStream;
use pegasus_channel::output::proxy::{MultiScopeOutputProxy, OutputProxy};
use pegasus_channel::output::AnyOutput;
use pegasus_channel::output::handle::{MiniScopeStreamSink};
use pegasus_channel::output::streaming::StreamPush;

use super::{MultiScopeOutput, Operator, OperatorInfo};
use crate::error::{JobExecError};
use crate::operators::Output;

pub trait UnaryFunction {
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


fn consume_stream<I, O, T, F>(src: &mut MiniScopeBatchStream<I>, sink: &mut MiniScopeStreamSink<O, T>, func: &mut F) -> Result<(), JobExecError>
    where I: Data,
          O: Data,
          T: StreamPush<O> + BlockHandle<O>,
          F: FnMut(&mut MiniScopeBatchStream<I>, &mut MiniScopeStreamSink<O, T>) -> Result<(), JobExecError>

{
    match (func)(src, sink) {
        Ok(_) => (),
        Err(JobExecError::Inner { mut source }) => {
            if let Some(b) = source.check_data_block() {
                src.block(b);
            } else if let Some(tag) = source.check_data_abort() {
                assert_eq!(&tag, src.tag());
                src.abort();
            } else {
                return Err(JobExecError::Inner { source });
            }
        },
        Err(e) => {
            return Err(e);
        }
    }
    Ok(sink.flush()?)
}

impl<I, O, F> UnaryFunction for UnaryImpl<I, O, F>
where
    I: Data,
    O: Data,
    F: FnMut(&mut MiniScopeBatchStream<I>, &mut MiniScopeStreamSink<O, Output<O>>) -> Result<(), JobExecError>,
{
    fn on_receive(
        &mut self, input: &Box<dyn AnyInput>, output: &Box<dyn AnyOutput>,
    ) -> Result<(), JobExecError> {
        let mut input_proxy = InputProxy::<I>::downcast(input).expect("input type cast fail;");
        let mut output_proxy = OutputProxy::<O>::downcast(output).expect("output type cast fail;");
        for stream in input_proxy.streams() {
            match output_proxy.new_session(stream.tag().clone()) {
                Ok(mut session) => {
                    consume_stream(stream, &mut session, &mut self.func)?;
                }
                Err(PushError::Aborted(tag)) => {
                    assert_eq!(&tag, stream.tag());
                    stream.abort();
                },
                Err(PushError::WouldBlock(b)) => {
                    if let Some(b) = b {
                        assert_eq!(&b.0, stream.tag());
                        stream.block(BlockGuard::from(b));
                    }
                }
                Err(source) => return Err(IOErrorKind::from(source))?,
            }
        }
        Ok(())
    }
}

impl<I, O, F> UnaryFunction for MultiScopeUnaryImpl<I, O, F>
where
    I: Data,
    O: Data,
    F: FnMut(&mut MiniScopeBatchStream<I>, &mut MiniScopeStreamSink<O, MultiScopeOutput<O>>) -> Result<(), JobExecError>,
{
    fn on_receive(
        &mut self, input: &Box<dyn AnyInput>, output: &Box<dyn AnyOutput>,
    ) -> Result<(), JobExecError> {
        let mut input_proxy = MultiScopeInputProxy::<I>::downcast(input).expect("input type cast fail;");
        let mut output_proxy =
            MultiScopeOutputProxy::<O>::downcast(output).expect("output type cast fail;");
        for stream in input_proxy.streams() {
            match output_proxy.new_session(stream.tag().clone()) {
                Ok(mut session) => {
                    consume_stream(stream, &mut session, &mut self.func)?;
                }
                Err(PushError::Aborted(tag)) => {
                    assert_eq!(&tag, stream.tag());
                    stream.abort();
                },
                Err(PushError::WouldBlock(b)) => {
                    if let Some(b) = b {
                        assert_eq!(&b.0, stream.tag());
                        stream.block(BlockGuard::from(b));
                    }
                }
                Err(source) => return Err(IOErrorKind::from(source))?,
            }
        }
        Ok(())
    }
}

pub struct UnaryOperator {
    info: OperatorInfo,
    input: Box<dyn AnyInput>,
    output: Box<dyn AnyOutput>,
    func: Box<dyn UnaryFunction>,
}

impl UnaryOperator {
    pub fn new(
        info: OperatorInfo, input: Box<dyn AnyInput>, output: Box<dyn AnyOutput>,
        func: Box<dyn UnaryFunction>,
    ) -> Self {
        Self { info, input, output, func }
    }
}

impl Operator for UnaryOperator {
    fn info(&self) -> &OperatorInfo {
        &self.info
    }

    fn fire(&mut self) -> Result<(), JobExecError> {
        self.func.on_receive(&self.input, &self.output)
    }
}

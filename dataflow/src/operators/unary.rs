use pegasus_channel::data::Data;
use pegasus_channel::input::proxy::{InputProxy, MultiScopeInputProxy};
use pegasus_channel::input::AnyInput;
use pegasus_channel::output::proxy::{MultiScopeOutputProxy, OutputProxy};
use pegasus_channel::output::AnyOutput;

use super::{BatchInput, BatchOutput, MultiScopeBatchInput, MultiScopeBatchOutput, Operator, OperatorInfo};
use crate::error::JobExecError;

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
    fn new(func: F) -> Self {
        Self { func, _ph: std::marker::PhantomData }
    }
}

struct MultiScopeUnaryImpl<I, O, F> {
    func: F,
    _ph: std::marker::PhantomData<(I, O)>,
}

impl<I, O, F> MultiScopeUnaryImpl<I, O, F> {
    fn new(func: F) -> Self {
        Self { func, _ph: std::marker::PhantomData }
    }
}

impl<I, O, F> UnaryFunction for UnaryImpl<I, O, F>
where
    I: Data,
    O: Data,
    F: FnMut(&mut BatchInput<I>, &mut BatchOutput<O>) -> Result<(), JobExecError>,
{
    fn on_receive(
        &mut self, input: &Box<dyn AnyInput>, output: &Box<dyn AnyOutput>,
    ) -> Result<(), JobExecError> {
        let mut input_proxy = InputProxy::<I>::downcast(input).expect("input type cast fail;");
        let mut output_proxy = OutputProxy::<O>::downcast(output).expect("output type cast fail;");
        (self.func)(&mut input_proxy, &mut output_proxy)
    }
}

impl<I, O, F> UnaryFunction for MultiScopeUnaryImpl<I, O, F>
where
    I: Data,
    O: Data,
    F: FnMut(&mut MultiScopeBatchInput<I>, &mut MultiScopeBatchOutput<O>) -> Result<(), JobExecError>,
{
    fn on_receive(
        &mut self, input: &Box<dyn AnyInput>, output: &Box<dyn AnyOutput>,
    ) -> Result<(), JobExecError> {
        let mut input_proxy = MultiScopeInputProxy::<I>::downcast(input).expect("input type cast fail;");
        let mut output_proxy =
            MultiScopeOutputProxy::<O>::downcast(output).expect("output type case fail;");
        (self.func)(&mut input_proxy, &mut output_proxy)
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

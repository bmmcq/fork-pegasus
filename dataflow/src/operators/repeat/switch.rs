use pegasus_channel::data::Data;
use pegasus_channel::event::emitter::BaseEventEmitter;
use pegasus_channel::input::AnyInput;
use pegasus_channel::input::proxy::InputProxy;
use pegasus_channel::output::AnyOutput;
use pegasus_common::tag::Tag;
use crate::errors::JobExecError;
use crate::operators::{Operator, State};

pub struct RepeatSourceOperator<D: Data> {
    worker_index: u16,
    times: u32,
    event_emitter: BaseEventEmitter,
    input: [Box<dyn AnyInput>; 2],
    output: [Box<dyn AnyOutput>; 1],
    _ph: std::marker::PhantomData<D>
}

impl <D: Data> Operator for RepeatSourceOperator<D> {
    fn inputs(&self) -> &[Box<dyn AnyInput>] {
        todo!()
    }

    fn outputs(&self) -> &[Box<dyn AnyOutput>] {
        todo!()
    }

    fn fire(&mut self) -> Result<State, JobExecError> {
        let mut input = InputProxy::<D>::downcast(&self.input[0]).expect("input type cast fail;");
        input.streams();
        todo!()
    }

    fn abort(&mut self, output_port: u8, tag: Tag) -> Result<(), JobExecError> {
        todo!()
    }

    fn close(&mut self) {
        todo!()
    }
}




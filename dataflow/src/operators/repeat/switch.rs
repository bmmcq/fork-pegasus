use pegasus_channel::block::BlockHandle;
use pegasus_channel::data::Data;
use pegasus_channel::event::emitter::BaseEventEmitter;
use pegasus_channel::input::proxy::MultiScopeInputProxy;
use pegasus_channel::input::AnyInput;
use pegasus_channel::output::handle::MiniScopeStreamSink;
use pegasus_channel::output::proxy::MultiScopeOutputProxy;
use pegasus_channel::output::AnyOutput;
use pegasus_common::tag::Tag;

use crate::errors::JobExecError;
use crate::operators::consume::MiniScopeBatchStream;
use crate::operators::{MultiScopeOutput, Operator, State};

pub struct RepeatSwitchOperator<I, O, F> {
    worker_index: u16,
    times: u32,
    event_emitter: BaseEventEmitter,
    input: [Box<dyn AnyInput>; 2],
    output: [Box<dyn AnyOutput>; 1],
    func: F,
    _ph: std::marker::PhantomData<(I, O)>,
}

impl<I, O, F> Operator for RepeatSwitchOperator<I, O, F>
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
        let mut input = MultiScopeInputProxy::<I>::downcast(&self.input[0]).expect("input type cast fail;");

        let mut output_proxy =
            MultiScopeOutputProxy::<O>::downcast(&self.output[1]).expect("output type cast fail;");

        if output_proxy.has_blocks() {
            output_proxy.try_unblock()?;
        }

        if input.check_ready()? {
            for stream in input.streams() {
                let loops = stream.tag().current_uncheck();
                if loops >= self.times {
                    // send to output 0 which leave loop;
                } else {
                    // send into loop;
                }
            }
        }

        Ok(State::Finished)
    }

    fn abort(&mut self, output_port: u8, tag: Tag) -> Result<(), JobExecError> {
        todo!()
    }

    fn close(&mut self) {
        todo!()
    }
}

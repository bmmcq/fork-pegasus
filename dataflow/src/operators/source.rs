use std::any::Any;

use pegasus_channel::block::BlockHandle;
use pegasus_channel::data::Data;
use pegasus_channel::input::AnyInput;
use pegasus_channel::output::builder::OutputBuilder;
use pegasus_channel::output::handle::MiniScopeStreamSinkFactory;
use pegasus_channel::output::proxy::OutputProxy;
use pegasus_channel::output::AnyOutput;
use pegasus_common::downcast::AsAny;
use pegasus_common::tag::Tag;

use crate::errors::JobExecError;
use crate::operators::builder::Builder;
use crate::operators::{Operator, State};

pub struct SourceOperator<Iter> {
    extern_data: Option<Iter>,
    _inputs: Vec<Box<dyn AnyInput>>,
    output: [Box<dyn AnyOutput>; 1],
}

impl<Iter> Operator for SourceOperator<Iter>
where
    Iter: Iterator + Send + 'static,
    Iter::Item: Data,
{
    fn inputs(&self) -> &[Box<dyn AnyInput>] {
        self._inputs.as_slice()
    }

    fn outputs(&self) -> &[Box<dyn AnyOutput>] {
        self.output.as_slice()
    }

    fn fire(&mut self) -> Result<State, JobExecError> {
        let mut output_proxy =
            OutputProxy::<Iter::Item>::downcast(&self.output[0]).expect("output type cast fail;");
        if output_proxy.has_blocks() {
            output_proxy.try_unblock()?;
            if output_proxy.has_blocks() {
                Ok(State::Blocking(1))
            } else {
                Ok(State::Finished)
            }
        } else if let Some(extern_data) = self.extern_data.take() {
            let mut session = output_proxy
                .new_session(&Tag::Null)
                .expect("new session expect not none;");
            match session.give_iterator(extern_data) {
                Ok(_) => {
                    session.flush()?;
                    Ok(State::Finished)
                }
                Err(err) => {
                    if err.is_would_block() {
                        session.flush()?;
                        Ok(State::Blocking(1))
                    } else {
                        Err(err)?
                    }
                }
            }
        } else {
            Ok(State::Finished)
        }
    }

    fn abort(&mut self, output_port: u8, tag: Tag) -> Result<(), JobExecError> {
        assert_eq!(output_port, 0);
        assert_eq!(tag, Tag::Null);
        Ok(())
    }

    fn close(&mut self) {
        if let Err(e) = self.output[0].close() {
            error!("source operator close fail: {}", e);
        }
    }
}

pub struct SourceOperatorBuilder<Iter> {
    extern_data: Iter,
    output: Box<dyn OutputBuilder>,
}

impl<Iter> SourceOperatorBuilder<Iter> {
    pub fn new(extern_data: Iter, output: Box<dyn OutputBuilder>) -> Self {
        Self { extern_data, output }
    }
}

impl<Iter> AsAny for SourceOperatorBuilder<Iter>
where
    Iter: Sized + 'static,
{
    fn as_any_mut(&mut self) -> &mut dyn Any {
        self
    }

    fn as_any_ref(&self) -> &dyn Any {
        self
    }
}

impl<Iter> Builder for SourceOperatorBuilder<Iter>
where
    Iter: IntoIterator + 'static,
    Iter::Item: Data,
    Iter::IntoIter: Send + 'static,
{
    fn build(self: Box<Self>) -> Box<dyn Operator> {
        Box::new(SourceOperator {
            extern_data: Some(self.extern_data.into_iter()),
            _inputs: vec![],
            output: [self.output.build()],
        })
    }
}

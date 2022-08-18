use pegasus_channel::data::Data;
use crate::errors::{JobBuildError, JobExecError};
use crate::operators::consume::MiniScopeBatchStream;
use crate::operators::StreamSink;
use crate::plan::streams::Stream;

pub struct ExchangeStream<D: Data> {
    inner: Stream<D>
}



impl <D: Data> ExchangeStream<D> {
    pub(crate) fn new(stream: Stream<D>) -> Self {
        Self { inner: stream }
    }

    pub async fn unary<O, CF, F>(self, name: &str, construct: CF) -> Result<Stream<O>, JobBuildError>
        where
            O: Data,
            CF: FnOnce() -> F,
            F: FnMut(&mut MiniScopeBatchStream<D>, &mut StreamSink<O>) -> Result<(), JobExecError>
            + Send
            + 'static,
    {
        self.inner.unary(name, construct).await
    }
}
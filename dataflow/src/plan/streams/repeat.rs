use pegasus_channel::alloc::ChannelKind;
use pegasus_channel::data::Data;
use pegasus_channel::output::builder::{MultiScopeOutputBuilderRef, OutputBuildRef};
use crate::context::ScopeContext;
use crate::errors::{JobBuildError, JobExecError};
use crate::operators::consume::MiniScopeBatchStream;
use crate::operators::{MultiScopeStreamSink, StreamSink};
use crate::plan::builder::DataflowBuilder;
use crate::plan::streams::Stream;

pub struct RepeatSource<D: Data> {
    src: Stream<D>,
}

impl <D> RepeatSource<D> where D: Data {
    pub async fn unary<O, CF, F>(self, name: &str, construct: CF) -> Result<RepeatStream<O>, JobBuildError>
        where
            O: Data,
            CF: FnOnce() -> F,
            F: FnMut(&mut MiniScopeBatchStream<D>, &mut MultiScopeStreamSink<O>) -> Result<(), JobExecError>
            + Send
            + 'static,
    {
       todo!()
    }
}

pub struct RepeatStream<D: Data> {
    src_op_index: usize,
    batch_size: u16,
    batch_capacity: u16,
    scope_ctx: ScopeContext,
    src: MultiScopeOutputBuilderRef<D>,
    channel: ChannelKind<D>,
    builder: DataflowBuilder,
}
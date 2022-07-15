use ahash::AHashMap;
use crate::communication::buffer::ScopeBuffer;
use crate::communication::decorator::ScopeStreamPush;
use crate::communication::IOResult;
use crate::communication::output::producers::{BatchProducer, MultiScopeBatchProducer};
use crate::data::batching::RoBatch;
use crate::data::MicroBatch;
use crate::data_plane::Push;
use crate::errors::IOError;
use crate::graph::Port;
use crate::progress::Eos;
use crate::Tag;


pub type AggregateProducer<T, P> = BatchProducer<T, P>;
pub type AggregateByScopeProducer<T, P> = MultiScopeBatchProducer<T, AggregateByScopePush<T, P>>;

pub struct AggregateByScopePush<T, P> {
    src: u32,
    ch_index: u32,
    scope_level: u32,
    port: Port,
    pushes: Vec<P>
}

impl <T, P> Push<MicroBatch<T>> for AggregateByScopePush<T, P> where P: Push<MicroBatch<T>> {
    fn push(&mut self, msg: MicroBatch<T>) -> Result<(), IOError> {
       todo!()
    }

    fn flush(&mut self) -> Result<(), IOError> {
        todo!()
    }

    fn close(&mut self) -> Result<(), IOError> {
        todo!()
    }
}
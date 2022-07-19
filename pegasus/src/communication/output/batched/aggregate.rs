use ahash::AHashMap;

use crate::communication::buffer::ScopeBuffer;
use crate::communication::output::batched::Rectifier;
use crate::communication::IOResult;
use crate::data::batching::RoBatch;
use crate::data::MicroBatch;
use crate::data_plane::Push;
use crate::errors::IOError;
use crate::graph::Port;
use crate::progress::Eos;
use crate::Tag;

//pub type AggregateProducer<T, P> = BufPush<T, AggregatePush<T, P>>;
//pub type AggregateByScopeProducer<T, P> = MultiScopeBufPush<T, AggregateByScopePush<T, P>>;

pub struct AggregatePush<T, P> {
    src: u32,
    target: u32,
    ch_index: u32,
    scope_level: u32,
    port: Port,
    push: P,
}

impl<T, P> Push<MicroBatch<T>> for AggregatePush<T, P>
where
    P: Push<MicroBatch<T>>,
{
    fn push(&mut self, mut msg: MicroBatch<T>) -> Result<(), IOError> {
        if let Some(eos) = msg.get_end_mut() {
            let send = eos.total_send;
            eos.add_child_send(self.target as u32, send as usize);
        }

        self.push.push(msg)
    }

    fn flush(&mut self) -> Result<(), IOError> {
        self.push.flush()
    }

    fn close(&mut self) -> Result<(), IOError> {
        self.push.close()
    }
}

pub struct AggregateByScopePush<T, P> {
    src: u32,
    ch_index: u32,
    scope_level: u32,
    port: Port,
    rectifier: Rectifier,
    pushes: Vec<P>,
}

impl<T, P> Push<MicroBatch<T>> for AggregateByScopePush<T, P>
where
    P: Push<MicroBatch<T>>,
{
    fn push(&mut self, mut msg: MicroBatch<T>) -> Result<(), IOError> {
        assert!(!msg.tag.is_root());
        let target = self
            .rectifier
            .get(msg.tag.current_uncheck() as u64);
        if let Some(eos) = msg.get_end_mut() {
            let count = eos.total_send;
            eos.add_child_send(target as u32, count as usize);
        }
        self.pushes[target].push(msg)
    }

    fn flush(&mut self) -> Result<(), IOError> {
        for p in self.pushes.iter_mut() {
            p.flush()?;
        }
        Ok(())
    }

    fn close(&mut self) -> Result<(), IOError> {
        for p in self.pushes.iter_mut() {
            p.close()?;
        }
        Ok(())
    }
}

use crate::data::{Data, MiniScopeBatch};
use crate::output::Rectifier;
use crate::error::PushError;
use crate::{ChannelInfo, Push};

pub struct AggregatePush<T, P> {
    #[allow(dead_code)]
    ch_info: ChannelInfo,
    #[allow(dead_code)]
    worker_index: u16,
    target: u16,
    push: P,
    _ph: std::marker::PhantomData<T>,
}

impl<T, P> Push<MiniScopeBatch<T>> for AggregatePush<T, P>
where
    T: Data,
    P: Push<MiniScopeBatch<T>>,
{
    fn push(&mut self, mut msg: MiniScopeBatch<T>) -> Result<(), PushError> {
        if let Some(eos) = msg.get_end_mut() {
            let send = eos.total_send;
            eos.add_child_send(self.target, send as usize);
        }

        self.push.push(msg)
    }

    fn flush(&mut self) -> Result<(), PushError> {
        self.push.flush()
    }

    fn close(&mut self) -> Result<(), PushError> {
        self.push.close()
    }
}

pub struct AggregateByScopePush<T, P> {
    #[allow(dead_code)]
    ch_info: ChannelInfo,
    #[allow(dead_code)]
    worker_index: u16,
    rectifier: Rectifier,
    pushes: Vec<P>,
    _ph: std::marker::PhantomData<T>,
}

impl<T, P> Push<MiniScopeBatch<T>> for AggregateByScopePush<T, P>
where
    T: Data,
    P: Push<MiniScopeBatch<T>>,
{
    fn push(&mut self, mut msg: MiniScopeBatch<T>) -> Result<(), PushError> {
        assert!(!msg.tag.is_root());
        let target = self
            .rectifier
            .get(msg.tag.current_uncheck() as u64);
        if let Some(eos) = msg.get_end_mut() {
            let count = eos.total_send;
            eos.add_child_send(target as u16, count as usize);
        }
        self.pushes[target].push(msg)
    }

    fn flush(&mut self) -> Result<(), PushError> {
        for p in self.pushes.iter_mut() {
            p.flush()?;
        }
        Ok(())
    }

    fn close(&mut self) -> Result<(), PushError> {
        for p in self.pushes.iter_mut() {
            p.close()?;
        }
        Ok(())
    }
}

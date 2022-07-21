use crate::data::{Data, MiniScopeBatch};
use crate::{ChannelInfo, IOError, Push};

pub struct BroadcastPush<T, P> {
    #[allow(dead_code)]
    ch_info: ChannelInfo,
    #[allow(dead_code)]
    worker_index: u16,
    pushes: Vec<P>,
    _ph: std::marker::PhantomData<T>,
}

impl<T, P> Push<MiniScopeBatch<T>> for BroadcastPush<T, P>
where
    T: Data + Clone,
    P: Push<MiniScopeBatch<T>>,
{
    fn push(&mut self, mut msg: MiniScopeBatch<T>) -> Result<(), IOError> {
        let len = self.pushes.len();
        if let Some(eos) = msg.get_end_mut() {
            let count = eos.total_send as usize;
            for i in 0..len {
                eos.add_child_send(i as u16, count);
            }
        }
        for i in 1..len {
            self.pushes[i].push(msg.clone())?;
        }
        self.pushes[0].push(msg)
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

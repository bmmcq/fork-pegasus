use crate::data::MicroBatch;
use crate::data_plane::Push;
use crate::errors::IOError;
use crate::graph::Port;

pub struct BroadcastPush<T, P> {
    src: u32,
    ch_index: u32,
    scope_level: u32,
    port: Port,
    pushes: Vec<P>,
}

impl<T: Clone, P> Push<MicroBatch<T>> for BroadcastPush<T, P>
where
    P: Push<MicroBatch<T>>,
{
    fn push(&mut self, mut msg: MicroBatch<T>) -> Result<(), IOError> {
        let len = self.pushes.len();
        if let Some(eos) = msg.get_end_mut() {
            let count = eos.total_send as usize;
            for i in 0..len {
                eos.add_child_send(i as u32, count);
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

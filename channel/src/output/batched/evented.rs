use crate::data::{Data, MiniScopeBatch};
use crate::event::emitter::EventEmitter;
use crate::event::{Event, EventKind};
use crate::{IOError, Port, Push};

pub struct EventEosBatchPush<T, PD, PE> {
    worker_index: u16,
    target_worker: u16,
    target_port: Port,
    event_push: EventEmitter<PE>,
    inner: PD,
    _ph: std::marker::PhantomData<T>,
}

impl<T, PD, PE> Push<MiniScopeBatch<T>> for EventEosBatchPush<T, PD, PE>
where
    T: Data,
    PD: Push<MiniScopeBatch<T>>,
    PE: Push<Event>,
{
    fn push(&mut self, mut msg: MiniScopeBatch<T>) -> Result<(), IOError> {
        let mut event_eos = None;
        if let Some(end) = msg.take_end() {
            assert!(end.has_parent(self.worker_index), "unexpected eos");
            if end.parent_peers().len() > 1 {
                event_eos = Some(end);
            } else {
                msg.set_end(end);
            }
        }

        self.inner.push(msg)?;
        if let Some(eos) = event_eos.take() {
            let event = Event::new(self.worker_index, self.target_port, EventKind::Eos(eos));
            self.event_push
                .send(self.target_worker, event)?;
        }
        Ok(())
    }

    fn flush(&mut self) -> Result<(), IOError> {
        self.inner.flush()
    }

    fn close(&mut self) -> Result<(), IOError> {
        self.inner.close()
    }
}

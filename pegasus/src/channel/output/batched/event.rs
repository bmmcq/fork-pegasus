use crate::data::MicroBatch;
use crate::data_plane::{DataPlanePush, Push};
use crate::errors::IOError;
use crate::event::emitter::EventEmitter;
use crate::event::{Event, EventKind};
use crate::graph::Port;

pub struct EventEosBatchPush<T> {
    worker_index: u32,
    target_worker: u32,
    target_port: Port,
    event_push: EventEmitter,
    inner: DataPlanePush<MicroBatch<T>>,
}

impl<T> Push<MicroBatch<T>> for EventEosBatchPush<T> {
    fn push(&mut self, mut msg: MicroBatch<T>) -> Result<(), IOError> {
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

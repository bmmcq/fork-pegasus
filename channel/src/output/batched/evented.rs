use pegasus_common::tag::Tag;

use crate::abort::AbortHandle;
use crate::data::{Data, MiniScopeBatch};
use crate::error::PushError;
use crate::event::emitter::EventSender;
use crate::event::{Event, EventKind};
use crate::{Port, Push};

pub struct EventEosBatchPush<T, PD, PE> {
    src_index: u16,
    target_index: u16,
    target_port: Port,
    event_push: EventSender<PE>,
    inner: PD,
    _ph: std::marker::PhantomData<T>,
}

impl<T, PD, PE> EventEosBatchPush<T, PD, PE> {
    pub fn new(
        worker_index: u16, target_worker: u16, target_port: Port, event_push: EventSender<PE>, inner: PD,
    ) -> Self {
        Self {
            src_index: worker_index,
            target_index: target_worker,
            target_port,
            event_push,
            inner,
            _ph: std::marker::PhantomData,
        }
    }
}

impl<T, PD, PE> Push<MiniScopeBatch<T>> for EventEosBatchPush<T, PD, PE>
where
    T: Data,
    PD: Push<MiniScopeBatch<T>>,
    PE: Push<Event>,
{
    fn push(&mut self, mut msg: MiniScopeBatch<T>) -> Result<(), PushError> {
        let mut event_eos = None;
        if let Some(end) = msg.take_end() {
            assert!(end.has_parent(self.src_index), "unexpected eos");
            if end.parent_peers().len() > 1 {
                event_eos = Some(end);
            } else {
                msg.set_end(end);
            }
        }

        self.inner.push(msg)?;
        if let Some(eos) = event_eos.take() {
            let event = Event::new(self.src_index, self.target_port, EventKind::Eos(eos));
            self.event_push.send(self.target_index, event)?;
        }
        Ok(())
    }

    fn flush(&mut self) -> Result<(), PushError> {
        self.inner.flush()
    }

    fn close(&mut self) -> Result<(), PushError> {
        self.inner.close()
    }
}

impl<T, PD, PE> AbortHandle for EventEosBatchPush<T, PD, PE>
where
    T: Data,
    PD: Push<MiniScopeBatch<T>> + AbortHandle,
    PE: Push<Event>,
{
    fn abort(&mut self, tag: Tag, worker: u16) -> Option<Tag> {
        let tag = self.inner.abort(tag, worker)?;
        if worker == self.target_index {
            Some(tag)
        } else {
            None
        }
    }
}

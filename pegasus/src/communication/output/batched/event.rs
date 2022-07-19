use crate::data::MicroBatch;
use crate::data_plane::DataPlanePush;
use crate::event::emitter::EventEmitter;

pub struct EventEosPush<T, P> {
    event_push: EventEmitter
    inner: DataPlanePush<MicroBatch<T>>,
}
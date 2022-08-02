use pegasus_common::tag::Tag;

use crate::data::Data;
use crate::eos::Eos;
use crate::error::PushError;

pub enum Pushed<T> {
    Finished,
    WouldBlock(Option<T>),
}

/// Non-blocking streaming data push;
/// A pusher which push streaming data into the underlying channel without blocking;
/// The underlying channel of the push should be unbounded or non-blocking;
pub trait StreamPush<T: Data> {
    fn push(&mut self, tag: &Tag, msg: T) -> Result<Pushed<T>, PushError>;

    fn push_last(&mut self, msg: T, end: Eos) -> Result<(), PushError>;

    fn push_iter<I: Iterator<Item = T>>(&mut self, tag: &Tag, iter: &mut I) -> Result<Pushed<T>, PushError>;

    fn notify_end(&mut self, end: Eos) -> Result<(), PushError>;

    fn flush(&mut self) -> Result<(), PushError>;

    fn close(&mut self) -> Result<(), PushError>;
}

pub trait Countable {
    fn count_pushed(&self, tag: &Tag) -> usize;
}

pub trait Pinnable {
    fn pin(&mut self, tag: &Tag) -> Result<bool, PushError>;

    fn unpin(&mut self) -> Result<(), PushError>;
}

// pub struct MultiScopeStreamPush<T, P> {
//     ch_index: u32,
//     worker_index: u32,
//     port: Port,
//     scope_level: u32,
//     buffer: BoundedBuffer<Item<T>>,
//     batches: Vec<RoBatch<Item<T>>>,
//     inner: P,
// }
//
// impl <T, P> StreamPush<T> for MultiScopeStreamPush<T, P> where P: Push<Package<T>> {
//     fn push(&mut self, tag: &Tag, msg: T) -> IOResult<Pushed<T>> {
//         match self.buffer.add(Item::data(tag.clone(), msg)) {
//             Ok(Some(batch)) => {
//                 self.inner.push(Package::new(self.worker_index, batch))?;
//                 Ok(Pushed::Finished)
//             }
//             Ok(None) => Ok(Pushed::Finished),
//             Err(mut msg) => Ok(Pushed::WouldBlock(Some(msg.take_data().unwrap()))),
//         }
//     }
//
//     fn push_last(&mut self, msg: T, end: Eos) -> IOResult<()> {
//
//     }
//
//     fn push_iter<I: Iterator<Item=T>>(&mut self, tag: &Tag, iter: &mut I) -> IOResult<Pushed<T>> {
//         todo!()
//     }
//
//     fn notify_end(&mut self, end: Eos) -> IOResult<()> {
//         todo!()
//     }
//
//     fn flush(&mut self) -> IOResult<()> {
//         todo!()
//     }
//
//     fn close(&mut self) -> IOResult<()> {
//         todo!()
//     }
// }

pub mod batching;
pub mod partition;

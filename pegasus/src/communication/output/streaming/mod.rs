use crate::communication::buffer::BoundedBuffer;
use crate::data::batching::RoBatch;
use crate::data::{Item, Package};
use crate::data_plane::Push;
use crate::errors::IOResult;
use crate::graph::Port;
use crate::progress::Eos;
use crate::{Data, Tag};

pub enum Pushed<T> {
    Finished,
    WouldBlock(Option<T>),
}

/// Non-blocking streaming data push;
/// A pusher which push streaming data into the underlying channel without blocking;
/// The underlying channel of the push should be unbounded or non-blocking;
pub trait StreamPush<T: Data> {
    fn push(&mut self, tag: &Tag, msg: T) -> IOResult<Pushed<T>>;

    fn push_last(&mut self, msg: T, end: Eos) -> IOResult<()>;

    fn push_iter<I: Iterator<Item = T>>(&mut self, tag: &Tag, iter: &mut I) -> IOResult<Pushed<T>>;

    fn notify_end(&mut self, end: Eos) -> IOResult<()>;

    fn flush(&mut self) -> IOResult<()>;

    fn close(&mut self) -> IOResult<()>;
}

pub trait Countable {
    fn count_pushed(&self, tag: &Tag) -> usize;
}

pub trait Pinnable {
    fn pin(&mut self, tag: &Tag) -> IOResult<bool>;

    fn unpin(&mut self) -> IOResult<()>;
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

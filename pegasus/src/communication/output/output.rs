use crate::communication::output::streaming::{Pushed, StreamPush};
use crate::communication::output::streaming::batching::BufStreamPush;
use crate::communication::output::streaming::partition::PartitionStreamPush;
use crate::errors::IOResult;
use crate::progress::Eos;
use crate::Tag;

pub enum EnumStreamPush<T> {
    Exchange(PartitionStreamPush<T, BufStreamPush<T, P>>)
}

impl<T> StreamPush<T> for EnumStreamPush<T> {
    fn push(&mut self, tag: &Tag, msg: T) -> IOResult<Pushed<T>> {
        todo!()
    }

    fn push_last(&mut self, msg: T, end: Eos) -> IOResult<()> {
        todo!()
    }

    fn push_iter<I: Iterator<Item = T>>(&mut self, tag: &Tag, iter: &mut I) -> IOResult<Pushed<T>> {
        todo!()
    }

    fn notify_end(&mut self, end: Eos) -> IOResult<()> {
        todo!()
    }

    fn flush(&mut self) -> IOResult<()> {
        todo!()
    }

    fn close(&mut self) -> IOResult<()> {
        todo!()
    }
}

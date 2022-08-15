use pegasus_common::tag::Tag;

use crate::data::Data;
use crate::eos::Eos;
use crate::error::PushError;
use crate::output::streaming::{Countable, Pinnable, Pushed, StreamPush};
use crate::output::Rectifier;
use crate::ChannelInfo;

pub trait PartitionRoute {
    type Item;

    fn partition_by(&self, item: &Self::Item) -> u64;
}

impl<T> PartitionRoute for Box<T>
where
    T: PartitionRoute + ?Sized,
{
    type Item = T::Item;

    fn partition_by(&self, item: &Self::Item) -> u64 {
        (**self).partition_by(item)
    }
}

struct FnPartitionRoute<F, D> {
    func: F,
    _ph: std::marker::PhantomData<D>,
}

unsafe impl<F, D> Send for FnPartitionRoute<F, D> where F: Send + 'static {}

impl<F, D> PartitionRoute for FnPartitionRoute<F, D>
where
    F: Fn(&D) -> u64,
{
    type Item = D;

    fn partition_by(&self, item: &Self::Item) -> u64 {
        (self.func)(item)
    }
}

impl<F, D> From<F> for Box<dyn PartitionRoute<Item = D> + Send + 'static>
where
    D: 'static,
    F: Fn(&D) -> u64 + Send + 'static,
{
    fn from(func: F) -> Self {
        let fr = FnPartitionRoute { func, _ph: std::marker::PhantomData };
        Box::new(fr)
    }
}

struct Partitioner<D> {
    rectifier: Rectifier,
    router: Box<dyn PartitionRoute<Item = D> + Send + 'static>,
}

impl<D> Partitioner<D> {
    fn new<P>(len: usize, router: P) -> Self
    where
        P: PartitionRoute<Item = D> + Send + 'static,
    {
        let rectifier = Rectifier::new(len);
        Partitioner { rectifier, router: Box::new(router) }
    }

    #[inline]
    fn get_partition(&self, item: &D) -> usize {
        let par_key = self.router.partition_by(item);
        self.rectifier.get(par_key)
    }
}

pub struct PartitionStreamPush<T, P> {
    ch_info: ChannelInfo,
    pub worker_index: u16,
    pushes: Vec<P>,
    route: Partitioner<T>,
}

impl<T, P> PartitionStreamPush<T, P> {
    pub fn new<PR>(ch_info: ChannelInfo, worker_index: u16, router: PR, pushes: Vec<P>) -> Self
    where
        PR: PartitionRoute<Item = T> + Send + 'static,
    {
        let route = Partitioner::new(pushes.len(), router);
        Self { ch_info, worker_index, pushes, route }
    }
}

impl<T, P> Pinnable for PartitionStreamPush<T, P>
where
    T: Data,
    P: StreamPush<T> + Pinnable,
{
    fn pin(&mut self, tag: &Tag) -> Result<bool, PushError> {
        for p in self.pushes.iter_mut() {
            if !p.pin(tag)? {
                return Ok(false);
            }
        }
        Ok(true)
    }

    fn unpin(&mut self) -> Result<(), PushError> {
        for p in self.pushes.iter_mut() {
            p.unpin()?;
        }
        Ok(())
    }
}

impl<T, P> StreamPush<T> for PartitionStreamPush<T, P>
where
    T: Data,
    P: StreamPush<T> + Countable + Pinnable,
{
    fn push(&mut self, tag: &Tag, msg: T) -> Result<Pushed<T>, PushError> {
        assert_eq!(tag.len(), self.ch_info.scope_level as usize);
        let target = self.route.get_partition(&msg);
        self.pushes[target].push(tag, msg)
    }

    fn push_last(&mut self, msg: T, mut end: Eos) -> Result<(), PushError> {
        assert_eq!(end.tag.len(), self.ch_info.scope_level as usize);
        assert_eq!(end.child_peers().len(), 0);
        end.total_send = 0;
        end.global_total_send = 0;

        let target = self.route.get_partition(&msg);
        for (i, p) in self.pushes.iter().enumerate() {
            let count = p.count_pushed(&end.tag);
            end.add_child_send(i as u16, count);
        }

        for (i, p) in self.pushes.iter_mut().enumerate() {
            if i != target {
                p.notify_end(end.clone())?;
            }
        }
        self.pushes[target].push_last(msg, end)
    }

    fn push_iter<I: Iterator<Item = T>>(
        &mut self, tag: &Tag, iter: &mut I,
    ) -> Result<Pushed<T>, PushError> {
        assert_eq!(tag.len(), self.ch_info.scope_level as usize);

        for p in self.pushes.iter_mut() {
            if !p.pin(tag)? {
                return Ok(Pushed::WouldBlock(None));
            }
        }

        while let Some(item) = iter.next() {
            let target = self.route.get_partition(&item);
            if let Pushed::WouldBlock(v) = self.pushes[target].push(tag, item)? {
                return Ok(Pushed::WouldBlock(v));
            }
        }
        Ok(Pushed::Finished)
    }

    fn notify_end(&mut self, mut eos: Eos) -> Result<(), PushError> {
        assert_eq!(eos.tag.len(), self.ch_info.scope_level as usize);
        assert_eq!(eos.child_peers().len(), 0);

        eos.total_send = 0;
        eos.global_total_send = 0;

        for (i, p) in self.pushes.iter().enumerate() {
            let count = p.count_pushed(&eos.tag);
            eos.add_child_send(i as u16, count);
        }

        for i in 1..self.pushes.len() {
            self.pushes[i].notify_end(eos.clone())?;
        }
        self.pushes[0].notify_end(eos)
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

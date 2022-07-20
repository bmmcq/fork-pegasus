use ahash::AHashMap;

use crate::api::function::{FnResult, RouteFunction};
use crate::channel_id::ChannelInfo;
use crate::communication::buffer::{BufferPtr, ScopeBuffer};
use crate::communication::output::batched::Rectifier;
use crate::communication::output::streaming::batching::{BufStreamPush, MultiScopeBufStreamPush};
use crate::communication::output::streaming::{Countable, Pinnable, Pushed, StreamPush};
use crate::communication::IOResult;
use crate::data::batching::{RoBatch, WoBatch};
use crate::data::MicroBatch;
use crate::data_plane::intra_thread::ThreadPush;
use crate::data_plane::Push;
use crate::errors::IOError;
use crate::graph::Port;
use crate::progress::Eos;
use crate::{Data, Tag};

pub struct PartitionStreamPush<T, P> {
    pub src: u32,
    pub port: Port,
    scope_level: u8,
    index: u32,
    pushes: Vec<P>,
    route: Partitioner<T>,
}

impl<T, P> Pinnable for PartitionStreamPush<T, P>
where
    T: Data,
    P: StreamPush<T> + Pinnable,
{
    fn pin(&mut self, tag: &Tag) -> IOResult<bool> {
        for p in self.pushes.iter_mut() {
            if !p.pin(tag) {
                return Ok(false);
            }
        }
        Ok(true)
    }

    fn unpin(&mut self) -> IOResult<()> {
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
    fn push(&mut self, tag: &Tag, msg: T) -> IOResult<Pushed<T>> {
        assert_eq!(tag.len(), self.scope_level as usize);
        let target = self.route.get_partition(&msg)?;
        self.pushes[target].push(tag, msg)
    }

    fn push_last(&mut self, msg: T, mut end: Eos) -> IOResult<()> {
        assert_eq!(end.tag.len(), self.scope_level as usize);
        assert_eq!(end.child_peers().len(), 0);
        end.total_send = 0;
        end.global_total_send = 0;

        let target = self.route.get_partition(&msg)?;
        for (i, p) in self.pushes.iter().enumerate() {
            let count = p.count_pushed();
            end.add_child_send(i as u32, count);
        }

        for (i, p) in self.pushes.iter_mut().enumerate() {
            if i != target {
                p.notify_end(end.clone());
            }
        }
        self.pushes[target].push_last(msg, end)
    }

    fn push_iter<I: Iterator<Item = T>>(&mut self, tag: &Tag, iter: &mut I) -> IOResult<Pushed<T>> {
        assert_eq!(tag.len(), self.scope_level as usize);

        for p in self.pushes.iter_mut() {
            if !p.pin(tag) {
                return Ok(Pushed::WouldBlock(None));
            }
        }

        while let Some(item) = iter.next() {
            let target = self.route.get_partition(&item)?;
            if let Some(item) = self.pushes[target].push(tag, item)? {
                return Ok(Pushed::WouldBlock(Some(item)));
            }
        }
        Ok(Pushed::Finished)
    }

    fn notify_end(&mut self, mut eos: Eos) -> IOResult<()> {
        assert_eq!(eos.tag.len(), self.scope_level);
        assert_eq!(eos.child_peers().len(), 0);

        eos.total_send = 0;
        eos.global_total_send = 0;

        for (i, p) in self.pushes.iter().enumerate() {
            let count = p.count_pushed();
            eos.add_child_send(i as u32, count);
        }

        for i in 1..self.pushes.len() {
            self.pushes[i].notify_end(eos.clone())?;
        }
        self.pushes[0].notify_end(eos)
    }

    fn flush(&mut self) -> IOResult<()> {
        for p in self.pushes.iter_mut() {
            p.flush()?;
        }
        Ok(())
    }

    fn close(&mut self) -> IOResult<()> {
        for p in self.pushes.iter_mut() {
            p.close()?;
        }
        Ok(())
    }
}

struct Partitioner<D> {
    rectifier: Rectifier,
    router: Box<dyn RouteFunction<D>>,
}

impl<D> Partitioner<D> {
    fn new(len: usize, router: Box<dyn RouteFunction<D>>) -> Self {
        let rectifier = Rectifier::new(len);
        Partitioner { rectifier, router }
    }

    #[inline]
    fn get_partition(&self, item: &D) -> FnResult<usize> {
        let par_key = self.router.route(item)?;
        Ok(self.rectifier.get(par_key))
    }
}

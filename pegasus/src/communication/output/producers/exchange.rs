use ahash::AHashMap;
use crate::api::function::{FnResult, RouteFunction};
use crate::channel_id::ChannelInfo;
use crate::communication::buffer::{BufferPtr, ScopeBuffer};
use crate::communication::decorator::evented::EventEmitPush;
use crate::communication::decorator::ScopeStreamPush;
use crate::communication::IOResult;
use crate::communication::output::producers::{BatchProducer, MultiScopeBatchProducer};
use crate::data::batching::{RoBatch, WoBatch};
use crate::data::MicroBatch;
use crate::data_plane::intra_thread::ThreadPush;
use crate::errors::IOError;
use crate::graph::Port;
use crate::progress::Eos;
use crate::Tag;

pub struct ExchangeProducer<T> {
    pub src: u32,
    pub port: Port,
    index: u32,
    pushes: Vec<BatchProducer<T, EventEmitPush<T>>>,
    route: Exchange<T>,
}

impl<T> ScopeStreamPush<T> for ExchangeProducer<T> {
    fn port(&self) -> Port {
        todo!()
    }

    fn push(&mut self, tag: &Tag, msg: T) -> IOResult<Option<T>> {
        assert!(tag.is_root());
        let target = self.route.route(&msg)?;
        self.pushes[target].push(tag, msg)
    }

    fn push_last(&mut self, msg: T, mut end: Eos) -> IOResult<()> {
        assert!(end.tag.is_root());
        assert_eq!(end.child_peers().len(), 0);
        end.total_send = 0;
        end.global_total_send = 0;
        let target = self.route.route(&msg)?;
        for (i, p) in self.pushes.iter().enumerate() {
            let count = p.count_send();
            end.add_child_send(i as u32, count);
        }
        for (i, p) in self.pushes.iter_mut().enumerate() {
            if i != target {
                p.notify_end(end.clone());
            }
        }
        self.pushes[target].push_last(msg, end)
    }

    fn try_push_iter<I: Iterator<Item=T>>(&mut self, tag: &Tag, iter: &mut I) -> IOResult<()> {
        assert!(tag.is_root());

        while let Some(item) = iter.next() {
            let target = self.route.route(&item)?;
            if let Some(item) = self.pushes[target].push(tag, item)? {
                let mut b = WoBatch::new(1);
                b.push(item);
                self.pushes[target].push_batch(tag.clone(), b.finalize())?;
                return Err(IOError::would_block());
            }
        }
        Ok(())
    }

    fn notify_end(&mut self, mut eos: Eos) -> IOResult<()> {
        assert!(eos.tag.is_root());
        assert_eq!(eos.child_peers().len(), 0);
        eos.total_send = 0;
        eos.global_total_send = 0;

        for (i, p) in self.pushes.iter().enumerate() {
            let count = p.count_send();
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

pub struct MultiScopeExchangeProducer<T> {
    pub src: u32,
    pub port: Port,
    pub scope_level: usize,
    index: u32,
    pushes: Vec<MultiScopeBatchProducer<T, EventEmitPush<T>>>,
    route: Exchange<T>,
}

impl <T> ScopeStreamPush<T> for MultiScopeExchangeProducer<T> {
    fn port(&self) -> Port {
        todo!()
    }

    fn push(&mut self, tag: &Tag, msg: T) -> IOResult<Option<T>> {
        assert_eq!(tag.len(), self.scope_level);
        let target = self.route.route(&msg)?;
        self.pushes[target].push(tag, msg)
    }

    fn push_last(&mut self, msg: T, mut eos: Eos) -> IOResult<()> {
        assert_eq!(eos.tag.len(), self.scope_level);
        assert_eq!(eos.child_peers().len(), 0);

        let target = self.route.route(&msg)?;
        for (i, p) in self.pushes.iter().enumerate() {
            let count = p.count_send(&eos.tag);
            eos.add_child_send(i as u32, count);
        }

        for (i, p) in self.pushes.iter_mut().enumerate() {
            if i != target {
                p.notify_end(eos.clone())?;
            }
        }
        self.pushes[target].push_last(msg, eos)
    }

    fn try_push_iter<I: Iterator<Item=T>>(&mut self, tag: &Tag, iter: &mut I) -> IOResult<()> {
        for p in self.pushes.iter_mut() {
            if !p.pin(tag)? {
                return Err(IOError::would_block());
            }
        }

        while let Some(next) = iter.next() {
            let target = self.route.route(&next)?;
            if let Some(item) = self.pushes[target].push(tag, next)? {
                let mut b = WoBatch::new(1);
                b.push(item);
                self.pushes[target].push_batch(tag.clone(), b.finalize())?;
                return Err(IOError::would_block());
            }
        }

        Ok(())
    }

    fn notify_end(&mut self, mut eos: Eos) -> IOResult<()> {
        assert_eq!(eos.tag.len(), self.scope_level);
        assert_eq!(eos.child_peers().len(), 0);

        for (i, p) in self.pushes.iter().enumerate() {
            let count = p.count_send(&eos.tag);
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

struct Exchange<D> {
    rectifier: Rectifier,
    router: Box<dyn RouteFunction<D>>,
}

impl<D> Exchange<D> {
    fn new(len: usize, router: Box<dyn RouteFunction<D>>) -> Self {
        let rectifier = Rectifier::new(len);
        Exchange { rectifier, router }
    }

    #[inline]
    fn route(&self, item: &D) -> FnResult<usize> {
        let par_key = self.router.route(item)?;
        Ok(self.rectifier.get(par_key))
    }
}

enum Rectifier {
    And(u64),
    Mod(u64),
}

impl Rectifier {
    fn new(length: usize) -> Self {
        if length & (length - 1) == 0 {
            Rectifier::And(length as u64 - 1)
        } else {
            Rectifier::Mod(length as u64)
        }
    }

    #[inline]
    fn get(&self, v: u64) -> usize {
        let r = match self {
            Rectifier::And(b) => v & *b,
            Rectifier::Mod(b) => v % *b,
        };
        r as usize
    }
}



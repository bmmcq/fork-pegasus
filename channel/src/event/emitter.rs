use std::cell::RefCell;
use std::collections::VecDeque;

use pegasus_common::rc::UnsafeRcPtr;

use super::Event;
use crate::error::IOResult;
use crate::{IOError, Pull, Push};

#[derive(Clone)]
pub struct EventEmitter<P> {
    tx: UnsafeRcPtr<RefCell<Vec<P>>>,
}

impl<P> EventEmitter<P> {
    pub fn new(tx: Vec<P>) -> Self {
        EventEmitter { tx: UnsafeRcPtr::new(RefCell::new(tx)) }
    }

    pub fn peers(&self) -> usize {
        self.tx.borrow().len()
    }
}

impl<P> EventEmitter<P>
where
    P: Push<Event>,
{
    pub fn send(&mut self, target: u16, event: Event) -> IOResult<()> {
        let offset = target as usize;
        let mut borrow = self.tx.borrow_mut();
        trace!("EventBus: send {:?} to {} port {:?};", event.kind, target, event.target_port);
        borrow[offset].push(event)
    }

    pub fn broadcast(&mut self, event: Event) -> IOResult<()> {
        trace!("EventBus: broadcast {:?} to port {:?}", event.kind, event.target_port);
        let mut borrow = self.tx.borrow_mut();
        for i in 1..borrow.len() {
            borrow[i].push(event.clone())?;
        }
        borrow[0].push(event)?;
        Ok(())
    }

    pub fn flush(&mut self) -> IOResult<()> {
        let mut borrow = self.tx.borrow_mut();
        for p in borrow.iter_mut() {
            p.flush()?;
        }
        Ok(())
    }

    pub fn close(&mut self) -> IOResult<()> {
        let mut borrow = self.tx.borrow_mut();
        for p in borrow.iter_mut() {
            p.close()?;
        }
        Ok(())
    }
}

pub struct EventCollector<P> {
    rx: P,
    received: VecDeque<Event>,
}

impl<P> EventCollector<P> {
    pub fn new(rx: P) -> Self {
        EventCollector { rx, received: VecDeque::new() }
    }
}

impl<P> EventCollector<P>
where
    P: Pull<Event>,
{
    pub fn collect(&mut self) -> Result<bool, IOError> {
        while let Some(event) = self.rx.pull_next()? {
            self.received.push_back(event);
        }
        Ok(!self.received.is_empty())
    }

    #[allow(dead_code)]
    #[inline]
    pub fn has_updates(&self) -> bool {
        !self.received.is_empty()
    }

    pub fn get_updates(&mut self) -> &mut VecDeque<Event> {
        &mut self.received
    }
}
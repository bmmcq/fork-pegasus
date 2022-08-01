//
//! Copyright 2020 Alibaba Group Holding Limited.
//!
//! Licensed under the Apache License, Version 2.0 (the "License");
//! you may not use this file except in compliance with the License.
//! You may obtain a copy of the License at
//!
//! http://www.apache.org/licenses/LICENSE-2.0
//!
//! Unless required by applicable law or agreed to in writing, software
//! distributed under the License is distributed on an "AS IS" BASIS,
//! WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
//! See the License for the specific language governing permissions and
//! limitations under the License.

use std::sync::atomic::{AtomicBool, Ordering};
use std::sync::Arc;

use crossbeam_channel::{Receiver, SendError, Sender, TryRecvError};
use rand::Rng;

#[derive(Debug, Copy, Clone, PartialEq, Eq)]
pub enum RecvError {
    Eof,
    UnexpectedEof,
}

impl RecvError {
    pub fn is_eof(&self) -> bool {
        matches!(self, RecvError::Eof)
    }
}

pub struct MessageSender<T> {
    id: u64,
    inner: Option<Sender<T>>,
    poisoned: Arc<AtomicBool>,
    is_closed: bool,
}

pub struct MessageReceiver<T> {
    inner: Receiver<T>,
    sender_poisoned: Arc<AtomicBool>,
}

impl<T> MessageSender<T> {
    fn new(tx: Sender<T>, state: &Arc<AtomicBool>) -> Self {
        let id: u64 = rand::thread_rng().gen();
        trace!("create sender with id {}", id);
        MessageSender { id, inner: Some(tx), poisoned: state.clone(), is_closed: false }
    }

    fn poison(&self) {
        self.poisoned.store(true, Ordering::SeqCst);
    }
}

impl<T> Clone for MessageSender<T> {
    fn clone(&self) -> Self {
        MessageSender {
            id: self.id,
            inner: self.inner.clone(),
            poisoned: self.poisoned.clone(),
            is_closed: false,
        }
    }
}

impl<T> Drop for MessageSender<T> {
    fn drop(&mut self) {
        if !self.is_closed {
            warn!("dropping an unclosed 'MessageSender' id = {}", self.id);
            self.poison();
        }
    }
}

impl<T> MessageReceiver<T> {
    fn new(rx: Receiver<T>, sender_poisoned: &Arc<AtomicBool>) -> Self {
        MessageReceiver { inner: rx, sender_poisoned: sender_poisoned.clone() }
    }
}

pub fn unbound<T: Send>() -> (MessageSender<T>, MessageReceiver<T>) {
    let (tx, rx) = crossbeam_channel::unbounded::<T>();
    let poisoned = Arc::new(AtomicBool::new(false));
    (MessageSender::new(tx, &poisoned), MessageReceiver::new(rx, &poisoned))
}

impl<T> MessageSender<T> {
    pub fn send(&self, message: T) -> Result<(), T> {
        if let Some(sender) = self.inner.as_ref() {
            sender
                .send(message)
                .map_err(|SendError(err)| err)
        } else {
            Err(message)
        }
    }

    pub fn close(&mut self) {
        if !self.is_closed {
            self.is_closed = true;
            self.inner.take();
        }
    }
}

impl<T> MessageReceiver<T> {
    pub fn recv(&self) -> Result<T, RecvError> {
        match self.inner.recv() {
            Ok(d) => Ok(d),
            Err(_err) => {
                if self.sender_poisoned.load(Ordering::SeqCst) {
                    Err(RecvError::UnexpectedEof)
                } else {
                    Err(RecvError::Eof)
                }
            }
        }
    }

    pub fn try_recv(&self) -> Result<Option<T>, RecvError> {
        match self.inner.try_recv() {
            Ok(d) => Ok(Some(d)),
            Err(TryRecvError::Empty) => {
                if self.sender_poisoned.load(Ordering::SeqCst) {
                    Err(RecvError::UnexpectedEof)
                } else {
                    Ok(None)
                }
            }
            Err(TryRecvError::Disconnected) => {
                if self.sender_poisoned.load(Ordering::SeqCst) {
                    Err(RecvError::UnexpectedEof)
                } else {
                    Err(RecvError::Eof)
                }
            }
        }
    }
}

#[cfg(test)]
mod test {
    use super::*;

    #[test]
    fn message_channel() {
        let (mut tx, rx) = unbound();
        let mut tx_1 = tx.clone();
        tx.send(1).unwrap();
        tx.send(1).unwrap();
        tx_1.send(1).unwrap();
        tx_1.send(1).unwrap();
        let mut count = 0;
        while let Ok(Some(m)) = rx.try_recv() {
            count += 1;
            assert_eq!(1, m);
        }
        assert_eq!(4, count);
        let mut tx_2 = tx.clone();
        tx.close();
        tx_1.close();
        tx_2.send(2).unwrap();
        assert_eq!(2, rx.recv().unwrap());
        tx_2.send(3).unwrap();
        assert_eq!(3, rx.recv().unwrap());
        tx_2.close();

        match rx.recv() {
            Err(err) => {
                assert!(err.is_eof());
            }
            _ => panic!("broken pipe not detected"),
        }
    }

    #[test]
    fn message_channel_error() {
        let (tx, rx) = unbound();
        let mut tx_1 = tx.clone();
        tx.send(0).unwrap();
        tx_1.poison();
        tx_1.close();
        loop {
            match rx.try_recv() {
                Ok(Some(a)) => assert_eq!(a, 0),
                Err(err) => {
                    assert!(!err.is_eof());
                    break;
                }
                _ => (),
            }
        }
    }

    #[test]
    fn message_channel_error_drop() {
        let (tx, rx) = unbound();
        let mut tx_1 = tx.clone();
        tx.send(0).unwrap();
        std::mem::drop(tx);
        tx_1.send(0).unwrap();
        tx_1.close();

        loop {
            match rx.try_recv() {
                Ok(Some(v)) => assert_eq!(v, 0),
                Err(err) => {
                    // because one sender didn't close ；
                    assert!(!err.is_eof());
                    break;
                }
                _ => (),
            }
        }
    }
}

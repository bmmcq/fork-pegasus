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

use std::cell::RefCell;
use std::collections::VecDeque;

use pegasus_common::rc::UnsafeRcPtr;

use crate::channel_id::ChannelId;
use crate::data_plane::{Pull, Push};
use crate::errors::{IOError, IOErrorKind};

pub struct ThreadPush<T> {
    pub id: ChannelId,
    queue: UnsafeRcPtr<RefCell<VecDeque<T>>>,
    is_closed: UnsafeRcPtr<RefCell<bool>>,
}

impl<T> ThreadPush<T> {
    fn new(
        id: ChannelId, queue: UnsafeRcPtr<RefCell<VecDeque<T>>>, is_closed: UnsafeRcPtr<RefCell<bool>>,
    ) -> Self {
        ThreadPush { id, queue, is_closed }
    }
}

// impl<T> Clone for ThreadPush<T> {
//     fn clone(&self) -> Self {
//         ThreadPush {
//             id: self.id,
//             queue: self.queue.clone(),
//             is_closed: self.is_closed.clone(),
//         }
//     }
// }

impl<T: Send> Push<T> for ThreadPush<T> {
    #[inline]
    fn push(&mut self, msg: T) -> Result<(), IOError> {
        if !*self.is_closed.borrow() {
            self.queue.borrow_mut().push_back(msg);
            Ok(())
        } else {
            error_worker!("ThreadPush#push after close;");
            let mut error = IOError::new(IOErrorKind::SendAfterClose);
            error.set_ch_id(self.id);
            Err(error)
        }
    }

    #[inline]
    fn close(&mut self) -> Result<(), IOError> {
        *self.is_closed.borrow_mut() = true;
        Ok(())
    }
}

pub struct ThreadPull<T> {
    pub id: ChannelId,
    queue: UnsafeRcPtr<RefCell<VecDeque<T>>>,
    is_closed: UnsafeRcPtr<RefCell<bool>>,
}

impl<T> ThreadPull<T> {
    fn new(
        id: ChannelId, queue: UnsafeRcPtr<RefCell<VecDeque<T>>>, is_closed: UnsafeRcPtr<RefCell<bool>>,
    ) -> Self {
        ThreadPull { id, queue, is_closed }
    }
}

impl<T: Send> Pull<T> for ThreadPull<T> {
    fn pull_next(&mut self) -> Result<Option<T>, IOError> {
        match self.queue.borrow_mut().pop_front() {
            Some(t) => Ok(Some(t)),
            None => {
                if *self.is_closed.borrow() {
                    // is closed;
                    Err(IOError::eof())
                } else if self.queue.strong_count() == 1 {
                    Err(IOErrorKind::UnexpectedEof)?
                } else {
                    Ok(None)
                }
            }
        }
    }

    fn has_next(&mut self) -> Result<bool, IOError> {
        Ok(!self.queue.borrow().is_empty())
    }
}

pub fn pipeline<T>(id: ChannelId) -> (ThreadPush<T>, ThreadPull<T>) {
    let queue = UnsafeRcPtr::new(RefCell::new(VecDeque::new()));
    let is_closed = UnsafeRcPtr::new(RefCell::new(false));
    (ThreadPush::new(id, queue.clone(), is_closed.clone()), ThreadPull::new(id, queue, is_closed))
}

#[cfg(test)]
mod test {
    use super::*;

    #[test]
    fn thread_push_pull() {
        let (mut tx, mut rx) = pipeline::<u64>(ChannelId::new(0, 0));
        for i in 0..65535 {
            tx.push(i).unwrap();
        }

        let mut j = 0;
        while let Some(i) = rx.pull_next().unwrap() {
            assert_eq!(i, j);
            j += 1;
        }
        assert_eq!(j, 65535);
        tx.close().unwrap();
        let result = rx.pull_next();
        match result {
            Err(err) => {
                assert!(err.is_eof(), "unexpected error {:?}", err);
            }
            Ok(_) => {
                panic!("undetected error");
            }
        }
    }
}

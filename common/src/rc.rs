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

use std::ops::Deref;
use std::rc::Rc;

/// This is a highly unsafe reference-count pointer;
///
/// # Safety
///
/// This is highly unsafe:
///
/// * It is much like the standard rc pointer [`Rc`], with a extra feature that it can be send through
/// threads, this may brings huge security problems, because this pointer doesn't provide any [`Sync`]
/// promise;
///
/// * It can be only used in the situation where you are very confident that this pointer and all its
/// copies will be owned by only one thread at a time, otherwise use [`Arc`] instead;
///
///
/// If you have a structure that is not thread-safe, it won't be read or write by multi-threads
/// at same time under any circumstances, and you need a reference count pointer to it to make it
/// accessible at many places.
///
/// At last, this structure may be owned by different threads at different time;
///
/// The [`Rc`] can't be [`Send`], and the [`Arc`] pointer need the structure it points to be [`Sync`],
/// although a mutex lock can be used to make the structure [`Sync`], but it may incurs performance
/// degradation, and a lock is not suitable for this situation indeed. With that in mind, this pointer
/// may be a good choice;
///
/// [`Rc`]: https://doc.rust-lang.org/std/rc/struct.Rc.html
/// [`Send`]: https://doc.rust-lang.org/std/marker/trait.Send.html
/// [`Sync`]: https://doc.rust-lang.org/std/marker/trait.Sync.html
/// [`Arc`]: https://doc.rust-lang.org/std/sync/struct.Arc.html
///

pub struct UnsafeRcPtr<T: ?Sized> {
    ptr: Rc<T>,
}

unsafe impl<T: ?Sized + Send> Send for UnsafeRcPtr<T> {}

impl<T: ?Sized> Clone for UnsafeRcPtr<T> {
    fn clone(&self) -> Self {
        UnsafeRcPtr { ptr: self.ptr.clone() }
    }
}

impl<T: ?Sized> Deref for UnsafeRcPtr<T> {
    type Target = T;

    fn deref(&self) -> &Self::Target {
        &*self.ptr
    }
}

impl<T: Sized> UnsafeRcPtr<T> {
    pub fn new(entry: T) -> Self {
        UnsafeRcPtr { ptr: Rc::new(entry) }
    }

    pub fn try_unwrap(this: Self) -> Result<T, Self> {
        Rc::try_unwrap(this.ptr).map_err(|ptr| UnsafeRcPtr { ptr })
    }

    pub fn strong_count(&self) -> usize {
        Rc::strong_count(&self.ptr)
    }
}

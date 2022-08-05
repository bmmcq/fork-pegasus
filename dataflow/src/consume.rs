// use std::cell::RefMut;
// use pegasus_channel::base::BasePull;
// use pegasus_channel::data::{Data, MiniScopeBatch};
// use pegasus_channel::error::{IOError, IOErrorKind};
// use pegasus_channel::input::handle::{InputHandle, PopEntry};
// use crate::error::JobExecError;
//
// pub struct BatchInput<'a, T: Data> {
//     inner: RefMut<'a, InputHandle<T, BasePull<MiniScopeBatch<T>>>>
// }
//
// impl <'a, T> BatchInput<'a, T> where T: Data {
//     pub fn for_each_batch<F>(&mut self, mut func: F) -> Result<bool, JobExecError>
//         where F: FnMut(&mut MiniScopeBatch<T>) -> Result<(), JobExecError>
//     {
//         loop {
//             match self.inner.pop() {
//                 Ok(PopEntry::Ready(mut batch)) => {
//                     func(&mut batch)?;
//                 },
//                 Ok(PopEntry::NotReady) => {
//                     return Ok(false);
//                 }
//                 Ok(PopEntry::End) => {
//                     return Ok(true);
//                 }
//                 Err(source) => {
//                     return Err(IOError::new(IOErrorKind::PullErr { source }))?;
//                 }
//             }
//         }
//     }
// }
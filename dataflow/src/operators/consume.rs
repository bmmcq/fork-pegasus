use pegasus_channel::block::BlockGuard;
use pegasus_channel::data::{Data, MiniScopeBatch};
use pegasus_channel::input::handle::{MiniScopeBatchQueue, PopEntry};
use pegasus_common::tag::Tag;

use crate::errors::JobExecError;

pub struct MiniScopeBatchStream<'a, T> {
    inner: &'a mut MiniScopeBatchQueue<T>,
}

impl<'a, T> MiniScopeBatchStream<'a, T>
where
    T: Data,
{
    pub fn new(inner: &'a mut MiniScopeBatchQueue<T>) -> Self {
        Self { inner }
    }

    pub fn tag(&self) -> &Tag {
        self.inner.tag()
    }

    pub fn abort(&mut self) {
        self.inner.abort()
    }

    pub fn block(&mut self, guard: BlockGuard) {
        self.inner.block(guard);
    }

    pub fn for_each_batch<F>(&'a mut self, mut func: F) -> Result<(), JobExecError>
    where
        F: FnMut(&mut MiniScopeBatch<T>) -> Result<(), JobExecError>,
    {
        loop {
            match self.inner.front() {
                PopEntry::End | PopEntry::NotReady => break,
                PopEntry::Ready(batch) => {
                    let is_last = batch.is_last();
                    if let Err(error) = func(batch) {
                        match error {
                            JobExecError::Inner { mut source } => {
                                if let Some(b) = source.check_data_block() {
                                    self.inner.block(b);
                                    break;
                                } else {
                                    return Err(JobExecError::Inner { source });
                                }
                            }
                            JobExecError::UserError { source } => {
                                return Err(JobExecError::UserError { source });
                            }
                        }
                    }

                    if is_last {
                        break;
                    } else {
                        // next;
                    }
                }
            }

            let _discard = self.inner.pop();
        }
        Ok(())
    }
}

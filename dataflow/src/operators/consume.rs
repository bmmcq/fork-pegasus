use pegasus_channel::data::{Data, MiniScopeBatch};
use pegasus_channel::input::handle::{MiniScopeBatchStream, PopEntry};
use crate::error::JobExecError;

pub struct MiniScopeBatchConsume<'a, T> {
    inner: &'a mut MiniScopeBatchStream<T>,
}

impl <'a, T> MiniScopeBatchConsume<'a, T> where T: Data {
    pub fn for_each_batch<F>(&'a mut self, mut func: F) -> Result<(), JobExecError>
        where F: FnMut(&mut MiniScopeBatch<T>) -> Result<(), JobExecError>
    {
        loop {
            match self.inner.front() {
                PopEntry::End | PopEntry::NotReady => break,
                PopEntry::Ready(batch) => {
                    if let Err(error) = func(batch) {
                        match error {
                            JobExecError::Inner { mut source } => {
                                if let Some(b) = source.check_data_block() {
                                    self.inner.block(b);
                                } else if let Some(tag) = source.check_data_abort() {
                                    assert_eq!(&tag, self.inner.tag());
                                    self.inner.abort();
                                } else {
                                    return Err(JobExecError::Inner { source });
                                }
                            }
                            JobExecError::UserError { source } => {
                                return Err(JobExecError::UserError { source });
                            }
                        }
                    }
                    let _discard = self.inner.pull();
                }
            }
        }
        Ok(())
    }
}


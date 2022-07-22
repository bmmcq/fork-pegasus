use pegasus_common::tag::Tag;
use crate::block::BlockGuard;
use crate::{IOError, Pull};
use crate::data::MiniScopeBatch;
use crate::error::IOResult;

pub enum PopEntry<T> {
    EOF,
    NotReady,
    Ready(MiniScopeBatch<T>),
}

pub struct InputHandle<T, P> {
    is_exhaust : bool,
    tag: Tag,
    cache: Option<MiniScopeBatch<T>>,
    block: Option<BlockGuard>,
    input: P,
}

impl <T, P> InputHandle<T, P> where P: Pull<MiniScopeBatch<T>> {

    pub fn pop(&mut self) -> Result<PopEntry<T>, IOError> {
        if self.is_exhaust {
            return Ok(PopEntry::EOF);
        }

        if let Some(block) = self.block.take() {
            if block.is_blocked() {
                self.block = Some(block);
                return Ok(PopEntry::NotReady);
            }
        }

        if let Some(item) = self.cache.take() {
            assert_eq!(item.tag, self.tag);
            return Ok(PopEntry::Ready(item));
        }

        match self.input.pull_next() {
            Ok(Some(b)) => {
                assert_eq!(b.tag, self.tag);
                Ok(PopEntry::Ready(b))
            },
            Ok(None) => Ok(PopEntry::NotReady),
            Err(e) => {
                if e.is_eof() {
                    self.is_exhaust = true;
                    Ok(PopEntry::EOF)
                } else {
                    Err(e)
                }
            }
        }
    }

    pub fn check_ready(&mut self) -> IOResult<bool> {
        if self.cache.is_some() {
            return Ok(true);
        }

        if let Some(item) = self.input.pull_next()? {
            self.cache = Some(item);
            Ok(true)
        } else {
            Ok(false)
        }
    }

    pub fn block(&mut self, guard: BlockGuard) {
        assert_eq!(guard.tag(), &self.tag);
        self.block = Some(guard);
    }

    pub fn is_block(&self, tag: &Tag) -> bool {
        assert_eq!(tag, &self.tag);
        self.block.as_ref().map(|b| b.is_blocked()).unwrap_or(false)
    }
}


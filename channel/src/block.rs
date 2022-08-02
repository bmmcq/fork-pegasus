use std::sync::{Arc, Weak};

use pegasus_common::tag::Tag;

use crate::error::PushError;

pub enum BlockKind<D> {
    None,
    One(D),
    Iter(Option<D>, Box<dyn Iterator<Item = D> + Send + 'static>),
}

pub struct BlockGuard {
    tag: Tag,
    guard: Weak<()>,
}

impl BlockGuard {
    pub fn tag(&self) -> &Tag {
        &self.tag
    }

    pub fn is_blocked(&self) -> bool {
        self.guard.upgrade().is_some()
    }

    pub fn take(self) -> (Tag, Weak<()>) {
        (self.tag, self.guard)
    }
}

impl From<(Tag, Weak<()>)> for BlockGuard {
    fn from(raw: (Tag, Weak<()>)) -> Self {
        Self { tag: raw.0, guard: raw.1 }
    }
}

pub struct BlockEntry<D> {
    tag: Tag,
    kind: BlockKind<D>,
    hook: Arc<()>,
}

impl<D> BlockEntry<D> {
    pub fn one(tag: Tag, msg: D) -> Self {
        Self { tag, kind: BlockKind::One(msg), hook: Arc::new(()) }
    }

    pub fn iter<I>(tag: Tag, head: Option<D>, iter: I) -> Self
    where
        I: Iterator<Item = D> + Send + 'static,
    {
        Self { tag, kind: BlockKind::Iter(head, Box::new(iter)), hook: Arc::new(()) }
    }

    pub fn take_block(&mut self) -> BlockKind<D> {
        std::mem::replace(&mut self.kind, BlockKind::None)
    }

    pub fn re_block(&mut self, msg: D) {
        self.kind = BlockKind::One(msg)
    }

    pub fn re_block_iter(&mut self, head: Option<D>, iter: Box<dyn Iterator<Item = D> + Send + 'static>) {
        self.kind = BlockKind::Iter(head, iter)
    }

    pub fn get_tag(&self) -> &Tag {
        &self.tag
    }

    pub fn get_hook(&self) -> BlockGuard {
        let guard = Arc::downgrade(&self.hook);
        BlockGuard { tag: self.tag.clone(), guard }
    }

    pub fn has_block(&self) -> bool {
        !matches!(self.kind, BlockKind::None)
    }
}

pub trait BlockHandle<T> {
    fn has_blocks(&self) -> bool;

    fn try_unblock(&mut self) -> Result<(), PushError>;
}

use std::cell::{RefCell, RefMut};

use crate::data::Data;
use crate::eos::Eos;
use crate::error::PushError;
use crate::output::delta::MergedScopeDelta;
use crate::output::handle::{MultiScopeOutputHandle, OutputHandle};
use crate::output::unify::EnumStreamBufPush;
use crate::output::{AnyOutput, Output, OutputInfo};

pub struct OutputProxy<D: Data>(RefCell<OutputHandle<D, EnumStreamBufPush<D>>>);
pub struct MultiScopeOutputProxy<D: Data>(RefCell<MultiScopeOutputHandle<D, EnumStreamBufPush<D>>>);

impl<D: Data> OutputProxy<D> {
    pub fn new(
        worker_index: u16, info: OutputInfo, delta: MergedScopeDelta, output: EnumStreamBufPush<D>,
    ) -> Self {
        let handle = OutputHandle::new(worker_index, info, delta, output);
        Self(RefCell::new(handle))
    }

    pub fn downcast(output: &Box<dyn AnyOutput>) -> Option<RefMut<OutputHandle<D, EnumStreamBufPush<D>>>> {
        output
            .as_any_ref()
            .downcast_ref::<Self>()
            .map(|op| op.0.borrow_mut())
    }
}

impl<D: Data> Output for OutputProxy<D> {
    fn info(&self) -> OutputInfo {
        self.0.borrow().info()
    }

    fn flush(&self) -> Result<(), PushError> {
        self.0.borrow_mut().flush()
    }

    fn notify_eos(&self, end: Eos) -> Result<(), PushError> {
        self.0.borrow_mut().notify_end(end)
    }

    fn close(&self) -> Result<(), PushError> {
        self.0.borrow_mut().close()
    }

    fn is_closed(&self) -> bool {
        self.0.borrow().is_closed()
    }
}

impl<D: Data> MultiScopeOutputProxy<D> {
    pub fn new(
        worker_index: u16, info: OutputInfo, delta: MergedScopeDelta, output: EnumStreamBufPush<D>,
    ) -> Self {
        let handle = MultiScopeOutputHandle::new(worker_index, info, delta, output);
        Self(RefCell::new(handle))
    }

    pub fn downcast(
        output: &Box<dyn AnyOutput>,
    ) -> Option<RefMut<MultiScopeOutputHandle<D, EnumStreamBufPush<D>>>> {
        output
            .as_any_ref()
            .downcast_ref::<Self>()
            .map(|op| op.0.borrow_mut())
    }
}

impl<D: Data> Output for MultiScopeOutputProxy<D> {
    fn info(&self) -> OutputInfo {
        self.0.borrow().info()
    }

    fn flush(&self) -> Result<(), PushError> {
        self.0.borrow_mut().flush()
    }

    fn notify_eos(&self, end: Eos) -> Result<(), PushError> {
        self.0.borrow_mut().notify_end(end)
    }

    fn close(&self) -> Result<(), PushError> {
        self.0.borrow_mut().close()
    }

    fn is_closed(&self) -> bool {
        self.0.borrow().is_closed()
    }
}

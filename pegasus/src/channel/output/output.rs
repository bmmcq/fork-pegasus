use std::any::Any;
use std::cell::RefCell;
use crate::channel::output::handle::{MultiScopeOutputHandle, MultiScopeOutputSession, OutputHandle, OutputSession};
use crate::channel::output::{Output, OutputInfo};
use crate::channel::output::unify::EnumStreamPush;
use crate::{Data, Tag};
use crate::api::scope::MergedScopeDelta;
use crate::errors::IOResult;
use crate::progress::Eos;

pub struct OutputProxy<D: Data>(RefCell<OutputHandle<D, EnumStreamPush<D>>>);
pub struct MultiScopeOutputProxy<D: Data>(RefCell<MultiScopeOutputHandle<D, EnumStreamPush<D>>>);

impl <D: Data> OutputProxy<D> {

    pub fn new(info: OutputInfo, delta: MergedScopeDelta, output: EnumStreamPush<D>) -> Self {
        let handle = OutputHandle::new(info, delta, output);
        Self(RefCell::new(handle))
    }

    pub fn downcast<'a>(output: &'a Box<dyn Any + Output>) -> Option<&'a OutputProxy<D>> {
        output.downcast_ref::<Self>()
    }

    pub fn new_session(&self, tag: Tag) -> IOResult<OutputSession<D, EnumStreamPush<D>>> {
        self.0.borrow_mut().new_session(tag)
    }
}

impl<D: Data> Output for OutputProxy<D> {
    fn info(&self) -> &OutputInfo {
        self.0.borrow().info()
    }

    fn flush(&self) -> IOResult<()> {
       self.0.borrow_mut().flush()
    }

    fn notify_end(&self, end: Eos) -> IOResult<()> {
        self.0.borrow_mut().notify_end(end)
    }

    fn close(&self) -> IOResult<()> {
        self.0.borrow_mut().close()
    }

    fn is_closed(&self) -> bool {
        self.0.borrow().is_closed()
    }
}

impl <D: Data> MultiScopeOutputProxy<D> {

    pub fn new(info: OutputInfo, delta: MergedScopeDelta, output: EnumStreamPush<D>) -> Self {
        let handle = MultiScopeOutputHandle::new(info, delta, output);
        Self(RefCell::new(handle))
    }

    pub fn downcast<'a>(output: &'a Box<dyn Any + Output>) -> Option<&'a MultiScopeOutputProxy<D>> {
        output.downcast_ref::<Self>()
    }

    pub fn new_session(&self, tag: Tag) -> IOResult<MultiScopeOutputSession<D, EnumStreamPush<D>>> {
        self.0.borrow_mut().new_session(tag)
    }
}

impl<D: Data> Output for MultiScopeOutputProxy<D> {
    fn info(&self) -> &OutputInfo {
        self.0.borrow().info()
    }

    fn flush(&self) -> IOResult<()> {
        self.0.borrow_mut().flush()
    }

    fn notify_end(&self, end: Eos) -> IOResult<()> {
        self.0.borrow_mut().notify_end(end)
    }

    fn close(&self) -> IOResult<()> {
        self.0.borrow_mut().close()
    }

    fn is_closed(&self) -> bool {
        self.0.borrow().is_closed()
    }
}

use std::cell::{RefCell, RefMut};

use pegasus_common::downcast::AsAny;
use pegasus_common::tag::Tag;

use crate::base::BasePull;
use crate::data::{Data, MiniScopeBatch};
use crate::eos::Eos;
use crate::error::PullError;
use crate::input::handle::{InputHandle, MultiScopeInputHandle};
use crate::input::{Input, InputInfo};
use crate::output::AnyOutput;

pub struct InputProxy<T: Data>(RefCell<InputHandle<T, BasePull<MiniScopeBatch<T>>>>);
pub struct MultiScopeInputProxy<T: Data>(RefCell<MultiScopeInputHandle<T, BasePull<MiniScopeBatch<T>>>>);

impl<T> InputProxy<T>
where
    T: Data,
{
    pub fn new(worker_index: u16, tag: Tag, info: InputInfo, input: BasePull<MiniScopeBatch<T>>) -> Self {
        Self(RefCell::new(InputHandle::new(worker_index, tag, info, input)))
    }

    pub fn downcast(
        input: &Box<dyn AnyOutput>,
    ) -> Option<RefMut<InputHandle<T, BasePull<MiniScopeBatch<T>>>>> {
        input
            .as_any_ref()
            .downcast_ref::<Self>()
            .map(|i| i.0.borrow_mut())
    }
}

impl<T> Input for InputProxy<T>
where
    T: Data,
{
    fn info(&self) -> InputInfo {
        self.0.borrow().info()
    }

    fn check_ready(&self) -> Result<bool, PullError> {
        self.0.borrow_mut().check_ready()
    }

    fn notify_eos(&self, eos: Eos) -> Result<(), PullError> {
        self.0.borrow_mut().notify_eos(eos)
    }

    fn is_exhaust(&self) -> bool {
        self.0.borrow().is_exhaust()
    }
}

impl<T> MultiScopeInputProxy<T>
where
    T: Data,
{
    pub fn new(worker_index: u16, info: InputInfo, input: BasePull<MiniScopeBatch<T>>) -> Self {
        Self(RefCell::new(MultiScopeInputHandle::new(worker_index, info, input)))
    }

    pub fn downcast(
        input: &Box<dyn AnyOutput>,
    ) -> Option<RefMut<MultiScopeInputHandle<T, BasePull<MiniScopeBatch<T>>>>> {
        input
            .as_any_ref()
            .downcast_ref::<Self>()
            .map(|i| i.0.borrow_mut())
    }
}

impl<T> Input for MultiScopeInputProxy<T>
where
    T: Data,
{
    fn info(&self) -> InputInfo {
        self.0.borrow().info()
    }

    fn check_ready(&self) -> Result<bool, PullError> {
        self.0.borrow_mut().check_ready()
    }

    fn notify_eos(&self, eos: Eos) -> Result<(), PullError> {
        self.0.borrow_mut().notify_eos(eos)
    }

    fn is_exhaust(&self) -> bool {
        self.0.borrow().is_exhaust()
    }
}

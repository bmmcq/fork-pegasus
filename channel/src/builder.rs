use crate::data::Data;
use crate::output::builder::SharedOutputBuild;
use crate::output::delta::ScopeDelta;
use crate::output::streaming::partition::PartitionRoute;

pub enum ChannelKind<T: Data> {
    Pipeline,
    Exchange(Box<dyn PartitionRoute<Item = T> + Send + 'static>),
    Aggregate,
    Broadcast,
}

impl<T: Data> ChannelKind<T> {
    pub fn is_pipeline(&self) -> bool {
        matches!(self, Self::Pipeline)
    }
}

pub struct ChannelBuilder<T: Data> {

    source: SharedOutputBuild<T>,

    batch_size: u16,

    batch_capacity: u16,

    max_concurrent_scopes: u16,

    kind: ChannelKind<T>,
}

impl<T: Data> ChannelBuilder<T> {

    pub fn set_batch_size(&mut self, batch_size: u16) -> &mut Self {
        self.batch_size = batch_size;
        self
    }

    pub fn get_batch_size(&self) -> u16 {
        self.batch_size
    }

    pub fn set_batch_capacity(&mut self, capacity: u16) -> &mut Self {
        self.batch_capacity = capacity;
        self
    }

    pub fn get_batch_capacity(&self) -> u16 {
        self.batch_capacity
    }

    pub fn set_max_concurrent_scopes(&mut self, size: u16) -> &mut Self {
        self.max_concurrent_scopes = size;
        self
    }

    pub fn get_max_concurrent_scopes(&self) -> u16 {
        self.max_concurrent_scopes
    }

    pub fn set_channel_kind(&mut self, kind: ChannelKind<T>) -> &mut Self {
        self.kind = kind;
        self
    }

    pub fn add_delta(&mut self, delta: ScopeDelta) -> Option<ScopeDelta> {
        self.source.add_delta(delta)
    }

    pub fn get_scope_level(&self) -> u8 {
        self.source.get_scope_level()
    }

    pub fn is_pipeline(&self) -> bool {
        self.kind.is_pipeline()
    }
}
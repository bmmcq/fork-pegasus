use crate::data::Data;
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

use std::iter::Map;

use pegasus_channel::data::{Data, Item};
use pegasus_channel::output::streaming::partition::PartitionRoute;

pub struct Dataflow<It> {
    iter: It,
    builder: DataflowBuilder,
}

impl<It> Dataflow<It>
where
    It: Iterator + Send + 'static,
{
    pub fn map<T, F>(self, f: F) -> Dataflow<Map<It, F>>
    where
        F: FnMut(It::Item) -> T,
    {
        let map = self.iter.map(f);
        Dataflow { iter: map, builder: self.builder }
    }
}

impl <D, It> Dataflow<It> where D: Data, It: Iterator<Item = D> + Send + 'static {
    pub fn exchange<R>(self, route: R) where R: PartitionRoute<Item = D> + Send + 'static {
        let Dataflow { builder, iter } = self;

        builder.input_from(iter);
    }
}

pub struct DataflowBuilder {
    operators:
}

impl DataflowBuilder {
    pub fn input_from<Iter, D>(&self, iter: Iter)
    where
        D: Data,
        Iter: IntoIterator<Item = D>,
        Iter::IntoIter: Send + 'static,
    {

    }
}

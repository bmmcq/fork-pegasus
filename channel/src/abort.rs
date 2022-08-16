use pegasus_common::tag::Tag;

pub trait AbortHandle {
    /// stop and abort producing or consuming or processing any data of that scope;
    ///
    fn abort(&mut self, tag: Tag, worker: u16) -> Option<Tag>;
}

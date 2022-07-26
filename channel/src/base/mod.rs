use std::collections::LinkedList;
use crate::data::Data;
use crate::error::IOError;
use crate::{ChannelId, ChannelInfo, Pull, Push};
use crate::output::streaming::Pushed;

mod inter_processes;
mod intra_process;
mod intra_thread;


#[enum_dispatch(Push<T>)]
pub enum BasePush<T: Data> {
    IntraThread(intra_thread::ThreadPush<T>),
    IntraProcess(intra_process::IntraProcessPush<T>),
}

#[enum_dispatch(Pull<T>)]
pub enum BasePull<T: Data> {
    IntraThread(intra_thread::ThreadPull<T>),
    IntraProcess(intra_process::IntraProcessPull<T>),
}

pub fn alloc_pipeline<T: Data>(ch_id: ChannelId) -> (BasePush<T>, BasePull<T>) {
    let (push, pull) = intra_thread::pipeline::<T>(ch_id);
    (BasePush::IntraThread(push), BasePull::IntraThread(pull))
}

pub fn alloc_local_exchange<T: Data>(peers: u16, ch_id: ChannelId) -> LinkedList<(Vec<BasePush<T>>, BasePull<T>)> {
    assert!(peers > 1);
    let mut sends = LinkedList::new();
    let mut recvs = LinkedList::new();
    for _ in 0..peers {
        let (send, recv) = pegasus_common::channel::unbound::<T>();
        sends.push_back(send);
        recvs.push_back(recv);
    }

    let last = recvs.pop_back().expect("unreachable: peers > 1;");
    let mut chs = LinkedList::new();
    for r in recvs {
        let sends = sends.clone();
        let mut pushes = Vec::with_capacity(sends.len());
        for send in sends {
            let push = intra_process::IntraProcessPush::new(ch_id, send);
            pushes.push(BasePush::IntraProcess(push));
        }
        let pull = BasePull::IntraProcess(intra_process::IntraProcessPull::new(ch_id, r));
        chs.push_back((pushes, pull))
    }

    let mut pushes = Vec::with_capacity(sends.len());
    for send in sends {
        let push = intra_process::IntraProcessPush::new(ch_id, send);
        pushes.push(BasePush::IntraProcess(push));
    }
    let pull = BasePull::IntraProcess(intra_process::IntraProcessPull::new(ch_id, last));
    chs.push_back((pushes, pull));

    chs
}
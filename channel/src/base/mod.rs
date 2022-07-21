
use crate::data::Data;
use crate::{Push, Pull};
use crate::error::IOError;

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

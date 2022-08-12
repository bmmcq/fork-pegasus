use std::sync::atomic::{AtomicU32, Ordering};

use pegasus_common::config::JobServerConfig;
use pegasus_server::{Decode, ServerInstance};

pub use crate::base::inter_processes::{Decoder, SimpleDecoder};
use crate::data::Data;
use crate::error::{IOError, PullError, PushError};
use crate::{ChannelId, Pull, Push};

mod inter_processes;
mod intra_process;
mod intra_thread;

#[enum_dispatch(Push<T>)]
pub enum BasePush<T: Data> {
    IntraThread(intra_thread::ThreadPush<T>),
    IntraProcess(intra_process::IntraProcessPush<T>),
    InterProcesses(inter_processes::RemotePush<T>),
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

pub fn alloc_local_exchange<T: Data>(peers: u16, ch_id: ChannelId) -> Vec<(Vec<BasePush<T>>, BasePull<T>)> {
    assert!(peers > 0);
    let mut sends = Vec::with_capacity(peers as usize);
    let mut recvs = Vec::with_capacity(peers as usize);
    for _ in 0..peers {
        let (send, recv) = pegasus_common::channel::unbound::<T>();
        sends.push(send);
        recvs.push(recv);
    }

    let last = recvs.pop().expect("unreachable: peers > 1;");
    let mut chs = Vec::with_capacity(peers as usize);
    for r in recvs {
        let sends = sends.clone();
        let mut pushes = Vec::with_capacity(sends.len());
        for (i, send) in sends.into_iter().enumerate() {
            let push = intra_process::IntraProcessPush::new(i as u16, ch_id, send);
            pushes.push(BasePush::IntraProcess(push));
        }

        let pull = BasePull::IntraProcess(intra_process::IntraProcessPull::new(ch_id, r));
        chs.push((pushes, pull))
    }

    let mut pushes = Vec::with_capacity(sends.len());
    for (i, send) in sends.into_iter().enumerate() {
        let push = intra_process::IntraProcessPush::new(i as u16, ch_id, send);
        pushes.push(BasePush::IntraProcess(push));
    }

    let pull = BasePull::IntraProcess(intra_process::IntraProcessPull::new(ch_id, last));
    chs.push((pushes, pull));

    chs
}

pub async fn alloc_cluster_exchange<T, D>(
    ch_id: ChannelId, config: &JobServerConfig, decoders: Vec<D>,
) -> Result<Vec<(Vec<BasePush<T>>, BasePull<T>)>, IOError>
where
    T: Data + Decode,
    D: Decoder<Item = T> + Send + 'static,
{
    let peers = config.total_peers();
    assert!(peers > 1);
    let this_server_id = ServerInstance::global().get_id();

    let range = config
        .get_peers_on_server(this_server_id)
        .expect("server not include;");
    let local_peers = range.len();
    assert_eq!(local_peers, decoders.len());

    let mut local_pushes = Vec::with_capacity(local_peers);
    let mut recvs = Vec::with_capacity(local_peers);
    for i in 0..local_peers {
        let (send, recv) = pegasus_common::channel::unbound::<T>();
        local_pushes.push(intra_process::IntraProcessPush::<T>::new(i as u16, ch_id, send));
        recvs.push(recv);
    }

    let servers = config.servers().collect::<Vec<_>>();
    // convert plan channel id to ipc channel id;
    let ipc_ch_id = NEXT_IPC_CHANNEL_ID.fetch_add(1, Ordering::SeqCst);

    let mut forwards = Vec::with_capacity(local_pushes.len());
    for (i, d) in decoders.into_iter().enumerate() {
        let p = local_pushes[i].clone();
        forwards.push(inter_processes::RemoteForward::new(p, d));
    }
    let mut producers = ServerInstance::global()
        .alloc_ipc_channel(ipc_ch_id, &servers, forwards)
        .await?;

    let mut chs = Vec::with_capacity(recvs.len());
    let last = recvs
        .pop()
        .expect("channel receiver unexpected eof;");

    for r in recvs {
        let pull = intra_process::IntraProcessPull::new(ch_id, r);
        let mut pushes = Vec::with_capacity(peers as usize);
        for (peers, server_id) in config.as_ref() {
            if *server_id == this_server_id {
                assert_eq!(*peers as usize, local_pushes.len());
                for p in local_pushes.clone() {
                    pushes.push(BasePush::IntraProcess(p));
                }
            } else {
                let p = producers
                    .get(server_id)
                    .cloned()
                    .expect("ipc sender unexpected eof;");
                for i in 0..*peers - 1 {
                    let push = inter_processes::RemotePush::new(*server_id, i, p.clone());
                    pushes.push(BasePush::InterProcesses(push));
                }
                let push = inter_processes::RemotePush::new(*server_id, peers - 1, p);
                pushes.push(BasePush::InterProcesses(push));
            }
        }
        chs.push((pushes, BasePull::IntraProcess(pull)));
    }

    let mut pushes = Vec::with_capacity(peers as usize);
    for (peers, server_id) in config.as_ref() {
        if *server_id == this_server_id {
            let to_local_pushes = std::mem::replace(&mut local_pushes, vec![]);
            assert_eq!(*peers as usize, to_local_pushes.len());
            for p in to_local_pushes {
                pushes.push(BasePush::IntraProcess(p));
            }
        } else {
            let p = producers
                .remove(server_id)
                .expect("producer lost");
            for i in 0..*peers - 1 {
                let push = inter_processes::RemotePush::new(*server_id, i, p.clone());
                pushes.push(BasePush::InterProcesses(push));
            }
            let push = inter_processes::RemotePush::new(*server_id, peers - 1, p);
            pushes.push(BasePush::InterProcesses(push));
        }
    }
    let pull = BasePull::IntraProcess(intra_process::IntraProcessPull::new(ch_id, last));
    chs.push((pushes, pull));
    Ok(chs)
}

static NEXT_IPC_CHANNEL_ID: AtomicU32 = AtomicU32::new(0);

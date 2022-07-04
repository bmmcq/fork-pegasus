use std::collections::HashMap;
use std::pin::Pin;
use std::task::{Context, Poll};

use futures::stream::SelectAll;
use futures::Stream;
use pegasus::{JobConf, JobServerConf};
use tonic::Streaming;

use crate::client::connection::Connection;
use crate::pb::{JobConfig, JobRequest};
use crate::JobResponse;

mod connection;
mod errors;

pub use connection::ServerAddrTable;
pub use errors::JobExecError;

pub fn set_up<T>(addr_table: T)
where
    T: ServerAddrTable,
{
    crate::client::connection::set_server_addr_table(addr_table)
}

pub struct JobClient {
    cached_conns: HashMap<u64, Connection>,
    server_size : usize,
    submit_cnt: usize,
    is_closed: bool,
}

impl JobClient {
    pub fn new() -> Self {
        let server_size = crate::client::connection::count_available_server();
        JobClient { cached_conns: HashMap::new(), server_size, submit_cnt: 0, is_closed: false }
    }

    pub async fn submit(
        &mut self, mut config: JobConf, job: Vec<u8>,
    ) -> Result<JobResponseStream, JobExecError> {
        self.submit_cnt += 1;
        let mut conf: JobConfig = JobConfig::default();
        conf.job_id = config.job_id;
        std::mem::swap(&mut config.job_name, &mut conf.job_name);
        conf.workers = config.workers;
        conf.time_limit = config.time_limit;
        conf.batch_size = config.batch_size;
        conf.batch_capacity = config.batch_capacity;
        conf.memory_limit = config.memory_limit;
        conf.trace_enable = config.trace_enable;

        match config.servers() {
            JobServerConf::Select(ref list) => {
                if list.is_empty() {
                    return Err(JobExecError::InvalidConfig("server list can't be empty".to_owned()));
                }

                conf.servers = list.clone();
                let req = JobRequest { conf: Some(conf), payload: job };
                if list.len() == 1 {
                    let index = (self.submit_cnt % self.server_size) as u64;
                    let conn = self.get_connection(index).await?;
                    let resp = conn.submit(req).await?;
                    let stream = resp.into_inner();
                    Ok(JobResponseStream::Single(stream))
                } else {
                    self.multi_submit(list, req).await
                }
            }
            JobServerConf::Total(n) => {
                if *n == 0 {
                    return Err(JobExecError::InvalidConfig("server size can't be 0".to_owned()));
                }

                if *n == 1 {
                    let req = JobRequest { conf: Some(conf), payload: job };
                    let index = (self.submit_cnt % self.server_size) as u64;
                    let conn = self.get_connection(index).await?;
                    let resp = conn.submit(req).await?;
                    let stream = resp.into_inner();
                    Ok(JobResponseStream::Single(stream))
                } else {
                    let list = (0..*n).collect::<Vec<_>>();
                    conf.servers = list.clone();
                    let req = JobRequest { conf: Some(conf), payload: job };
                    self.multi_submit(&list, req).await
                }
            }
        }
    }

    pub async fn close(&mut self) {
        self.is_closed = true;
        let conns = std::mem::replace(&mut self.cached_conns, Default::default());
        crate::client::connection::recycle(conns.into_iter().map(|(_, v)| v)).await
    }

    async fn get_connection(&mut self, server_id: u64) -> Result<&mut Connection, JobExecError> {
        if self.cached_conns.contains_key(&server_id) {
            return Ok(self.cached_conns.get_mut(&server_id).unwrap());
        }

        let conn = crate::client::connection::get_connection(server_id).await?;
        Ok(self
            .cached_conns
            .entry(server_id)
            .or_insert(conn))
    }

    async fn multi_submit(
        &mut self, servers: &[u64], req: JobRequest,
    ) -> Result<JobResponseStream, JobExecError> {
        let mut resp_vec = Vec::new();
        for id in &servers[1..] {
            let conn = self.get_connection(*id).await?;
            let resp = conn.submit(req.clone()).await?.into_inner();
            resp_vec.push(resp);
        }
        let conn = self.get_connection(servers[0]).await?;
        let resp = conn.submit(req).await?.into_inner();
        resp_vec.push(resp);
        Ok(JobResponseStream::select(resp_vec))
    }
}

impl Drop for JobClient {
    fn drop(&mut self) {
        let conns = std::mem::replace(&mut self.cached_conns, Default::default());
        crate::client::connection::try_recycle(conns.into_iter().map(|(_, v)| v))
    }
}

pub enum JobResponseStream {
    Single(Streaming<JobResponse>),
    Select(SelectAll<Streaming<JobResponse>>),
}

impl JobResponseStream {
    pub fn select<I>(streams: I) -> Self
    where
        I: IntoIterator<Item = Streaming<JobResponse>>,
    {
        let select = futures::stream::select_all(streams);
        JobResponseStream::Select(select)
    }
}

impl Stream for JobResponseStream {
    type Item = Result<Vec<u8>, JobExecError>;

    fn poll_next(mut self: Pin<&mut Self>, cx: &mut Context<'_>) -> Poll<Option<Self::Item>> {
        match *self {
            JobResponseStream::Single(ref mut s) => Pin::new(s)
                .poll_next(cx)
                .map_ok(|r| r.payload)
                .map_err(|e| JobExecError::ServerError(e)),
            JobResponseStream::Select(ref mut s) => Pin::new(s)
                .poll_next(cx)
                .map_ok(|r| r.payload)
                .map_err(|e| JobExecError::ServerError(e)),
        }
    }
}

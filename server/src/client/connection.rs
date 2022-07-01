use std::collections::{HashMap, VecDeque};
use std::net::SocketAddr;
use std::ops::{Deref, DerefMut};
use std::time::Instant;

use ahash::AHashMap;
use crossbeam_utils::sync::ShardedLock;
use lazy_static::lazy_static;
use tokio::sync::Mutex;

use crate::client::errors::ConnectError;
use crate::pb::job_service_client::JobServiceClient;
use crate::ServerId;

pub(crate) struct Connection {
    server_id: ServerId,
    server_addr: SocketAddr,
    client: JobServiceClient<tonic::transport::Channel>,
}

impl Deref for Connection {
    type Target = JobServiceClient<tonic::transport::Channel>;

    fn deref(&self) -> &Self::Target {
        &self.client
    }
}

impl DerefMut for Connection {
    fn deref_mut(&mut self) -> &mut Self::Target {
        &mut self.client
    }
}
// TODO: maybe async trait;
pub trait ServerAddrTable: Send + Sync + 'static {
    fn get_addr(&self, server_id: ServerId) -> Option<SocketAddr>;
}

impl ServerAddrTable for HashMap<ServerId, SocketAddr> {
    fn get_addr(&self, server_id: ServerId) -> Option<SocketAddr> {
        self.get(&server_id).map(|s| *s)
    }
}

lazy_static! {
    static ref SERVER_ADDR_TABLE: ShardedLock<Option<Box<dyn ServerAddrTable>>> = ShardedLock::new(None);
    static ref CONNECTION_MGR: Mutex<ConnectionManager> = Mutex::new(ConnectionManager::new());
}

pub(crate) fn set_server_addr_table<T>(table: T)
where
    T: ServerAddrTable,
{
    let shared_table = Box::new(table);
    let mut lock = SERVER_ADDR_TABLE
        .write()
        .expect("address table write lock poisoned");
    *lock = Some(shared_table)
}

pub(crate) async fn get_connection(server_id: u64) -> Result<Connection, ConnectError> {
    let mut lock = CONNECTION_MGR.lock().await;
    lock.get_connection(server_id).await
}

pub(crate) async fn recycle<I>(conns: I)
where
    I: IntoIterator<Item = Connection>,
{
    let mut lock = CONNECTION_MGR.lock().await;
    for conn in conns {
        lock.recycle_connection(conn);
    }
}

pub(crate) fn try_recycle<I>(conns: I)
where
    I: IntoIterator<Item = Connection>,
{
    match CONNECTION_MGR.try_lock() {
        Ok(mut locked) => {
            for conn in conns {
                locked.recycle_connection(conn);
            }
        }
        Err(_) => {
            warn!("fail to try recycle connections;")
        }
    }
}

#[inline]
fn fetch_latest_addr(server_id: ServerId) -> Result<SocketAddr, ConnectError> {
    match SERVER_ADDR_TABLE.try_read() {
        Ok(lock) => {
            if let Some(table) = lock.as_ref() {
                if let Some(addr) = table.get_addr(server_id) {
                    Ok(addr)
                } else {
                    Err(ConnectError::ServerNotFount(server_id))
                }
            } else {
                Err(ConnectError::AddrTableNotFount)
            }
        }
        Err(_) => Err(ConnectError::AddrTableNotFount),
    }
}

struct CachedConns {
    server_id: ServerId,
    server_addr: SocketAddr,
    last_check_time: Instant,
    conns: VecDeque<Connection>,
}

impl CachedConns {
    fn new(server_id: ServerId) -> Result<Self, ConnectError> {
        let server_addr = fetch_latest_addr(server_id)?;
        Ok(Self { server_id, server_addr, last_check_time: Instant::now(), conns: VecDeque::new() })
    }

    async fn get(&mut self) -> Result<Connection, ConnectError> {
        if !self.conns.is_empty() {
            if self.last_check_time.elapsed().as_secs() < 5 {
                return Ok(self.conns.pop_front().unwrap());
            }

            let addr = fetch_latest_addr(self.server_id)?;
            self.last_check_time = Instant::now();
            if addr == self.server_addr {
                return Ok(self.conns.pop_front().unwrap());
            }
            debug!("update server[{}] address from {} to {};", self.server_id, self.server_addr, addr);
            self.server_addr = addr;
            self.conns.clear();
        }

        if self.last_check_time.elapsed().as_secs() >= 5 {
            let addr = fetch_latest_addr(self.server_id)?;
            if self.server_addr != addr {
                debug!("update server[{}] address from {} to {};", self.server_id, self.server_addr, addr);
                self.server_addr = addr;
            }
            self.last_check_time = Instant::now();
        }
        self.connect().await
    }

    fn recycle(&mut self, conn: Connection) {
        if conn.server_addr == self.server_addr {
            self.conns.push_back(conn);
        }
    }

    async fn connect(&self) -> Result<Connection, ConnectError> {
        let url = format!("http://{}:{}", self.server_addr.ip(), self.server_addr.port());
        match JobServiceClient::connect(url).await {
            Ok(client) => {
                debug!("create new connection to server[{}] at {};", self.server_id, self.server_addr);
                Ok(Connection { server_id: self.server_id, server_addr: self.server_addr, client })
            }
            Err(e) => {
                error!(
                    "can't create connection to server {} at {}: {}",
                    self.server_id, self.server_addr, e
                );
                Err(ConnectError::FailConnect(self.server_id, e))
            }
        }
    }
}

pub struct ConnectionManager {
    conns: AHashMap<ServerId, CachedConns>,
}

impl ConnectionManager {
    fn new() -> Self {
        Self { conns: AHashMap::new() }
    }

    async fn get_connection(&mut self, server_id: ServerId) -> Result<Connection, ConnectError> {
        if let Some(pool) = self.conns.get_mut(&server_id) {
            pool.get().await
        } else {
            let mut pool = CachedConns::new(server_id)?;
            let conn = pool.get().await?;
            self.conns.insert(server_id, pool);
            Ok(conn)
        }
    }

    fn recycle_connection(&mut self, conn: Connection) {
        if let Some(pool) = self.conns.get_mut(&conn.server_id) {
            pool.recycle(conn)
        }
    }
}

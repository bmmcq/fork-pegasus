//
//! Copyright 2020 Alibaba Group Holding Limited.
//!
//! Licensed under the Apache License, Version 2.0 (the "License");
//! you may not use this file except in compliance with the License.
//! You may obtain a copy of the License at
//!
//! http://www.apache.org/licenses/LICENSE-2.0
//!
//! Unless required by applicable law or agreed to in writing, software
//! distributed under the License is distributed on an "AS IS" BASIS,
//! WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
//! See the License for the specific language governing permissions and
//! limitations under the License.

use std::hash::Hasher;
use std::path::Path;

use ahash::AHasher;
use pegasus_network::config::{NetworkConfig, ServerAddr};
use serde::Deserialize;

use crate::errors::StartupError;

#[macro_export]
macro_rules! configure_with_default {
    ($ty:ty, $name:expr, $value: expr) => {{
        std::env::var($name)
            .map(|s| s.parse::<$ty>().unwrap_or($value))
            .unwrap_or($value)
    }};
}

#[derive(Debug, Deserialize)]
pub struct Configuration {
    pub network: Option<NetworkConfig>,
    pub max_pool_size: Option<u32>,
}

impl Configuration {
    pub fn parse(content: &str) -> Result<Self, toml::de::Error> {
        toml::from_str(&content)
    }

    pub fn singleton() -> Self {
        Configuration { network: None, max_pool_size: None }
    }

    pub fn new(server_id: u64, servers: Vec<(String, u16)>) -> Self {
        let network = Some(NetworkConfig::with(
            server_id,
            servers
                .into_iter()
                .map(|(a, b)| ServerAddr::new(a, b))
                .collect(),
        ));
        Self { network, max_pool_size: None }
    }

    pub fn server_id(&self) -> u64 {
        if let Some(net_conf) = self.network.as_ref() {
            net_conf.server_id
        } else {
            0
        }
    }

    pub fn servers_size(&self) -> usize {
        if let Some(net_conf) = self.network.as_ref() {
            net_conf.servers_size
        } else {
            1
        }
    }

    pub fn network_config(&self) -> Option<&NetworkConfig> {
        self.network.as_ref()
    }
}

pub fn read_from<P: AsRef<Path>>(path: P) -> Result<Configuration, StartupError> {
    let config_str = std::fs::read_to_string(path)?;
    Ok(Configuration::parse(&config_str)?)
}

lazy_static! {
    /// set `true` to enable canceling all descendants' data of the early-stop scope
    pub static ref ENABLE_CANCEL_CHILD: bool = configure_with_default!(bool, "ENABLE_CANCEL_CHILD", true);
}

#[derive(Debug, Clone)]
pub enum JobServerConf {
    /// Only server in the list will be used;
    Select(Vec<u64>),
    /// specify total 'n' servers will be used;
    Total(u64)
}

impl JobServerConf {
    pub fn server_size(&self) -> usize {
        match self {
            JobServerConf::Select(v) => v.len(),
            JobServerConf::Total(n) => *n as usize
        }
    }

    pub fn get_server_ids(&self) -> Vec<u64> {
        match self {
            JobServerConf::Select(list) => list.clone(),
            JobServerConf::Total(n) => {
                if *n <= 1 {
                    vec![]
                } else {
                    (0..*n).collect::<Vec<_>>()
                }
            }
        }
    }
}

#[derive(Debug, Clone)]
pub struct JobConf {
    /// unique identifier of the job;
    pub job_id: u64,
    /// the name to describe the job;
    pub job_name: String,
    /// workers per server;
    pub workers: u32,
    /// the most milliseconds the job can run;
    pub time_limit: u64,
    /// the size used to batching streaming data;
    pub batch_size: u32,
    /// the size used to limit each operator's output size per-schedule;
    pub batch_capacity: u32,
    /// the most memory(MB) this job can use in each server;
    pub memory_limit: u32,
    /// set to print runtime dataflow plan before running;
    pub plan_print: bool,
    /// the id of servers this job will run on;
    servers: JobServerConf,
    /// set enable trace job run progress;
    pub trace_enable: bool,
    /// optimization factors of early-stop
    pub debug: bool,
}

impl JobConf {
    pub fn new<S: Into<String>>(name: S) -> Self {
        let mut conf = JobConf::default();
        let name = name.into();
        let mut hasher = AHasher::new_with_keys(74786, 65535);
        hasher.write(name.as_bytes());
        conf.job_id = hasher.finish();
        conf.job_name = name;
        conf
    }

    pub fn with_id<S: Into<String>>(job_id: u64, name: S, workers: u32) -> Self {
        let mut conf = JobConf::default();
        conf.job_id = job_id;
        conf.job_name = name.into();
        conf.workers = workers;
        conf
    }

    pub fn set_workers(&mut self, workers: u32) {
        self.workers = workers;
    }

    pub fn servers(&self) -> &JobServerConf {
        &self.servers
    }

    pub fn reset_servers(&mut self, servers: JobServerConf) {
        self.servers = servers
    }

    pub fn total_workers(&self) -> usize {
        let len = self.servers.server_size();
        if len == 0 {
            return self.workers as usize;
        } else {
            self.servers.server_size() * self.workers as usize
        }
    }
}

impl Default for JobConf {
    fn default() -> Self {
        let plan_print = log_enabled!(log::Level::Trace);
        JobConf {
            job_id: 0,
            job_name: "anonymity".to_owned(),
            workers: 1,
            time_limit: !0,
            batch_size: 1024,
            batch_capacity: 64,
            memory_limit: !0u32,
            plan_print,
            servers: JobServerConf::Total(1),
            trace_enable: false,
            debug: false,
        }
    }
}

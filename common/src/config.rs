use std::ops::Range;

pub type ServerId = u8;

pub struct JobServerConfig {
    servers: Vec<(u8, ServerId)>,
}

impl JobServerConfig {
    pub fn include_servers(&self) -> usize {
        self.servers.len()
    }

    pub fn total_peers(&self) -> u16 {
        let mut cnt = 0;
        for (p, _) in self.servers.iter() {
            cnt += *p as u16;
        }
        cnt
    }

    pub fn get_peers_on_server(&self, server_id: ServerId) -> Option<Range<u16>> {
        let mut start = 0u16;
        for (p, id) in self.servers.iter() {
            if *id == server_id {
                let end = start + *p as u16;
                return Some(start..end);
            } else {
                start += *p as u16;
            }
        }
        None
    }

    pub fn servers(&self) -> impl Iterator<Item = ServerId> + '_ {
        self.servers.iter().map(|(_, id)| *id)
    }

    pub fn as_ref(&self) -> &[(u8, ServerId)] {
        &self.servers
    }
}

pub struct JobConfig {
    job_id: u64,
    default_batch_size: u16,
    default_batch_capacity: u16,
    job_name: String,
    servers: JobServerConfig,
}

impl JobConfig {}

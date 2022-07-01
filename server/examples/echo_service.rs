use std::collections::HashMap;
use std::io::Write;
use std::net::{IpAddr, SocketAddr};
use std::path::PathBuf;
use std::str::FromStr;
use std::time::Instant;

use pegasus::api::Sink;
use pegasus::{BuildJobError, Configuration, JobConf, JobServerConf, Worker};
use pegasus_server::client::JobExecError;
use pegasus_server::job::JobAssembly;
use pegasus_server::rpc::RPCServerConfig;
use structopt::StructOpt;
use tokio_stream::StreamExt;

#[derive(Debug, StructOpt)]
#[structopt(name = "EchoServer", about = "example of rpc service")]
struct Config {
    #[structopt(long = "config", parse(from_os_str))]
    config_dir: PathBuf,
    #[structopt(long = "server")]
    server: bool,
    #[structopt(long = "client")]
    client: bool,
    #[structopt(short = "t", long = "times", default_value = "100")]
    times: u64,
    #[structopt(long = "setaddr")]
    setaddr: bool,
}

struct EchoJobParser;

impl JobAssembly for EchoJobParser {
    fn assemble(
        &self, job: &mut Vec<u8>, worker: &mut Worker<Vec<u8>, Vec<u8>>,
    ) -> Result<(), BuildJobError> {
        worker.dataflow(|input, output| {
            let src = std::mem::replace(job, vec![]);
            input.input_from(Some(src))?.sink_into(output)
        })
    }
}

#[tokio::main]
async fn main() {
    pegasus_common::logs::init_log();
    let config: Config = Config::from_args();
    let (server_config, rpc_config) = pegasus_server::config::load_configs(&config.config_dir).unwrap();
    if config.server {
        pegasus_server::cluster::standalone::start(rpc_config, server_config, EchoJobParser)
            .await
            .expect("start server failure;")
    } else if config.client {
        let servers_size = server_config.servers_size();
        let addr_table = resolve_server_addr(servers_size, &config, server_config, rpc_config);
        pegasus_server::client::set_up(addr_table);
        let mut client = pegasus_server::client::JobClient::new();
        let start = Instant::now();
        for i in 0..config.times {
            let mut conf = JobConf::with_id(i + 1, "Echo example", 1);
            conf.reset_servers(JobServerConf::Total(servers_size as u64));
            let result = client.submit(conf, vec![8u8; 8]).await.unwrap();
            let result_set: Result<Vec<Vec<u8>>, JobExecError> = result.collect().await;
            let result_set = result_set.unwrap();
            assert_eq!(result_set.len(), servers_size);
            for res in result_set {
                assert_eq!(res, vec![8u8; 8]);
            }
        }
        println!("finish {} echo request, used {:?};", config.times, start.elapsed());
    } else {
        println!("--server or --client");
    }
}

fn resolve_server_addr(
    servers_size: usize, config: &Config, server_config: Configuration, rpc_config: RPCServerConfig,
) -> HashMap<u64, SocketAddr> {
    let mut addr_table = HashMap::new();
    if config.setaddr {
        let mut not_connect = servers_size;
        let mut server_id = 0;
        while not_connect > 0 {
            print!("give server {} address: ", server_id);
            std::io::stdout().flush().unwrap();
            let mut buf = String::new();
            std::io::stdin().read_line(&mut buf).unwrap();
            let parsed = buf
                .trim_end_matches(|c| c == '\n' || c == '\t' || c == ' ')
                .parse::<SocketAddr>();
            match parsed {
                Ok(addr) => {
                    addr_table.insert(server_id, addr);
                    server_id += 1;
                    not_connect -= 1;
                }
                Err(e) => {
                    eprintln!("error address format {} error: {}", buf, e)
                }
            }
        }
    } else if servers_size == 1 {
        let mut host = "0.0.0.0".to_owned();
        if let Some(net_conf) = server_config.network_config() {
            if let Some(addr) = net_conf
                .get_server_addr(0)
                .map(|s| s.get_ip().to_owned())
            {
                host = addr;
            }
        } else if let Some(addr) = rpc_config.rpc_host {
            host = addr;
        }
        let host = IpAddr::from_str(&host).unwrap();
        let port = rpc_config
            .rpc_port
            .as_ref()
            .copied()
            .expect("rpc port not found");
        addr_table.insert(0, SocketAddr::new(host, port));
    } else {
        let net_conf = server_config
            .network_config()
            .expect("network config not found");
        let port = rpc_config
            .rpc_port
            .as_ref()
            .copied()
            .expect("rpc port not found");
        for i in 0..servers_size {
            let addr = net_conf
                .get_server_addr(i as u64)
                .expect("server not found");
            let host = IpAddr::from_str(addr.get_ip()).unwrap();
            addr_table.insert(i as u64, SocketAddr::new(host, port));
        }
    }
    addr_table
}

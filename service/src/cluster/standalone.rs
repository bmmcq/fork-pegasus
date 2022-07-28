use crate::job::JobAssembly;
use crate::rpc::{ListenerToInfo, RPCServerConfig};

pub async fn start<P>(
    rpc_config: RPCServerConfig, server_config: pegasus::Configuration, assemble: P,
) -> Result<(), Box<dyn std::error::Error>>
where
    P: JobAssembly,
{
    let detect = if let Some(net_conf) = server_config.network_config() {
        net_conf.get_servers()?.unwrap_or(vec![])
    } else {
        vec![]
    };

    crate::rpc::start_all(rpc_config, server_config, assemble, detect, ListenerToInfo).await?;
    Ok(())
}

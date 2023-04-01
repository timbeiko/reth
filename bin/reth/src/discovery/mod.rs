//! Discovery Debugging tool
use crate::{
    args::NetworkArgs,
    dirs::{ConfigPath, PlatformPath},
    prometheus_exporter,
    runner::CliContext,
};
use clap::{Parser, Subcommand};
use reth_db::mdbx::{Env, EnvKind, WriteMap};
use reth_discv4::DEFAULT_DISCOVERY_PORT;
use reth_network::{NetworkConfig, SessionLimits, SessionsConfig};
use reth_primitives::ChainSpec;
use reth_provider::ShareableDatabase;
use reth_staged_sync::{
    utils::{chainspec::chain_spec_value_parser, parse_socket_address},
    Config,
};
use reth_tasks::TaskExecutor;
use std::{
    net::{Ipv4Addr, SocketAddr, SocketAddrV4},
    sync::Arc,
};
use tracing::info;

/// `reth discover` command
#[derive(Debug, Parser)]
pub struct Command {
    /// The path to the configuration file to use.
    #[arg(long, value_name = "FILE", verbatim_doc_comment, default_value_t)]
    config: PlatformPath<ConfigPath>,

    /// The chain this node is running.
    ///
    /// Possible values are either a built-in chain or the path to a chain specification file.
    ///
    /// Built-in chains:
    /// - mainnet
    /// - goerli
    /// - sepolia
    #[arg(
        long,
        value_name = "CHAIN_OR_PATH",
        verbatim_doc_comment,
        default_value = "mainnet",
        value_parser = chain_spec_value_parser
    )]
    chain: Arc<ChainSpec>,

    #[clap(flatten)]
    network: NetworkArgs,

    /// Enable Prometheus metrics.
    ///
    /// The metrics will be served at the given interface and port.
    #[arg(long, value_name = "SOCKET", value_parser = parse_socket_address, help_heading = "Metrics")]
    metrics: Option<SocketAddr>,

    #[clap(subcommand)]
    command: Subcommands,
}

#[derive(Subcommand, Debug)]
/// `reth discovery` subcommands
pub enum Subcommands {
    /// Check if bootnodes are up, emitting ping metrics
    CheckBootnodes,
}

impl Command {
    /// Execute `discovery` command
    pub async fn execute(&self, ctx: CliContext) -> eyre::Result<()> {
        let tempdir = tempfile::TempDir::new()?;
        let noop_db = Arc::new(Env::<WriteMap>::open(&tempdir.into_path(), EnvKind::RW)?);

        let config: Config = confy::load_path(&self.config).unwrap_or_default();

        // ensure no eth sessions are established
        let sessions_config = SessionsConfig {
            limits: SessionLimits {
                max_pending_outbound: Some(0),
                max_pending_inbound: Some(0),
                max_established_outbound: Some(0),
                max_established_inbound: Some(0),
            },
            ..Default::default()
        };

        self.start_metrics_endpoint(Arc::clone(&noop_db)).await?;

        let network_config = self.load_network_config(
            &config,
            Arc::clone(&noop_db),
            ctx.task_executor.clone(),
            sessions_config,
        );

        let _ = network_config.start_network().await?;

        match self.command {
            Subcommands::CheckBootnodes => {
                info!(target: "reth::cli", "Checking bootnodes");
                loop {
                    // do nothing, but with an await point
                    tokio::time::sleep(std::time::Duration::from_secs(1)).await;
                }
            }
        }
    }

    async fn start_metrics_endpoint(&self, db: Arc<Env<WriteMap>>) -> eyre::Result<()> {
        if let Some(listen_addr) = self.metrics {
            info!(target: "reth::cli", addr = %listen_addr, "Starting metrics endpoint");

            prometheus_exporter::initialize_with_db_metrics(listen_addr, db).await?;
        }

        Ok(())
    }

    fn load_network_config(
        &self,
        config: &Config,
        db: Arc<Env<WriteMap>>,
        executor: TaskExecutor,
        sessions_config: SessionsConfig,
    ) -> NetworkConfig<ShareableDatabase<Arc<Env<WriteMap>>>> {
        // default is just gonna be 5 seconds for this
        self.network
            .network_config(config, self.chain.clone())
            .sessions_config(sessions_config)
            .check_bootnodes(15)
            .with_task_executor(Box::new(executor))
            .listener_addr(SocketAddr::V4(SocketAddrV4::new(
                Ipv4Addr::UNSPECIFIED,
                self.network.port.unwrap_or(DEFAULT_DISCOVERY_PORT),
            )))
            .discovery_addr(SocketAddr::V4(SocketAddrV4::new(
                Ipv4Addr::UNSPECIFIED,
                self.network.discovery.port.unwrap_or(DEFAULT_DISCOVERY_PORT),
            )))
            .build(ShareableDatabase::new(db, self.chain.clone()))
    }
}

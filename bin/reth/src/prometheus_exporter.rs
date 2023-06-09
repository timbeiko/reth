//! Prometheus exporter
use eyre::WrapErr;
use hyper::{
    service::{make_service_fn, service_fn},
    Body, Request, Response, Server,
};
use metrics_exporter_prometheus::{PrometheusBuilder, PrometheusHandle};
use metrics_util::layers::{PrefixLayer, Stack};
use reth_db::{
    database::Database,
    mdbx::{Env, WriteMap},
    tables,
};
use reth_metrics::metrics::{self, absolute_counter, describe_counter, Unit};
use std::{convert::Infallible, net::SocketAddr, sync::Arc};

pub(crate) trait Hook: Fn() + Send + Sync {}
impl<T: Fn() + Send + Sync> Hook for T {}

/// Installs Prometheus as the metrics recorder and serves it over HTTP with hooks.
///
/// The hooks are called every time the metrics are requested at the given endpoint, and can be used
/// to record values for pull-style metrics, i.e. metrics that are not automatically updated.
pub(crate) async fn initialize_with_hooks<F: Hook + 'static>(
    listen_addr: SocketAddr,
    hooks: impl IntoIterator<Item = F>,
) -> eyre::Result<()> {
    let recorder = PrometheusBuilder::new().build_recorder();
    let handle = recorder.handle();

    let hooks: Vec<_> = hooks.into_iter().collect();

    // Start endpoint
    start_endpoint(listen_addr, handle, Arc::new(move || hooks.iter().for_each(|hook| hook())))
        .await
        .wrap_err("Could not start Prometheus endpoint")?;

    // Build metrics stack
    Stack::new(recorder)
        .push(PrefixLayer::new("reth"))
        .install()
        .wrap_err("Couldn't set metrics recorder.")?;

    Ok(())
}

/// Starts an endpoint at the given address to serve Prometheus metrics.
async fn start_endpoint<F: Hook + 'static>(
    listen_addr: SocketAddr,
    handle: PrometheusHandle,
    hook: Arc<F>,
) -> eyre::Result<()> {
    let make_svc = make_service_fn(move |_| {
        let handle = handle.clone();
        let hook = Arc::clone(&hook);
        async move {
            Ok::<_, Infallible>(service_fn(move |_: Request<Body>| {
                (hook)();
                let metrics = handle.render();
                async move { Ok::<_, Infallible>(Response::new(Body::from(metrics))) }
            }))
        }
    });
    let server =
        Server::try_bind(&listen_addr).wrap_err("Could not bind to address")?.serve(make_svc);

    tokio::spawn(async move { server.await.expect("Metrics endpoint crashed") });

    Ok(())
}

/// Installs Prometheus as the metrics recorder and serves it over HTTP with database and process
/// metrics.
pub(crate) async fn initialize(
    listen_addr: SocketAddr,
    db: Arc<Env<WriteMap>>,
    process: metrics_process::Collector,
) -> eyre::Result<()> {
    let db_stats = move || {
        // TODO: A generic stats abstraction for other DB types to deduplicate this and `reth db
        // stats`
        let _ = db.view(|tx| {
            for table in tables::TABLES.iter().map(|(_, name)| name) {
                let table_db =
                    tx.inner.open_db(Some(table)).wrap_err("Could not open db.")?;

                let stats = tx
                    .inner
                    .db_stat(&table_db)
                    .wrap_err(format!("Could not find table: {table}"))?;

                let page_size = stats.page_size() as usize;
                let leaf_pages = stats.leaf_pages();
                let branch_pages = stats.branch_pages();
                let overflow_pages = stats.overflow_pages();
                let num_pages = leaf_pages + branch_pages + overflow_pages;
                let table_size = page_size * num_pages;

                absolute_counter!("db.table_size", table_size as u64, "table" => *table);
                absolute_counter!("db.table_pages", leaf_pages as u64, "table" => *table, "type" => "leaf");
                absolute_counter!("db.table_pages", branch_pages as u64, "table" => *table, "type" => "branch");
                absolute_counter!("db.table_pages", overflow_pages as u64, "table" => *table, "type" => "overflow");
            }

            Ok::<(), eyre::Report>(())
        });
    };

    // Register help strings for process metrics
    process.describe();

    let hooks: Vec<Box<dyn Hook<Output = ()>>> =
        vec![Box::new(db_stats), Box::new(move || process.collect())];
    initialize_with_hooks(listen_addr, hooks).await?;

    // We describe the metrics after the recorder is installed, otherwise this information is not
    // registered
    describe_counter!("db.table_size", Unit::Bytes, "The size of a database table (in bytes)");
    describe_counter!("db.table_pages", "The number of database pages for a table");

    Ok(())
}

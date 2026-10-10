use super::open_engine;
use crate::args::EngineRoot;
use anyhow::Context;
use console::{Style, Term};
use smallquery::TcpEndpoint;
use std::net::SocketAddr;
use std::time::Duration;

/// How long the in-process calls in flight may run on after Ctrl-C.
const SHUTDOWN_GRACE: Duration = Duration::from_secs(5);

pub async fn command_serve(
    term: &Term,
    root: EngineRoot,
    listen: SocketAddr,
) -> anyhow::Result<()> {
    let bold_style = Style::new().bold();
    let dimmed_style = Style::new().dim();
    let engine = open_engine(&root).await?;
    let server = engine
        .bind_tcp(TcpEndpoint::new(listen))
        .await
        .with_context(|| format!("Cannot listen on {listen}"))?;
    let addr = server.local_addr();
    term.write_line(&format!(
        "Serving {} on {}",
        bold_style.apply_to(root.url()),
        bold_style
            .clone()
            .green()
            .apply_to(format!("http://{addr}"))
    ))?;
    term.write_line(&format!(
        "{}",
        dimmed_style.apply_to(format!(
            "Connect with BigQueryDb::with_options_auth(BigQueryDbOptions::new(<project>)\
             .with_bigquery_api_url(\"http://{addr}\").with_bigquery_storage_api_url(\"http://{addr}\"), \
             anonymous::Builder::new().build()). Ctrl-C stops."
        ))
    ))?;
    tokio::select! {
        served = server.serve() => served.context("The server failed")?,
        signal = tokio::signal::ctrl_c() => signal.context("Cannot listen for Ctrl-C")?,
    }
    term.write_line("Shutting down.")?;
    engine.shutdown(SHUTDOWN_GRACE).await;
    Ok(())
}

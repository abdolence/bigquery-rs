//! `smq`: serves an engine's root on a loopback port, and lists and shows what it holds.

use crate::args::{CliArgs, CliCommand};
use crate::commands::*;
use clap::Parser;
use console::{Style, Term};

mod args;
mod commands;

/// Sends the engine's logs to standard error at the level `RUST_LOG` asks for. Without it, `serve`
/// logs at `info`, which shows every committed change, and the inspection commands only warn, so
/// that their table is all they print.
fn init_tracing(command: &CliCommand) {
    let default_level = match command {
        CliCommand::Serve { .. } => "info",
        CliCommand::Datasets { .. } | CliCommand::Tables { .. } | CliCommand::Show { .. } => "warn",
    };
    let filter = tracing_subscriber::EnvFilter::try_from_default_env()
        .unwrap_or_else(|_| tracing_subscriber::EnvFilter::new(default_level));
    let _ = tracing_subscriber::fmt()
        .with_env_filter(filter)
        .with_ansi(console::colors_enabled_stderr())
        .with_writer(std::io::stderr)
        .try_init();
}

#[tokio::main]
async fn main() -> std::io::Result<()> {
    let cli = CliArgs::parse();
    init_tracing(&cli.command);
    let term = Term::stdout();
    let bold_style = Style::new().bold();
    term.write_line(&format!(
        "{} v{}",
        bold_style.clone().green().apply_to("smallquery"),
        bold_style.apply_to(env!("CARGO_PKG_VERSION"))
    ))?;
    if let Err(err) = handle_args(cli, &term).await {
        Term::stderr().write_line(&format!("{}: {err:#}", bold_style.red().apply_to("Error")))?;
        std::process::exit(1);
    }
    Ok(())
}

async fn handle_args(cli: CliArgs, term: &Term) -> anyhow::Result<()> {
    match cli.command {
        CliCommand::Serve { root, listen } => command_serve(term, root, listen).await,
        CliCommand::Datasets { root, project } => command_datasets(term, root, project).await,
        CliCommand::Tables { root, dataset } => command_tables(term, root, dataset.into()).await,
        CliCommand::Show { root, table } => command_show(term, root, table.into()).await,
    }
}

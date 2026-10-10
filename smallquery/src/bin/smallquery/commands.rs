use crate::args::EngineRoot;
use anyhow::Context;
use indicatif::{ProgressBar, ProgressStyle};
use smallquery::{Engine, EngineConfig};
use std::time::Duration;

mod datasets_command;
pub use datasets_command::*;

mod serve_command;
pub use serve_command::*;

mod show_command;
pub use show_command::*;

mod tables_command;
pub use tables_command::*;

/// How long an inspection command's engine waits for its in-process calls when it closes.
const INSPECTION_GRACE: Duration = Duration::from_secs(1);

/// Opens the engine on `root` behind a spinner, since opening a large root takes a moment.
async fn open_engine(root: &EngineRoot) -> anyhow::Result<Engine> {
    let spinner = ProgressBar::new_spinner()
        .with_style(ProgressStyle::default_spinner())
        .with_message(format!("Opening {}", root.url()));
    spinner.enable_steady_tick(Duration::from_millis(100));
    let opened = Engine::open(EngineConfig::new(root.url().clone())).await;
    spinner.finish_and_clear();
    opened.with_context(|| format!("Cannot open the engine on {}", root.url()))
}

/// Opens the engine on a root that must exist already: [`Engine::open`] initializes an empty
/// root, which an inspection command must not leave behind on a mistyped path.
async fn open_existing_engine(root: &EngineRoot) -> anyhow::Result<Engine> {
    if let Some(dir) = root.local_dir() {
        let marker = dir.join("_smallquery").join("engine.json");
        if !marker.is_file() {
            anyhow::bail!(
                "{} is not a smallquery root: {} is missing. `smq serve --root` creates one",
                dir.display(),
                marker.display()
            );
        }
    }
    open_engine(root).await
}

/// The status of a failed in-process call, as its code and the engine's message.
fn call_failed(status: tonic::Status) -> anyhow::Error {
    anyhow::anyhow!("{:?}: {}", status.code(), status.message())
}

/// `count` followed by `noun`, in the plural unless `count` is 1.
fn counted(count: usize, noun: &str) -> String {
    if count == 1 {
        format!("1 {noun}")
    } else {
        format!("{count} {noun}s")
    }
}

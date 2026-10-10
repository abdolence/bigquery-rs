use clap::{Parser, Subcommand};
use smallquery::{DatasetRef, ProjectId, TableRef};
use std::net::SocketAddr;
use std::path::Path;
use std::str::FromStr;
use url::Url;

const LONG_ABOUT: &str = "\
A lightweight BigQuery engine over Apache Iceberg tables.

`serve` opens a root and serves the BigQuery v2 and Storage gRPC APIs on a loopback port. The \
other commands open the same root in process and print what its catalog holds; they run beside a \
serving engine.

  smq serve --root ./data --listen 127.0.0.1:9050
  smq datasets --root ./data
  smq tables --root ./data my-project.shop
  smq show --root ./data my-project.shop.orders

Logs go to standard error at the level RUST_LOG sets, `info` by default.";

#[derive(Parser, Debug)]
#[command(name = "smq", author, version, about, long_about = LONG_ABOUT)]
pub struct CliArgs {
    #[command(subcommand)]
    pub command: CliCommand,
}

#[derive(Subcommand, Debug)]
pub enum CliCommand {
    #[command(about = "Serve the BigQuery gRPC APIs from a root until Ctrl-C")]
    Serve {
        #[arg(long, help = ROOT_HELP)]
        root: EngineRoot,
        #[arg(
            long,
            default_value = "127.0.0.1:9050",
            help = "Loopback address to listen on; port 0 picks a free port"
        )]
        listen: SocketAddr,
    },
    #[command(about = "List the projects and datasets of a root, with their locations")]
    Datasets {
        #[arg(long, help = ROOT_HELP)]
        root: EngineRoot,
        #[arg(long, help = "Only the datasets of this project")]
        project: Option<ProjectId>,
    },
    #[command(about = "List the tables of a dataset")]
    Tables {
        #[arg(long, help = ROOT_HELP)]
        root: EngineRoot,
        #[arg(help = "The dataset, as <project>.<dataset>")]
        dataset: DatasetName,
    },
    #[command(about = "Show a table's schema, partitioning, clustering and pointer version")]
    Show {
        #[arg(long, help = ROOT_HELP)]
        root: EngineRoot,
        #[arg(help = "The table, as <project>.<dataset>.<table>")]
        table: TableName,
    },
}

const ROOT_HELP: &str = "The engine's root: a directory path or a file:// URL";

/// The root an engine opens, from a directory path or a URL.
#[derive(Debug, Clone)]
pub struct EngineRoot(Url);

impl EngineRoot {
    pub fn url(&self) -> &Url {
        &self.0
    }

    /// The directory of a `file://` root.
    pub fn local_dir(&self) -> Option<std::path::PathBuf> {
        (self.0.scheme() == "file")
            .then(|| self.0.to_file_path().ok())
            .flatten()
    }
}

impl FromStr for EngineRoot {
    type Err = String;

    fn from_str(root: &str) -> Result<Self, Self::Err> {
        if root.contains("://") {
            return Url::parse(root)
                .map(Self)
                .map_err(|err| format!("{root} is not a URL: {err}"));
        }
        let dir = std::path::absolute(Path::new(root))
            .map_err(|err| format!("{root} cannot be made absolute: {err}"))?;
        Url::from_directory_path(&dir)
            .map(Self)
            .map_err(|()| format!("{} is not a usable directory path", dir.display()))
    }
}

/// A dataset named `<project>.<dataset>`. A domain-scoped project ID holds `.` itself, and a
/// dataset ID never does, so the dataset is what follows the last `.`.
#[derive(Debug, Clone)]
pub struct DatasetName(DatasetRef);

impl FromStr for DatasetName {
    type Err = String;

    fn from_str(name: &str) -> Result<Self, Self::Err> {
        let (project, dataset) = name
            .rsplit_once('.')
            .ok_or_else(|| format!("{name} is not <project>.<dataset>"))?;
        Ok(Self(DatasetRef {
            project: project.parse().map_err(|err| format!("{err}"))?,
            dataset: dataset.parse().map_err(|err| format!("{err}"))?,
        }))
    }
}

impl From<DatasetName> for DatasetRef {
    fn from(name: DatasetName) -> Self {
        name.0
    }
}

/// A table named `<project>.<dataset>.<table>`; neither a dataset ID nor a table ID holds `.`.
#[derive(Debug, Clone)]
pub struct TableName(TableRef);

impl FromStr for TableName {
    type Err = String;

    fn from_str(name: &str) -> Result<Self, Self::Err> {
        let (dataset, table) = name
            .rsplit_once('.')
            .ok_or_else(|| format!("{name} is not <project>.<dataset>.<table>"))?;
        let DatasetName(dataset) = dataset.parse()?;
        Ok(Self(
            dataset.table(table.parse().map_err(|err| format!("{err}"))?),
        ))
    }
}

impl From<TableName> for TableRef {
    fn from(name: TableName) -> Self {
        name.0
    }
}

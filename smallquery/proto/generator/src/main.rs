//! Writes the tonic server traits of the six services `smallquery` serves into
//! `smallquery/src/proto/`, from the protos vendored in `smallquery/proto/googleapis`.
//!
//! Every message type the traits name is mapped onto gcloud-sdk's, so that the engine's
//! requests and responses are the very types the `bigquery` client sends; only the server
//! traits and their `*Server` wrappers are generated here.

use std::fs;
use std::path::{Path, PathBuf};

/// The protos that declare the served services. prost-build generates every file of their
/// import closure, so `model.proto` (imported by `job_stats.proto`) adds a
/// `model_service_server` that the engine never registers.
const SERVICE_PROTOS: &[&str] = &[
    "google/cloud/bigquery/v2/job.proto",
    "google/cloud/bigquery/v2/table.proto",
    "google/cloud/bigquery/v2/dataset.proto",
    "google/cloud/bigquery/v2/row_access_policy.proto",
    "google/cloud/bigquery/storage/v1/storage.proto",
];

/// Proto packages whose messages gcloud-sdk already provides. `.google.protobuf` is absent on
/// purpose: prost-build maps it itself and refuses a second entry, so its Rust path is set with
/// `prost_types_path` instead.
const EXTERN_PATHS: &[(&str, &str)] = &[
    (
        ".google.cloud.bigquery.v2",
        "::gcloud_sdk::google::cloud::bigquery::v2",
    ),
    (
        ".google.cloud.bigquery.storage.v1",
        "::gcloud_sdk::google::cloud::bigquery::storage::v1",
    ),
    (".google.rpc", "::gcloud_sdk::google::rpc"),
    (".google.api", "::gcloud_sdk::google::api"),
    // Without it `dataset.proto`'s import of `google/type/expr.proto` yields a second `Expr`.
    (".google.type", "::gcloud_sdk::google::r#type"),
];

fn main() {
    let proto_dir = Path::new(env!("CARGO_MANIFEST_DIR")).join("..");
    let include_root = proto_dir.join("googleapis");
    let out_dir = proto_dir.join("../src/proto");

    remove_generated_files(&out_dir);

    let mut config = tonic_prost_build::Config::new();
    config.prost_types_path("::gcloud_sdk::prost_types");

    let protos: Vec<PathBuf> = SERVICE_PROTOS
        .iter()
        .map(|proto| include_root.join(proto))
        .collect();

    EXTERN_PATHS
        .iter()
        .fold(
            tonic_prost_build::configure()
                .build_client(false)
                .build_server(true)
                // Unserved methods must answer `Unimplemented` naming the method, which the
                // default stubs do not; the engine writes every method out instead.
                .generate_default_stubs(false)
                .emit_rerun_if_changed(false)
                .out_dir(&out_dir),
            |builder, (proto_path, rust_path)| builder.extern_path(*proto_path, *rust_path),
        )
        .compile_with_config(config, &protos, &[include_root])
        .expect("protoc and tonic-prost-build compile the vendored protos");
}

/// Clears the previous output, so that a file no longer generated does not linger beside the
/// new ones.
fn remove_generated_files(out_dir: &Path) {
    fs::create_dir_all(out_dir).expect("the output directory is creatable");
    let entries = fs::read_dir(out_dir).expect("the output directory is readable");
    for entry in entries {
        let path = entry.expect("the output directory lists").path();
        if path.extension().is_some_and(|ext| ext == "rs") {
            fs::remove_file(&path).expect("a previous generated file is removable");
        }
    }
}

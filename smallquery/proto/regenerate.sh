#!/usr/bin/env bash
# Regenerates smallquery/src/proto/ from the protos vendored in proto/googleapis.
#
#   bash smallquery/proto/regenerate.sh
#
# Needs `protoc` on PATH (or PROTOC set). Honours CARGO_TARGET_DIR; without it the generator
# builds into proto/generator/target.
#
# The vendored protos are googleapis at commit 8503ac3ea32d7d6e0643a62322f69422ea272281, the
# commit gcloud-sdk's own BigQuery types were generated from, which they must stay: every message
# the traits name is gcloud-sdk's. The protobuf well-known types they import come with them.
# They are exactly the import closure of the generator's SERVICE_PROTOS. When gcloud-sdk moves
# to a newer googleapis commit, refresh them from a checkout of that commit and the matching
# protobuf `src` directory, then regenerate:
#
#   bash smallquery/proto/regenerate.sh vendor <googleapis-checkout> <protobuf-include-dir>
#
# and update the commit named above.
set -euo pipefail

proto_dir="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
vendored="$proto_dir/googleapis"

# Must match SERVICE_PROTOS in generator/src/main.rs.
service_protos=(
  google/cloud/bigquery/v2/job.proto
  google/cloud/bigquery/v2/table.proto
  google/cloud/bigquery/v2/dataset.proto
  google/cloud/bigquery/v2/row_access_policy.proto
  google/cloud/bigquery/storage/v1/storage.proto
)

vendor() {
  local googleapis="$1" protobuf="$2"
  local descriptors
  descriptors="$(mktemp)"
  trap 'rm -f "$descriptors"' RETURN

  "${PROTOC:-protoc}" -I "$googleapis" -I "$protobuf" --include_imports \
    --descriptor_set_out="$descriptors" "${service_protos[@]}"

  find "$vendored" -name '*.proto' -delete
  find "$vendored" -mindepth 1 -type d -empty -delete
  # Field 1 of each FileDescriptorProto in the set is its path relative to the include root.
  "${PROTOC:-protoc}" --decode_raw <"$descriptors" | sed -n 's/^  1: "\(.*\)"$/\1/p' |
    while read -r file; do
      local source="$googleapis/$file"
      [[ -f "$source" ]] || source="$protobuf/$file"
      mkdir -p "$vendored/$(dirname "$file")"
      cp "$source" "$vendored/$file"
    done
  cp "$googleapis/LICENSE" "$vendored/LICENSE"
}

case "${1:-}" in
  "") ;;
  vendor)
    [[ $# -eq 3 ]] || { echo "usage: $0 vendor <googleapis-checkout> <protobuf-include-dir>" >&2; exit 2; }
    vendor "$2" "$3"
    ;;
  *)
    echo "usage: $0 [vendor <googleapis-checkout> <protobuf-include-dir>]" >&2
    exit 2
    ;;
esac

cargo run --quiet --release --locked --manifest-path "$proto_dir/generator/Cargo.toml"

#!/usr/bin/env bash
# Builds the contenders and runs every benchmark once against a project:
#   bench-compare/run.sh --project my-project [--location europe-north2] [--runs 5]
# Raw results land in bench-compare/results/<run label>/results.json.
set -euo pipefail
here="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
export CARGO_TARGET_DIR="$here/target"
cargo build --release --manifest-path "$here/Cargo.toml" --bins
uv="${UV:-$(command -v uv || echo "$HOME/.local/bin/uv")}"
(cd "$here/python" && "$uv" sync --frozen --quiet)
exec "$here/python/.venv/bin/python" "$here/python/orchestrate.py" "$@"

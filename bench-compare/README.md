# Client benchmarks

Compares the `bigquery` crate with Google's own clients on the same machine, region and data:

- the official Rust crate `google-cloud-bigquery`;
- Python `google-cloud-bigquery` with `google-cloud-bigquery-storage` and `pyarrow`;
- the `bq` CLI, for query latency only.

This crate is not part of the published `bigquery` package and has its own workspace. The
results and the method are in the book, [Benchmarks](../docs/src/benchmarks.md).

## Running

You need:

- Rust (stable) and [uv](https://docs.astral.sh/uv/);
- application default credentials (`gcloud auth application-default login`) for a project
  where you can create datasets;
- optionally the `bq` CLI, found in `PATH` or through `BQ_BIN`.

```sh
bench-compare/run.sh --project my-project
```

The script builds the Rust contenders in release mode into `bench-compare/target`, syncs the
Python environment from `python/uv.lock`, and runs `python/orchestrate.py`, which:

- creates a scratch dataset `bench_compare_<time>` in `europe-north2` and the source tables
  from generated rows (`--location` changes the region);
- runs every scenario with 1 warm-up and 5 measured runs per client (`--runs`), one client at
  a time, rotating the client order each round;
- waits before each run until the machine is quiet, samples it during the run and repeats a
  run that was disturbed;
- deletes the dataset at the end, also when a run fails or is interrupted.

Raw results land in `bench-compare/results/<run label>/results.json`, with each client's
stderr next to it. `--only query_const,query_1k,query_200k_rows` runs a subset; the names are
the keys of `SCENARIOS` in `python/orchestrate.py`.

A full run takes about 35 minutes, most of it the official crate reading the 1M-row table over
REST. It costs a few cents: the generated-row queries bill 0 bytes, the scans and writes fall
under the Storage Read and Write free tiers, and only the official crate's `SELECT *` scan
bills the table size (about 215 MB per run).

## Layout

- `src/lib.rs`: the scenario SQL, the generated rows and the line protocol of the clients;
- `src/bin/ours.rs`: this crate, plus the setup, teardown and billing steps;
- `src/bin/official.rs`: `google-cloud-bigquery`;
- `python/client.py`: the Python client;
- `python/orchestrate.py`: runs everything and records the machine state.

"""The Python contender: google-cloud-bigquery with google-cloud-bigquery-storage and pyarrow.

Speaks the same line protocol as the Rust contenders (see ../src/lib.rs): one `ready` line,
then one JSON reply per request line, until stdin closes.

    python client.py --project P --dataset D --run-label L --location LOC
"""

import argparse
import json
import sys
import time
from importlib import metadata

from google.cloud import bigquery
from google.cloud import bigquery_storage
from google.cloud.bigquery import _pandas_helpers

LARGE_QUERY_ROWS = 200_000
SCAN_ROWS = 1_000_000
SCAN_TABLE = "scan_1m"
SQL_CONST = "SELECT 1 AS x"
SQL_1K = (
    "SELECT x, CONCAT('row_', CAST(x AS STRING)) AS s FROM UNNEST(GENERATE_ARRAY(1, 1000)) AS x"
)
SQL_200K = (
    "SELECT x, CONCAT('row_', CAST(x AS STRING)) AS s, x * 1.5 AS f, MOD(x, 2) = 0 AS b "
    f"FROM UNNEST(GENERATE_ARRAY(1, {LARGE_QUERY_ROWS})) AS x"
)


class PathProbe:
    """Records whether the client took the Storage Read path, and with how many streams.

    The client library decides by itself whether a result goes through Storage Read
    (`RowIterator._should_use_bqstorage`), so the harness wraps the two functions that only
    that path calls instead of guessing.
    """

    def __init__(self, bqs):
        self.storage = False
        self.streams = None
        original_download = _pandas_helpers.download_arrow_bqstorage

        def download(*args, **kwargs):
            self.storage = True
            return original_download(*args, **kwargs)

        _pandas_helpers.download_arrow_bqstorage = download
        original_session = bqs.create_read_session

        def create_read_session(*args, **kwargs):
            session = original_session(*args, **kwargs)
            self.streams = len(session.streams)
            return session

        bqs.create_read_session = create_read_session

    def reset(self):
        self.storage = False
        self.streams = None

    def path(self, fallback):
        return "storage_read" if self.storage else fallback


def main():
    parser = argparse.ArgumentParser()
    parser.add_argument("--project", required=True)
    parser.add_argument("--dataset", required=True)
    parser.add_argument("--run-label", default="manual")
    parser.add_argument("--location", default="europe-north2")
    args = parser.parse_args()

    client = bigquery.Client(project=args.project, location=args.location)
    bqs = bigquery_storage.BigQueryReadClient()
    probe = PathProbe(bqs)
    table = f"{args.project}.{args.dataset}.{SCAN_TABLE}"

    def job_config():
        return bigquery.QueryJobConfig(
            use_query_cache=False,
            labels={"bench_client": "python", "bench_run": args.run_label},
        )

    # query_and_wait (jobs.query) is the library's recommended way to run a query and read
    # its rows; the older query() + result() pair inserts a job first and costs a round trip.
    def query_rows(sql):
        start = time.perf_counter()
        it = client.query_and_wait(sql, job_config=job_config(), location=args.location)
        rows = list(it)
        secs = time.perf_counter() - start
        return {
            "secs": secs,
            "rows": len(rows),
            "path": probe.path("rest_json_pages"),
            "extra": {"job_created": it.job_id is not None},
        }

    def query_arrow(sql):
        start = time.perf_counter()
        it = client.query_and_wait(sql, job_config=job_config(), location=args.location)
        arrow = it.to_arrow(bqstorage_client=bqs)
        secs = time.perf_counter() - start
        return {
            "secs": secs,
            "rows": arrow.num_rows,
            "path": probe.path("rest_json_pages"),
            "streams": probe.streams,
            "extra": {"arrow_memory_bytes": arrow.nbytes, "job_created": it.job_id is not None},
        }

    def scan(kind):
        start = time.perf_counter()
        rows = client.list_rows(table)
        if kind == "arrow":
            result = rows.to_arrow(bqstorage_client=bqs)
            n, extra = result.num_rows, {"arrow_memory_bytes": result.nbytes}
        else:
            result = rows.to_dataframe(bqstorage_client=bqs)
            n, extra = len(result), {"form": "pandas.DataFrame"}
        secs = time.perf_counter() - start
        return {
            "secs": secs,
            "rows": n,
            "path": probe.path("rest_tabledata_list"),
            "streams": probe.streams,
            "extra": extra,
        }

    scenarios = {
        "query_const": lambda: query_rows(SQL_CONST),
        "query_1k": lambda: query_rows(SQL_1K),
        "query_200k_rows": lambda: query_rows(SQL_200K),
        "query_200k_arrow": lambda: query_arrow(SQL_200K),
        "scan_rows": lambda: scan("dataframe"),
        "scan_arrow": lambda: scan("arrow"),
    }
    not_available = {
        "write": "n/a: the Python Storage Write helper (AppendRowsStream) sends AppendRowsRequest "
        "messages the caller builds, protobuf rows with a hand-made descriptor; it has no "
        "row-object writer",
        "decode": "n/a: the in-process decode scenario compares the Rust clients only",
    }

    info = {
        "client": "python",
        "python": sys.version.split()[0],
        "packages": {
            name: metadata.version(name)
            for name in [
                "google-cloud-bigquery",
                "google-cloud-bigquery-storage",
                "pyarrow",
                "pandas",
                "grpcio",
                "db-dtypes",
            ]
        },
        "settings": "client defaults: queries through query_and_wait (jobs.query); Storage Read "
        "with max_stream_count 0 (BigQuery decides), LZ4, one thread per stream",
    }
    print(json.dumps({"ready": True, "info": info}), flush=True)
    for line in sys.stdin:
        request = json.loads(line)
        name = request["scenario"]
        probe.reset()
        try:
            if name in not_available:
                raise RuntimeError(not_available[name])
            reply = scenarios[name]()
        except Exception as err:  # reported to the orchestrator, which records it
            message = str(err)
            reply = {"error": message if message.startswith("n/a") else f"{type(err).__name__}: {err}"}
        print(json.dumps(reply), flush=True)


if __name__ == "__main__":
    main()

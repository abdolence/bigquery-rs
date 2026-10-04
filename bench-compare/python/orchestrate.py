"""Runs every contender on every scenario, one client at a time, and writes the raw results.

The clients are long-lived processes that answer one request per measured run (see
../src/lib.rs). For each scenario the orchestrator runs round 0 (the warm-up) and then the
measured rounds; inside a round it runs each client once, rotating the client order from round
to round, so drift in the machine or the network spreads over all contenders alike.

Every run is gated on the machine being quiet: before the run, the 1-minute load average and a
3 s snapshot of /proc; during the run, a /proc snapshot every 2 s. The load average is not
checked during a run, since the run itself raises it. "Foreign" below means every process
that is not this orchestrator or one of its descendants.

    python orchestrate.py --project P [--location LOC] [--runs 5] [--only A,B] [--out DIR]
"""

import argparse
import json
import os
import platform
import re
import shutil
import signal
import statistics
import subprocess
import sys
import threading
import time
from pathlib import Path

HERE = Path(__file__).resolve().parent
ROOT = HERE.parent
REPO = ROOT.parent
NPROC = os.cpu_count() or 1
TICK = os.sysconf("SC_CLK_TCK")

# The quiet rule: foreign CPU below 10% of the cores, no foreign process above 20% of one
# core, no compiler or linker from another session, and little foreign network traffic.
QUIET_FOREIGN_CORES = 0.10 * NPROC
QUIET_PROCESS_PCT = 20.0
QUIET_NET_BYTES_PER_SEC = 2_000_000
COMPILERS = {
    "rustc", "cargo", "cc", "c++", "gcc", "g++", "clang", "clang++", "ld", "ld.lld", "lld",
    "rust-lld", "mold", "cc1", "cc1plus", "collect2", "build-script-b", "rustdoc", "clippy-driver",
}
QUIET_WAIT_STEP = 30
QUIET_WAIT_MAX = 15 * 60
SAMPLE_EVERY = 2.0
MAX_ATTEMPTS = 4

SCENARIOS = [
    ("query_const", ["ours", "ours_required", "official", "python", "bq"]),
    ("query_1k", ["ours", "ours_required", "official", "python", "bq"]),
    ("query_200k_rows", ["ours", "ours_required", "official", "python"]),
    ("query_200k_arrow", ["ours", "official", "python"]),
    ("scan_rows", ["ours", "official", "python"]),
    ("scan_arrow", ["ours", "official", "python"]),
    ("write", ["ours", "official", "python"]),
    ("decode", ["ours", "official"]),
]


def log(msg):
    print(f"[{time.strftime('%H:%M:%S')}] {msg}", file=sys.stderr, flush=True)


# ---------------------------------------------------------------- machine state


def read_procs():
    """{pid: (ppid, comm, cmdline head, utime+stime ticks)} for every live process."""
    procs = {}
    for entry in os.scandir("/proc"):
        if not entry.name.isdigit():
            continue
        try:
            with open(f"/proc/{entry.name}/stat", "rb") as f:
                raw = f.read().decode(errors="replace")
        except OSError:
            continue
        lpar, rpar = raw.find("("), raw.rfind(")")
        comm = raw[lpar + 1 : rpar]
        fields = raw[rpar + 2 :].split()
        procs[int(entry.name)] = (int(fields[1]), comm, int(fields[11]) + int(fields[12]))
    return procs


def own_pids(procs):
    """This process and all of its descendants."""
    children = {}
    for pid, (ppid, _, _) in procs.items():
        children.setdefault(ppid, []).append(pid)
    ours, todo = set(), [os.getpid()]
    while todo:
        pid = todo.pop()
        ours.add(pid)
        todo.extend(children.get(pid, []))
    return ours


def cpu_busy_ticks():
    """user + nice + system ticks of all CPUs; interrupt time is left out, since the
    harness's own network transfers raise softirq time."""
    with open("/proc/stat") as f:
        fields = f.readline().split()[1:]
    return int(fields[0]) + int(fields[1]) + int(fields[2])


def net_bytes():
    total = 0
    with open("/proc/net/dev") as f:
        for line in f.readlines()[2:]:
            name, data = line.split(":", 1)
            if name.strip() == "lo":
                continue
            parts = data.split()
            total += int(parts[0]) + int(parts[8])
    return total


def loadavg():
    with open("/proc/loadavg") as f:
        return [float(x) for x in f.read().split()[:3]]


def cpu_freq():
    freqs, governors = [], set()
    for cpu in Path("/sys/devices/system/cpu").glob("cpu[0-9]*"):
        try:
            freqs.append(int((cpu / "cpufreq/scaling_cur_freq").read_text()) // 1000)
            governors.add((cpu / "cpufreq/scaling_governor").read_text().strip())
        except OSError:
            pass
    if not freqs:
        return None
    return {"mhz_min": min(freqs), "mhz_max": max(freqs), "governors": sorted(governors)}


def mem_available_mb():
    with open("/proc/meminfo") as f:
        for line in f:
            if line.startswith("MemAvailable:"):
                return int(line.split()[1]) // 1024
    return None


class Window:
    """CPU and network use between two snapshots, split into ours and foreign."""

    def __init__(self):
        self.t = time.monotonic()
        self.busy = cpu_busy_ticks()
        self.procs = read_procs()
        self.net = net_bytes()

    def until_now(self):
        now = Window()
        dt = max(now.t - self.t, 1e-6)
        ours = own_pids(now.procs) | own_pids(self.procs)
        ours_ticks, foreign = 0, []
        for pid, (_, comm, ticks) in now.procs.items():
            before = self.procs.get(pid)
            delta = ticks - before[2] if before else ticks
            if pid in ours:
                ours_ticks += delta
            elif delta > 0:
                foreign.append((delta / TICK / dt * 100.0, pid, comm))
        foreign.sort(reverse=True)
        foreign_cores = max(0.0, (now.busy - self.busy - ours_ticks) / TICK / dt)
        compilers = sorted(
            {comm for pid, (_, comm, _) in now.procs.items() if pid not in ours and comm in COMPILERS}
        )
        state = {
            "secs": round(dt, 2),
            "foreign_cores": round(foreign_cores, 2),
            "ours_cores": round(ours_ticks / TICK / dt, 2),
            "top_foreign": [
                {"pct": round(p, 1), "pid": pid, "comm": c} for p, pid, c in foreign[:5]
            ],
            "foreign_compilers": compilers,
            "net_bytes_per_sec": round((now.net - self.net) / dt),
        }
        return now, state


def violations(state, with_net):
    out = []
    if state["foreign_cores"] >= QUIET_FOREIGN_CORES:
        out.append(f"foreign CPU {state['foreign_cores']} cores")
    top = state["top_foreign"]
    if top and top[0]["pct"] >= QUIET_PROCESS_PCT:
        out.append(f"{top[0]['comm']} ({top[0]['pid']}) at {top[0]['pct']}%")
    if state["foreign_compilers"]:
        out.append("compiler running: " + ",".join(state["foreign_compilers"]))
    if with_net and state["net_bytes_per_sec"] >= QUIET_NET_BYTES_PER_SEC:
        out.append(f"network {state['net_bytes_per_sec']} B/s while the harness was idle")
    return out


def preflight():
    """One 3 s snapshot of the machine while the harness is idle."""
    start = Window()
    time.sleep(3)
    _, state = start.until_now()
    state["loadavg"] = loadavg()
    state["nproc"] = NPROC
    state["cpufreq"] = cpu_freq()
    state["mem_available_mb"] = mem_available_mb()
    state["violations"] = violations(state, with_net=True)
    if state["loadavg"][0] >= QUIET_FOREIGN_CORES:
        state["violations"].append(f"1-minute load {state['loadavg'][0]}")
    return state


def wait_quiet():
    """Waits in 30 s steps, up to 15 minutes, for a quiet preflight. Returns the last
    snapshot, the seconds waited and whether the machine stayed busy throughout."""
    waited = 0
    while True:
        state = preflight()
        if not state["violations"]:
            return state, waited, False
        if waited >= QUIET_WAIT_MAX:
            return state, waited, True
        log(f"  machine busy ({'; '.join(state['violations'])}), waiting {QUIET_WAIT_STEP}s")
        time.sleep(QUIET_WAIT_STEP)
        waited += QUIET_WAIT_STEP


class Sampler(threading.Thread):
    """Samples the machine every 2 s while a run is in flight."""

    def __init__(self):
        super().__init__(daemon=True)
        self.stop_flag = threading.Event()
        self.samples = []

    def run(self):
        window = Window()
        while not self.stop_flag.wait(SAMPLE_EVERY):
            window, state = window.until_now()
            state["loadavg1"] = loadavg()[0]
            self.samples.append(state)

    def finish(self):
        self.stop_flag.set()
        self.join()
        bad = [(s, violations(s, with_net=False)) for s in self.samples]
        bad = [{"sample": s, "violations": v} for s, v in bad if v]
        summary = {
            "samples": len(self.samples),
            "max_foreign_cores": max((s["foreign_cores"] for s in self.samples), default=None),
            "max_foreign_process": max(
                (s["top_foreign"][0] for s in self.samples if s["top_foreign"]),
                key=lambda p: p["pct"],
                default=None,
            ),
            "max_ours_cores": max((s["ours_cores"] for s in self.samples), default=None),
            "max_loadavg1": max((s["loadavg1"] for s in self.samples), default=None),
        }
        return summary, bad


# ---------------------------------------------------------------- clients


class LineClient:
    """A long-lived contender process speaking the request/reply line protocol."""

    def __init__(self, name, argv, stderr_path):
        self.name = name
        self.stderr = open(stderr_path, "w")
        self.proc = subprocess.Popen(
            argv, stdin=subprocess.PIPE, stdout=subprocess.PIPE, stderr=self.stderr, text=True
        )
        ready = json.loads(self.proc.stdout.readline())
        if not ready.get("ready"):
            raise RuntimeError(f"{name} did not start: {ready}")
        self.info = ready["info"]

    def run(self, scenario, run):
        self.proc.stdin.write(json.dumps({"scenario": scenario, "run": run}) + "\n")
        self.proc.stdin.flush()
        line = self.proc.stdout.readline()
        if not line:
            raise RuntimeError(f"{self.name} exited")
        return json.loads(line)

    def close(self):
        if self.proc.poll() is None:
            self.proc.stdin.close()
            try:
                self.proc.wait(timeout=30)
            except subprocess.TimeoutExpired:
                self.proc.kill()
        self.stderr.close()


class BqClient:
    """The bq CLI, one process per run, timed from outside since bq reports no timings.

    Each run also times `bq version`, so the process start-up can be told apart from the
    query itself."""

    SQL = {
        "query_const": "SELECT 1 AS x",
        "query_1k": "SELECT x, CONCAT('row_', CAST(x AS STRING)) AS s "
        "FROM UNNEST(GENERATE_ARRAY(1, 1000)) AS x",
    }

    def __init__(self, bq, project, location, run_label):
        self.name = "bq"
        self.bq = bq
        self.project = project
        self.location = location
        self.run_label = run_label
        out = subprocess.run([bq, "version"], capture_output=True, text=True, check=True)
        self.info = {
            "client": "bq",
            "version": out.stdout.strip().splitlines()[-1],
            "path": bq,
            "settings": "bq query --nouse_legacy_sql --nouse_cache --format=json, "
            "output read in full; time measured around the process",
        }

    def run(self, scenario, run):
        if scenario not in self.SQL:
            return {"error": "n/a: bq is measured for query latency only"}
        start = time.perf_counter()
        subprocess.run([self.bq, "version"], capture_output=True, check=True)
        startup = time.perf_counter() - start
        argv = [
            self.bq, f"--project_id={self.project}", f"--location={self.location}",
            "--format=json", "--quiet", "query", "--nouse_legacy_sql", "--nouse_cache",
            "--max_rows=1000000", "--label=bench_client:bq", f"--label=bench_run:{self.run_label}",
            self.SQL[scenario],
        ]
        start = time.perf_counter()
        out = subprocess.run(argv, capture_output=True, text=True)
        secs = time.perf_counter() - start
        if out.returncode != 0:
            return {"error": out.stderr.strip()[-500:]}
        rows = len(json.loads(out.stdout))
        return {
            "secs": secs,
            "rows": rows,
            "path": "bq_cli_rest",
            "extra": {"process_start_secs": startup},
        }

    def close(self):
        pass


# ---------------------------------------------------------------- record keeping


def sh(argv):
    try:
        return subprocess.run(argv, capture_output=True, text=True, timeout=60).stdout.strip()
    except (OSError, subprocess.TimeoutExpired) as err:
        return f"unavailable: {err}"


def lock_versions(names):
    lock = (ROOT / "Cargo.lock").read_text()
    found = {}
    for block in lock.split("[[package]]"):
        name = re.search(r'^name = "([^"]+)"', block, re.M)
        version = re.search(r'^version = "([^"]+)"', block, re.M)
        if name and version and name.group(1) in names:
            found.setdefault(name.group(1), []).append(version.group(1))
    return found


def network_baseline():
    """TCP and TLS set-up times to the global endpoints every client uses, five times each."""
    out = {}
    for host in ["bigquery.googleapis.com", "bigquerystorage.googleapis.com"]:
        times = []
        for _ in range(5):
            r = sh([
                "curl", "-s", "-o", "/dev/null", "-w", "%{time_connect} %{time_appconnect}",
                f"https://{host}/",
            ])
            try:
                connect, tls = (float(x) for x in r.split())
                times.append({"tcp_connect": connect, "tls_done": tls})
            except ValueError:
                times.append({"error": r})
        out[host] = {
            "address": sh(["getent", "ahosts", host]).splitlines()[0:1],
            "runs": times,
        }
    return out


def summarise(runs, logical_scan_bytes):
    """Median and spread per scenario and client, over accepted measured runs only."""
    table = {}
    for r in runs:
        if r["warmup"] or "outcome" not in r or "error" in r["outcome"]:
            continue
        table.setdefault(r["scenario"], {}).setdefault(r["client"], []).append(r)
    out = {}
    for scenario, clients in table.items():
        out[scenario] = {}
        for client, rs in clients.items():
            secs = [r["outcome"]["secs"] for r in rs]
            med = statistics.median(secs)
            o = rs[0]["outcome"]
            entry = {
                "runs": len(rs),
                "contended_runs": sum(1 for r in rs if r["contended"]),
                "median_secs": med,
                "min_secs": min(secs),
                "max_secs": max(secs),
                "rows": o["rows"],
                "rows_per_sec": o["rows"] / med if med else None,
                "path": o.get("path"),
                "streams": sorted({r["outcome"].get("streams") for r in rs} - {None}),
            }
            if scenario.startswith("scan") or scenario == "decode":
                entry["logical_mb_per_sec"] = logical_scan_bytes / med / 1e6
            sent = [r["outcome"].get("bytes") for r in rs if r["outcome"].get("bytes")]
            if scenario == "write" and sent:
                entry["mb_sent_per_run"] = statistics.median(sent) / 1e6
                entry["sent_mb_per_sec"] = statistics.median(sent) / med / 1e6
            out[scenario][client] = entry
    return out


# ---------------------------------------------------------------- main


def main():
    parser = argparse.ArgumentParser()
    parser.add_argument("--project", required=True)
    parser.add_argument("--location", default="europe-north2")
    parser.add_argument("--runs", type=int, default=5)
    parser.add_argument(
        "--scenarios", "--only", dest="scenarios", default=",".join(s for s, _ in SCENARIOS),
        help="comma-separated subset, e.g. query_const,query_1k,query_200k_rows",
    )
    parser.add_argument("--out", default=str(ROOT / "results"))
    parser.add_argument("--bq", default=os.environ.get("BQ_BIN") or shutil.which("bq"))
    args = parser.parse_args()
    # A plain `kill` still runs the teardown in the `finally` below.
    signal.signal(signal.SIGTERM, lambda *_: sys.exit(1))

    label = time.strftime("p%Y%m%d_%H%M%S")
    dataset = f"bench_compare_{label[1:]}"
    out_dir = Path(args.out) / label
    out_dir.mkdir(parents=True, exist_ok=True)
    ours_bin = str(ROOT / "target/release/ours")
    official_bin = str(ROOT / "target/release/official")
    common = ["--project", args.project, "--dataset", dataset, "--run-label", label,
              "--location", args.location]

    result = {
        "run_label": label,
        "dataset": dataset,
        "location": args.location,
        "started": time.strftime("%Y-%m-%dT%H:%M:%S%z"),
        "method": {
            "runs_per_scenario": args.runs,
            "warmup_runs": 1,
            "order": "round-robin per run, client order rotated each round",
            "quiet_rule": {
                "foreign_cores_below": QUIET_FOREIGN_CORES,
                "foreign_process_pct_below": QUIET_PROCESS_PCT,
                "no_foreign_compiler": sorted(COMPILERS),
                "idle_network_bytes_per_sec_below": QUIET_NET_BYTES_PER_SEC,
                "wait": f"{QUIET_WAIT_STEP}s steps up to {QUIET_WAIT_MAX}s",
                "during_run": f"sampled every {SAMPLE_EVERY}s; a run with a violating sample "
                f"is discarded and repeated, up to {MAX_ATTEMPTS} attempts",
            },
            "query_cache": "off for every client",
            "endpoints": "global defaults for every client: bigquery.googleapis.com "
            "(this crate over gRPC; the official crate, Python and bq over REST) and "
            "bigquerystorage.googleapis.com (gRPC)",
        },
        "machine": {
            "cpu": sh(["sh", "-c", "lscpu | grep 'Model name' | sed 's/.*: *//'"]),
            "nproc": NPROC,
            "mem": sh(["sh", "-c", "free -m | sed -n 2p"]),
            "kernel": platform.release(),
            "cpufreq": cpu_freq(),
            "location_note": "Sweden, home connection",
        },
        "versions": {
            "repo_commit": sh(["git", "-C", str(REPO), "rev-parse", "HEAD"]),
            "repo_dirty": bool(sh(["git", "-C", str(REPO), "status", "--porcelain", "--", "src", "Cargo.toml"])),
            "rustc": sh(["rustc", "-V"]),
            "cargo_lock": lock_versions({
                "bigquery", "google-cloud-bigquery", "google-cloud-bigquery-v2",
                "google-cloud-gax", "google-cloud-gax-internal", "google-cloud-auth",
                "gcloud-sdk", "tonic", "arrow-ipc", "arrow-array", "reqwest", "hyper",
            }),
        },
        "network": {"baseline": network_baseline()},
        "runs": [],
        "not_applicable": {},
    }

    def save():
        (out_dir / "results.json").write_text(json.dumps(result, indent=1))

    log(f"setup {dataset} in {args.location}")
    setup = subprocess.run([ours_bin, *common, "setup"], capture_output=True, text=True)
    if setup.returncode != 0:
        log(setup.stderr)
        sys.exit(1)
    result["setup"] = json.loads(setup.stdout)
    logical = result["setup"]["scan_table_logical_bytes"]
    clients = {}
    try:
        ping = subprocess.run([ours_bin, *common, "ping"], capture_output=True, text=True)
        result["network"]["datasets_get"] = json.loads(ping.stdout) if ping.returncode == 0 else ping.stderr
        clients["ours"] = LineClient("ours", [ours_bin, *common], out_dir / "ours.stderr")
        clients["ours_required"] = LineClient(
            "ours_required", [ours_bin, *common, "--job-creation-required"],
            out_dir / "ours_required.stderr",
        )
        clients["official"] = LineClient("official", [official_bin, *common], out_dir / "official.stderr")
        clients["python"] = LineClient(
            "python", [str(HERE / ".venv/bin/python"), str(HERE / "client.py"), *common],
            out_dir / "python.stderr",
        )
        if args.bq:
            clients["bq"] = BqClient(args.bq, args.project, args.location, label)
        else:
            result["not_applicable"]["bq"] = "bq CLI not found"
        result["clients"] = {name: c.info for name, c in clients.items()}
        save()

        wanted = args.scenarios.split(",")
        for scenario, names in SCENARIOS:
            if scenario not in wanted:
                continue
            names = [n for n in names if n in clients]
            active = list(names)
            for run in range(args.runs + 1):
                order = active[run % len(active):] + active[: run % len(active)] if active else []
                for name in order:
                    record = measure(clients[name], scenario, run)
                    outcome = record.get("outcome", {})
                    if str(outcome.get("error", "")).startswith("n/a"):
                        result["not_applicable"].setdefault(name, {})[scenario] = outcome["error"]
                        active.remove(name)
                        log(f"{scenario} {name}: {outcome['error']}")
                        continue
                    result["runs"].append(record)
                    save()
        result["summary"] = summarise(result["runs"], logical)
        save()
    finally:
        for c in clients.values():
            c.close()
        billing = subprocess.run([ours_bin, *common, "billing"], capture_output=True, text=True)
        result["billing"] = json.loads(billing.stdout) if billing.returncode == 0 else billing.stderr
        teardown = subprocess.run([ours_bin, *common, "teardown"], capture_output=True, text=True)
        result["teardown"] = json.loads(teardown.stdout) if teardown.returncode == 0 else teardown.stderr
        result["finished"] = time.strftime("%Y-%m-%dT%H:%M:%S%z")
        save()
        log(f"results in {out_dir / 'results.json'}")


def measure(client, scenario, run):
    """One run of one client, repeated while the machine breaks the quiet rule."""
    discarded = []
    for attempt in range(1, MAX_ATTEMPTS + 1):
        pre, waited, busy = wait_quiet()
        sampler = Sampler()
        sampler.start()
        net_before = net_bytes()
        started = time.strftime("%H:%M:%S")
        try:
            outcome = client.run(scenario, run)
        except Exception as err:  # a crashed client is recorded, not fatal to the others
            outcome = {"error": f"{type(err).__name__}: {err}"}
        summary, bad = sampler.finish()
        record = {
            "scenario": scenario,
            "client": client.name,
            "run": run,
            "warmup": run == 0,
            "attempt": attempt,
            "started": started,
            "preflight": pre,
            "waited_for_quiet_secs": waited,
            "during": summary,
            "net_bytes_during": net_bytes() - net_before,
            "outcome": outcome,
            "contended": busy,
            "discarded_attempts": discarded,
        }
        secs = outcome.get("secs")
        log(
            f"{scenario} {client.name} run {run} attempt {attempt}: "
            f"{f'{secs:.3f}s' if secs is not None else outcome.get('error', '')[:120]}"
            f"{' CONTENDED' if busy else ''}{' (sample violations)' if bad else ''}"
        )
        if not bad or "error" in outcome:
            return record
        discarded.append({"attempt": attempt, "secs": secs, "violations": bad})
    record["contended"] = True
    return record


if __name__ == "__main__":
    main()

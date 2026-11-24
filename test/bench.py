#!/usr/bin/env python3
"""Reproducible throughput/latency benchmark: lite-redis vs real Redis.

    HOST_CPU="$(sysctl -n machdep.cpu.brand_string)" ./dev make bench

Method (each choice exists to make the comparison fair and repeatable):
  * Both servers run on the same machine, each pinned to ONE core (core 0):
    lite-redis is single-threaded by design, and pinning stops the scheduler
    from migrating it (a source of noise). Real Redis is pinned the same way.
  * redis-benchmark runs on OTHER cores (2-7), with --threads 2. With the
    default single-threaded client, redis-benchmark itself is the bottleneck
    (the server sits at ~60% CPU), so you'd be measuring the client. Two
    client threads saturate one server core (~95% CPU).
  * Every workload runs REPS times, alternating which server goes first,
    and we report the median plus the min-max range.
  * Server CPU time is sampled from /proc/<pid>/stat around each run and
    split into user (our code) vs kernel (syscalls, TCP stack).
  * We also run the classic default-client invocation, labeled as
    client-bound, because that's what most people quote.
  * Every run lasts ~8-12 s. redis-benchmark only notices completion on a
    coarse timer in --threads mode (~250 ms), so short runs quantize the
    result (e.g. 2M requests in "exactly" 4.50 s); long runs keep that
    error under ~2.5%.

Writes results/benchmark.md.
"""
import csv
import io
import os
import platform
import shutil
import statistics
import subprocess
import sys
import time

LITE_PORT, REDIS_PORT = 6430, 6431
REPS = int(os.environ.get("REPS", "3"))
SERVER_CORE = "0"
CLIENT_CORES = "2-7"
TICK = os.sysconf("SC_CLK_TCK")

# (label, redis-benchmark args, description)
WORKLOADS = [
    ("default client (client-bound)", ["-t", "set,get", "-n", "2500000", "-c", "50"],
     "plain `redis-benchmark -t set,get -c 50`: one client thread, which can't saturate either server"),
    ("saturated, no pipelining", ["-t", "set,get", "-n", "5000000", "-c", "50", "--threads", "2"],
     "50 connections, 1 request in flight each, client with 2 threads"),
    ("pipelined -P 16", ["-t", "set,get", "-n", "30000000", "-c", "50", "-P", "16", "--threads", "2"],
     "16 requests per round trip: syscall cost amortized over 16 commands"),
    ("1 KB values", ["-t", "set,get", "-n", "4000000", "-c", "50", "-d", "1024", "--threads", "2"],
     "1024-byte values instead of 3 bytes"),
    ("1000 clients", ["-t", "set,get", "-n", "4000000", "-c", "1000", "--threads", "2"],
     "1000 concurrent connections on one thread"),
    ("SET with TTL, 1M random keys, -P 16",
     ["-n", "8000000", "-r", "1000000", "-c", "50", "-P", "16", "--threads", "2",
      "SET", "key:__rand_int__", "xxx", "EX", "100"],
     "every write inserts/updates the expiry heap (lite) / expires dict (Redis)"),
]


def sh(cmd):
    return subprocess.run(cmd, capture_output=True, text=True).stdout.strip()


def cpu_ticks(pid):
    fields = open(f"/proc/{pid}/stat").read().rsplit(")", 1)[1].split()
    return int(fields[11]), int(fields[12])  # utime, stime


def wait_ready(port):
    for _ in range(100):
        if sh(["redis-cli", "-p", str(port), "PING"]) == "PONG":
            return
        time.sleep(0.1)
    sys.exit(f"server on port {port} did not start")


def run_benchmark(port, pid, args):
    """One redis-benchmark run. Returns {test: {...}} plus server CPU split."""
    u0, s0 = cpu_ticks(pid)
    t0 = time.monotonic()
    out = subprocess.run(["taskset", "-c", CLIENT_CORES, "redis-benchmark", "-p", str(port),
                          "--csv"] + args, capture_output=True, text=True, timeout=600).stdout
    wall = time.monotonic() - t0
    u1, s1 = cpu_ticks(pid)
    rows = {}
    for row in csv.DictReader(io.StringIO(out)):
        name = row["test"].split(" ")[0]  # "SET key:__rand_int__ ..." -> "SET"
        rows[name] = {k: float(row[k]) for k in ("rps", "p50_latency_ms", "p99_latency_ms")}
    if not rows:
        sys.exit(f"redis-benchmark produced no results for {args}:\n{out}")
    user = (u1 - u0) / TICK / wall * 100
    kern = (s1 - s0) / TICK / wall * 100
    return rows, user, kern


def fmt_rps(x):
    return f"{x / 1e6:.2f}M" if x >= 1e6 else f"{x / 1e3:.0f}k"


def main():
    if not shutil.which("taskset"):
        sys.exit("taskset missing")
    lite = subprocess.Popen(["taskset", "-c", SERVER_CORE, "./lite-redis-server",
                             "--port", str(LITE_PORT)],
                            stdout=subprocess.DEVNULL, stderr=subprocess.DEVNULL)
    redis = subprocess.Popen(["taskset", "-c", SERVER_CORE, "redis-server", "--port",
                              str(REDIS_PORT), "--save", "", "--appendonly", "no"],
                             stdout=subprocess.DEVNULL, stderr=subprocess.DEVNULL)
    try:
        wait_ready(LITE_PORT)
        wait_ready(REDIS_PORT)
        servers = {"lite-redis": (LITE_PORT, lite.pid), "redis": (REDIS_PORT, redis.pid)}
        results = {}  # (workload, server) -> list of (rows, user, kern)
        for wi, (label, args, _) in enumerate(WORKLOADS):
            for rep in range(REPS):
                order = ["lite-redis", "redis"] if (rep + wi) % 2 == 0 else ["redis", "lite-redis"]
                for name in order:
                    port, pid = servers[name]
                    r = run_benchmark(port, pid, args)
                    results.setdefault((label, name), []).append(r)
                    tests = " ".join(f"{t} {fmt_rps(v['rps'])}" for t, v in r[0].items())
                    print(f"[{label}] rep {rep + 1}/{REPS} {name:10s} {tests}  "
                          f"(server CPU {r[1] + r[2]:.0f}%: user {r[1]:.0f}%, kernel {r[2]:.0f}%)",
                          flush=True)
    finally:
        lite.terminate()
        redis.terminate()
        lite.wait()
        redis.wait()

    # ---- report --------------------------------------------------------------
    host = os.environ.get("HOST_CPU", "unknown host CPU")
    lines = [
        "# lite-redis benchmark results",
        "",
        f"- Generated by `test/bench.py` (`make bench`), {time.strftime('%Y-%m-%d %H:%M UTC', time.gmtime())}",
        f"- Host: {host}; Docker Desktop Linux VM ({platform.machine()}, kernel {platform.release()}, "
        f"{os.cpu_count()} vCPUs)",
        f"- Servers: lite-redis (-O2) vs {sh(['redis-server', '--version']).split(' sha')[0]}, "
        f"each pinned to core {SERVER_CORE}, no persistence",
        f"- Client: redis-benchmark (same version) pinned to cores {CLIENT_CORES}, over loopback",
        f"- Each cell: **median of {REPS} runs** (min–max in brackets), runs alternating between servers",
        "- Latencies are redis-benchmark's per-request p50/p99 in ms",
        "- Each run lasts ~8-12 s (redis-benchmark's completion timer is coarse; short runs quantize the result)",
        "- Server CPU: lite-redis's CPU time during the run, split into user space (our code) and kernel (syscalls, TCP)",
        "",
        "| Workload | Test | lite-redis ops/s | Redis ops/s | lite / Redis | lite p50 / p99 ms | Redis p50 / p99 ms | lite CPU (user + kernel) |",
        "|---|---|---|---|---|---|---|---|",
    ]
    for label, _, desc in WORKLOADS:
        lr = results[(label, "lite-redis")]
        rr = results[(label, "redis")]
        for test in lr[0][0]:
            l_rps = [r[0][test]["rps"] for r in lr]
            r_rps = [r[0][test]["rps"] for r in rr]
            lm, rm = statistics.median(l_rps), statistics.median(r_rps)
            lp50 = statistics.median(r[0][test]["p50_latency_ms"] for r in lr)
            lp99 = statistics.median(r[0][test]["p99_latency_ms"] for r in lr)
            rp50 = statistics.median(r[0][test]["p50_latency_ms"] for r in rr)
            rp99 = statistics.median(r[0][test]["p99_latency_ms"] for r in rr)
            cu = statistics.median(r[1] for r in lr)
            ck = statistics.median(r[2] for r in lr)
            lines.append(
                f"| {label} | {test} | **{fmt_rps(lm)}** [{fmt_rps(min(l_rps))}–{fmt_rps(max(l_rps))}] "
                f"| {fmt_rps(rm)} [{fmt_rps(min(r_rps))}–{fmt_rps(max(r_rps))}] | {lm / rm:.2f}× "
                f"| {lp50:.3f} / {lp99:.3f} | {rp50:.3f} / {rp99:.3f} | {cu:.0f}% + {ck:.0f}% |")
    lines += ["", "Workloads:", ""]
    lines += [f"- **{label}**: {desc}; `redis-benchmark {' '.join(args)}`" for label, args, desc in WORKLOADS]
    lines += [
        "",
        "Notes:",
        "",
        "- With the default single-threaded client the servers sit well below 100% CPU: that row measures",
        "  redis-benchmark, not the server. The saturated rows are the meaningful comparison.",
        "- Most of lite-redis's CPU time is in the kernel (`read`/`write`/`epoll_wait` and the TCP stack);",
        "  command execution itself is a small fraction. Pipelining is fast because it amortizes those syscalls.",
        "- Numbers come from a laptop VM over loopback; they are for comparison on identical conditions,",
        "  not absolute claims about either server. Real Redis does more work per command (stats,",
        "  keyspace notifications, replication/persistence hooks, many more data types).",
    ]
    os.makedirs("results", exist_ok=True)
    with open("results/benchmark.md", "w") as f:
        f.write("\n".join(lines) + "\n")
    print("\n".join(lines))


if __name__ == "__main__":
    main()

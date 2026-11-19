#!/usr/bin/env python3
"""What a load run used (docs/PERFORMANCE.md §10): pods, CPU, memory, Postgres and Kafka.

load-test.sh runs `sample` every SDH_LOAD_SAMPLE_SEC (60) seconds from t0 to the end, and once more with --final,
then `report` for the report's Resources section. Each sample appends one line a thing to the run's directory:

  usage       <secs> pod <namespace> <app> <pod> <node> <CPU request> <memory limit, bytes> <cores> <working set>
              <secs> node <pool> <type> <capacity> <node> <allocatable cores> <allocatable bytes> <cores> <working set>
  pg-stats    <secs> <bytes of all databases> <commits> <rollbacks> <blocks read> <blocks hit> <temp bytes>
              <deadlocks> <active backends> <waiting on a lock> <waiting on I/O or LWLocks> <disk used> <disk size>
  kafka-disk  <secs> <topic> <bytes>; and <secs> (volume) <used> <size>

and, with --final, pg-schemas (bytes by schema of the sdh database) and pg-settings (the settings that size it).
Cores and working sets are the kubelet's (its stats summary, as node-cpu.sh reads it): CPU averaged over its last
few seconds, memory as the kernel counts it against the limit. Names of pods, nodes, schemas and topics, and
numbers only: no content.

  resources.py sample <run dir> <t0, unix seconds> <namespace>... [--final]
  resources.py report <run dir> <report.txt>
"""
import collections
import concurrent.futures
import json
import os
import re
import statistics
import subprocess
import sys
import time

INFRA = "sdh-infra"
PG = ["kubectl", "-n", INFRA, "exec", "-i", "postgres-0", "--"]
KAFKA = ["kubectl", "-n", INFRA, "exec", "-i", "kafka-0", "--"]
KAFKA_LOGS = "/var/lib/kafka/data/logs"


def run(args, timeout=30):
    """A command's output, or "" if it fails."""
    try:
        return subprocess.run(args, capture_output=True, text=True, timeout=timeout, check=True).stdout
    except (subprocess.SubprocessError, OSError):
        return ""


def psql(sql, db="sdh"):
    """Rows of a query, fields split on "|"."""
    out = run(PG + ["psql", "-U", "postgres", "-d", db, "-At", "-F", "|", "-c", sql])
    return [line.split("|") for line in out.splitlines() if line]


def quantity(text):
    """A Kubernetes quantity, such as 1100m, 2Gi or 3, as a number; 0 if none."""
    if not text:
        return 0.0
    units = {"m": 1e-3, "k": 1e3, "M": 1e6, "G": 1e9, "T": 1e12,
             "Ki": 2 ** 10, "Mi": 2 ** 20, "Gi": 2 ** 30, "Ti": 2 ** 40}
    m = re.fullmatch(r"([0-9.]+)([a-zA-Z]*)", text)
    return float(m.group(1)) * units.get(m.group(2), 1) if m else 0.0


def app_of(pod):
    """The group a pod is counted in: its app label, else its name without a ReplicaSet's or StatefulSet's suffix."""
    app = pod["metadata"].get("labels", {}).get("app")
    return app or re.sub(r"-[0-9a-f]{8,10}-[0-9a-z]{5}$|-[0-9]+$", "", pod["metadata"]["name"])


def summary(node):
    """A node's kubelet stats summary, or None."""
    out = run(["kubectl", "get", "--raw", "/api/v1/nodes/%s/proxy/stats/summary" % node], timeout=20)
    try:
        return json.loads(out)
    except ValueError:
        return None


def sample(out, t0, namespaces, final):
    secs = int(time.time()) - t0
    lines = []

    # Pods and nodes: their requests and limits from the API, what they use from each node's kubelet.
    nodes = json.loads(run(["kubectl", "get", "nodes", "-o", "json"]) or '{"items": []}')["items"]
    pods = []
    for ns in namespaces:
        pods += json.loads(run(["kubectl", "-n", ns, "get", "pods", "-o", "json"]) or '{"items": []}')["items"]
    with concurrent.futures.ThreadPoolExecutor(16) as pool:
        stats = dict(zip([n["metadata"]["name"] for n in nodes],
                         pool.map(summary, [n["metadata"]["name"] for n in nodes])))
    used = {}
    for s in stats.values():
        for p in (s or {}).get("pods", []):
            ref = p["podRef"]
            used[(ref["namespace"], ref["name"])] = (
                (p.get("cpu") or {}).get("usageNanoCores", 0) / 1e9,
                (p.get("memory") or {}).get("workingSetBytes", 0))
    for p in pods:
        if p.get("status", {}).get("phase") != "Running":
            continue
        key = (p["metadata"]["namespace"], p["metadata"]["name"])
        if key not in used:
            continue
        cpu_req = mem_lim = 0.0
        for c in p["spec"]["containers"]:
            r = c.get("resources", {})
            cpu_req += quantity(r.get("requests", {}).get("cpu"))
            mem_lim += quantity(r.get("limits", {}).get("memory"))
        lines.append(("usage", "%d pod %s %s %s %s %.3f %d %.3f %d" % (
            secs, key[0], app_of(p), key[1], p["spec"].get("nodeName", "?"), cpu_req, mem_lim, *used[key])))
    for n in nodes:
        name, labels = n["metadata"]["name"], n["metadata"].get("labels", {})
        s = stats.get(name)
        if not s:
            continue
        lines.append(("usage", "%d node %s %s %s %s %.3f %d %.3f %d" % (
            secs, labels.get("sdh-poc/pool", "?"), labels.get("node.kubernetes.io/instance-type", "?"),
            labels.get("eks.amazonaws.com/capacityType", "?"), name,
            quantity(n["status"]["allocatable"].get("cpu")), quantity(n["status"]["allocatable"].get("memory")),
            (s["node"].get("cpu") or {}).get("usageNanoCores", 0) / 1e9,
            (s["node"].get("memory") or {}).get("workingSetBytes", 0))))

    # Postgres: every database's size and activity, the backends waiting, and its volume.
    rows = psql("SELECT (SELECT sum(pg_database_size(datname)) FROM pg_database), sum(xact_commit),"
                " sum(xact_rollback), sum(blks_read), sum(blks_hit), sum(temp_bytes), sum(deadlocks)"
                " FROM pg_stat_database")
    waits = psql("SELECT count(*) FILTER (WHERE state = 'active'), count(*) FILTER (WHERE wait_event_type = 'Lock'),"
                 " count(*) FILTER (WHERE state = 'active' AND wait_event_type IN ('IO', 'LWLock'))"
                 " FROM pg_stat_activity WHERE backend_type = 'client backend'")
    disk = run(PG + ["df", "-Pk", "/var/lib/postgresql"]).splitlines()[1:2]
    if rows and waits:
        d = disk[0].split() if disk else ["-", "0", "0"]
        lines.append(("pg-stats", "%d %s %s %d %d" % (secs, " ".join(rows[0]), " ".join(waits[0]),
                                                      int(d[2]) * 1024, int(d[1]) * 1024)))

    # Kafka: each topic's bytes on disk, and its volume (du and df, not Kafka's own tools: no second JVM in the pod).
    topics = collections.Counter()
    for line in run(KAFKA + ["sh", "-c", "du -sk %s/*-[0-9]*" % KAFKA_LOGS]).splitlines():
        kb, _, path = line.partition("\t")
        topic = re.sub(r"-[0-9]+$", "", os.path.basename(path))
        if kb.isdigit():
            topics[topic] += int(kb) * 1024
    for topic, size in sorted(topics.items()):
        lines.append(("kafka-disk", "%d %s %d" % (secs, topic, size)))
    vol = run(KAFKA + ["df", "-Pk", KAFKA_LOGS]).splitlines()[1:2]
    if vol:
        v = vol[0].split()
        lines.append(("kafka-disk", "%d (volume) %d %d" % (secs, int(v[2]) * 1024, int(v[1]) * 1024)))

    if final:
        with open(os.path.join(out, "pg-schemas"), "w") as f:
            for name, size in psql("SELECT n.nspname, sum(pg_total_relation_size(c.oid)) FROM pg_class c"
                                   " JOIN pg_namespace n ON n.oid = c.relnamespace WHERE c.relkind IN ('r', 'm')"
                                   " GROUP BY 1 ORDER BY 2 DESC"):
                f.write("%s %s\n" % (name, size))
        with open(os.path.join(out, "pg-settings"), "w") as f:
            for row in psql("SELECT name, setting, coalesce(unit, '') FROM pg_settings WHERE name IN"
                            " ('max_connections', 'shared_buffers', 'work_mem', 'effective_cache_size',"
                            " 'max_wal_size', 'maintenance_work_mem') ORDER BY 1"):
                f.write(" ".join(row) + "\n")

    files = collections.defaultdict(list)
    for name, line in lines:
        files[name].append(line)
    for name, text in files.items():
        with open(os.path.join(out, name), "a") as f:
            f.write("\n".join(text) + "\n")


# --- The report ------------------------------------------------------------------------------------------------

def gib(n):
    return "%.2f" % (n / 2 ** 30)


def mib(n):
    return "%.0f" % (n / 2 ** 20)


def setting(value, unit=""):
    """A Postgres setting in its unit, such as 16384 of 8kB, as MiB, or as it is if it has no size."""
    m = re.fullmatch(r"([0-9]*)(kB|MB|GB)", unit)
    if not m or not value.isdigit():
        return value + unit
    size = int(value) * int(m.group(1) or 1) * {"kB": 2 ** 10, "MB": 2 ** 20, "GB": 2 ** 30}[m.group(2)]
    return "%s MiB" % mib(size)


def count(n):
    """A count, or an average of counts."""
    return "%.0f" % n if n == int(n) else "%.1f" % n


def spread(values, fmt):
    """min / avg / max of values, formatted; "-" if none."""
    if not values:
        return "-"
    return " / ".join(fmt(v) for v in (min(values), statistics.fmean(values), max(values)))


def read(path):
    try:
        with open(path) as f:
            return [line.split() for line in f if line.strip()]
    except OSError:
        return []


def report(out, report_txt):
    say = lambda text="": print(">> " + text if text else ">>")

    # Pods and nodes by the progress table's 15-second rows: landing=2 splitter=6 ...
    counts, nodes = collections.defaultdict(list), []
    for line in open(report_txt) if os.path.exists(report_txt) else []:
        if re.match(r"\s+\d+\s+\d+\s+\d+", line) and "landing=" in line:
            parts = line.split()
            nodes.append(int(parts[7]))
            for kv in parts[8:]:
                k, _, v = kv.partition("=")
                counts[k].append(int(v))
    say("Pods and nodes, min / avg / max, from the progress rows (every 15 s):")
    for k, v in counts.items():
        say("    %-10s %s" % (k, spread(v, count)))
    say("    %-10s %s" % ("nodes", spread(nodes, count)))

    usage = read(os.path.join(out, "usage"))
    if usage:
        pods_and_nodes(say, usage)
    else:
        say("Resources: pods and nodes not sampled (no %s)" % os.path.join(out, "usage"))
    postgres(say, out)
    kafka(say, out)


def pods_and_nodes(say, usage):
    secs = sorted({int(u[0]) for u in usage})
    say("Resources, sampled every %ss (%d samples; the kubelet's CPU and working set):"
        % (secs[1] - secs[0] if len(secs) > 1 else "?", len(secs)))

    # By app: pods; a pod's CPU and memory against its request and limit; the app's total.
    pods = [u for u in usage if u[1] == "pod"]
    by_app = collections.defaultdict(list)
    for u in pods:
        by_app[(u[2], u[3])].append(u)
    say("  by app: pods; a pod's cores (request), min / avg / max; a pod's memory MiB (limit), min / avg / max;")
    say("  the app's cores and memory GiB at its peak sample")
    say("    %-28s %-15s %-30s %-34s %s" % ("namespace/app", "pods", "cores a pod", "MiB a pod", "peak cores, GiB"))
    for (ns, app), rows in sorted(by_app.items(), key=lambda kv: -sum(float(r[8]) for r in kv[1])):
        per_sample = collections.defaultdict(list)
        for r in rows:
            per_sample[int(r[0])].append(r)
        n = [len(v) for v in per_sample.values()]
        cores = [float(r[8]) for r in rows]
        mem = [int(r[9]) for r in rows]
        peak_cpu = max(sum(float(r[8]) for r in v) for v in per_sample.values())
        peak_mem = max(sum(int(r[9]) for r in v) for v in per_sample.values())
        say("    %-28s %-15s %-30s %-34s %.2f, %s" % (
            "%s/%s" % (ns.replace("sdh-", ""), app)[:28], spread(n, count),
            "%s (%s)" % (spread(cores, lambda x: "%.2f" % x), "%.2f" % float(rows[0][6])),
            "%s (%s)" % (spread(mem, mib), mib(int(rows[0][7])) if int(rows[0][7]) else "none"),
            peak_cpu, gib(peak_mem)))
    near = sorted({(r[2], r[3]) for r in pods if int(r[7]) and int(r[9]) > 0.9 * int(r[7])})
    say("  pods above 90%% of their memory limit in a sample: %s" % (", ".join("%s/%s" % a for a in near) or "none"))

    # By node pool: nodes, and what they used of what they could give.
    by_pool = collections.defaultdict(lambda: collections.defaultdict(list))
    for u in usage:
        if u[1] == "node":
            by_pool[u[2]][int(u[0])].append(u)
    say("  by node pool: nodes, min / avg / max; share of allocatable CPU and memory used, avg / max of samples")
    for pool, per in sorted(by_pool.items()):
        n = [len(v) for v in per.values()]
        cpu = [sum(float(r[8]) for r in v) / sum(float(r[6]) for r in v) for v in per.values()]
        mem = [sum(int(r[9]) for r in v) / sum(int(r[7]) for r in v) for v in per.values()]
        types = collections.Counter("%s %s" % (r[3], r[4]) for v in per.values() for r in v)
        say("    %-8s %-17s CPU %3.0f%% / %3.0f%%, memory %3.0f%% / %3.0f%%; %s" % (
            pool, spread(n, lambda x: "%.1f" % x), 100 * statistics.fmean(cpu), 100 * max(cpu),
            100 * statistics.fmean(mem), 100 * max(mem),
            ", ".join("%s %d%%" % (t, 100 * c / sum(types.values())) for t, c in types.most_common())))



def postgres(say, out):
    # Postgres: sizes, rates between samples, waits, connections, disk; settings; schemas.
    pg = [[float(x) for x in r] for r in read(os.path.join(out, "pg-stats")) if len(r) == 13]
    if len(pg) > 1:
        first, last = pg[0], pg[-1]
        span = last[0] - first[0]
        rates = [(b[2] - a[2]) / (b[0] - a[0]) for a, b in zip(pg, pg[1:]) if b[0] > a[0] and b[2] >= a[2]]
        hit = (last[5] - first[5]) / max(1, (last[4] - first[4]) + (last[5] - first[5]))
        say("  Postgres: databases %s -> %s GiB (%+.0f MiB an hour); volume %s of %s GiB used at most" % (
            gib(first[1]), gib(last[1]), (last[1] - first[1]) / 2 ** 20 * 3600 / max(1, span),
            gib(max(r[11] for r in pg)), gib(last[12])))
        say("    commits a second, min / avg / max: %s; rollbacks %d; cache hits %.1f%%; temp files %s MiB;"
            " deadlocks %d" % (spread(rates, lambda x: "%.0f" % x), last[3] - first[3], 100 * hit,
                               mib(last[6] - first[6]), last[7] - first[7]))
        say("    client backends active, min / avg / max: %s; waiting on a lock: %s; on I/O or LWLocks: %s" % (
            spread([r[8] for r in pg], lambda x: "%.1f" % x), spread([r[9] for r in pg], lambda x: "%.1f" % x),
            spread([r[10] for r in pg], lambda x: "%.1f" % x)))
    else:
        say("  Postgres: not sampled")
    conns = [int(r[1]) for r in read(os.path.join(out, "pg-connections")) if len(r) > 1 and r[1].isdigit()]
    say("    connections (pg_stat_activity, every 15 s), min / avg / max: %s" % spread(conns, lambda x: "%.0f" % x))
    settings = read(os.path.join(out, "pg-settings"))
    if settings:
        say("    settings: " + ", ".join("%s %s" % (r[0], setting(*r[1:3])) for r in settings))
    schemas = read(os.path.join(out, "pg-schemas"))
    if schemas:
        say("    largest schemas of sdh at the end, MiB: " + ", ".join(
            "%s %s" % (r[0], mib(int(r[1]))) for r in schemas[:8]))



def kafka(say, out):
    # Kafka: bytes by topic, first and last sample, and the volume.
    kd = read(os.path.join(out, "kafka-disk"))
    if kd:
        t_first, t_last = int(kd[0][0]), int(kd[-1][0])
        at = lambda t: {r[1]: [int(x) for x in r[2:]] for r in kd if int(r[0]) == t}
        a, b = at(t_first), at(t_last)
        vol = [r for r in kd if r[1] == "(volume)"]
        say("  Kafka: volume %s of %s GiB used at most; topics' MiB, first -> last sample:" % (
            gib(max(int(r[2]) for r in vol)) if vol else "?", gib(int(vol[-1][3])) if vol else "?"))
        say("    " + ", ".join("%s %s -> %s" % (t, mib(a.get(t, [0])[0]), mib(b[t][0]))
                               for t in sorted(b, key=lambda t: -b[t][0]) if t != "(volume)" and b[t][0] >= 2 ** 20))
    else:
        say("  Kafka: not sampled")


if __name__ == "__main__":
    if len(sys.argv) >= 4 and sys.argv[1] == "sample":
        args = [a for a in sys.argv[4:] if a != "--final"]
        sample(sys.argv[2], int(sys.argv[3]), args or [INFRA], "--final" in sys.argv)
    elif len(sys.argv) == 4 and sys.argv[1] == "report":
        report(sys.argv[2], sys.argv[3])
    else:
        sys.exit(__doc__)

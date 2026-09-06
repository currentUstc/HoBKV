#!/usr/bin/env python3
import csv
import datetime as dt
import os
import re
from pathlib import Path

ROOT = Path(os.environ.get("EXPERIMENT_ROOT", "/home/k8s/liuyang/dlsm-zippy-cpu-sweep-20260907"))
RAW = ROOT / "raw"
OUT = ROOT / "analysis"
OUT.mkdir(parents=True, exist_ok=True)


def match_float(text, pattern, default=float("nan")):
    match = re.search(pattern, text, re.MULTILINE)
    return float(match.group(1)) if match else default


def match_int(text, pattern, default=0):
    match = re.search(pattern, text, re.MULTILINE)
    return int(match.group(1)) if match else default


def parse_timeline(path):
    result = {}
    with path.open() as handle:
        next(handle)
        for line in handle:
            event, epoch = line.rstrip().split("\t")
            result[event] = float(epoch)
    return result


def parse_cpu(path, timeline):
    header = path.read_text(errors="ignore").splitlines()[0]
    date_match = re.search(r"(\d{2})/(\d{2})/(\d{2})", header)
    if not date_match:
        raise RuntimeError(f"cannot parse date from {path}")
    month, day, year = map(int, date_match.groups())
    year += 2000
    samples = []
    for line in path.read_text(errors="ignore").splitlines():
        fields = line.split()
        if len(fields) < 14 or not re.fullmatch(r"\d{2}:\d{2}:\d{2}", fields[0]):
            continue
        if not fields[2].isdigit():
            continue
        hour, minute, second = map(int, fields[0].split(":"))
        stamp = dt.datetime(year, month, day, hour, minute, second).timestamp()
        cpu = float(fields[7])
        rss_kib = int(fields[12])
        if rss_kib > 0:
            samples.append((stamp, cpu, rss_kib))

    boundaries = {
        "overall": (-float("inf"), float("inf")),
        "load": (timeline["start"], timeline["load_background_end_put_start"]),
        "put": (timeline["load_background_end_put_start"], timeline["put_background_end_get_start"]),
        "get": (timeline["put_background_end_get_start"], timeline["get_end_mix_start"]),
        "mix": (timeline["get_end_mix_start"], timeline["end"] + 1.0),
    }
    result = {}
    for phase, (start, end) in boundaries.items():
        selected = [(cpu, rss) for stamp, cpu, rss in samples if start <= stamp < end]
        result[f"{phase}_cpu_avg"] = sum(cpu for cpu, _ in selected) / len(selected) if selected else float("nan")
        result[f"{phase}_cpu_peak"] = max((cpu for cpu, _ in selected), default=float("nan"))
        result[f"{phase}_rss_peak_gib"] = max((rss for _, rss in selected), default=0) / 1024 / 1024
        result[f"{phase}_samples"] = len(selected)
    return result


def parse_stalls(path, timeline):
    result = {"stall_log_overall": 0, "stall_log_load": 0, "stall_log_put": 0,
              "stall_log_get": 0, "stall_log_mix": 0}
    if not path.exists():
        return result
    boundaries = [
        ("load", timeline["start"], timeline["load_background_end_put_start"]),
        ("put", timeline["load_background_end_put_start"], timeline["put_background_end_get_start"]),
        ("get", timeline["put_background_end_get_start"], timeline["get_end_mix_start"]),
        ("mix", timeline["get_end_mix_start"], timeline["end"] + 1.0),
    ]
    for line in path.read_text(errors="ignore").splitlines():
        if "Current memtable full; waiting" not in line:
            continue
        result["stall_log_overall"] += 1
        match = re.match(r"(\d{4})/(\d{2})/(\d{2})-(\d{2}):(\d{2}):(\d{2})", line)
        if not match:
            continue
        year, month, day, hour, minute, second = map(int, match.groups())
        stamp = dt.datetime(year, month, day, hour, minute, second).timestamp()
        for phase, start, end in boundaries:
            if start <= stamp < end:
                result[f"stall_log_{phase}"] += 1
                break
    return result


rows = []
for run_dir in sorted(RAW.iterdir()):
    if not run_dir.is_dir() or not (run_dir / "compute.log").exists():
        continue
    match = re.fullmatch(r"(baseline|hotcold)-(\d+)c", run_dir.name)
    if not match:
        continue
    mode, cores_text = match.groups()
    text = (run_dir / "compute.log").read_text(errors="ignore").replace("\r", "\n")
    timeline = parse_timeline(run_dir / "timeline.tsv")
    row = {
        "mode": mode,
        "server_cores": int(cores_text),
        "load_iops": match_int(text, r"^fillmixgraph:.*?;\s*(\d+) ops/sec"),
        "load_wait_ms": match_float(text, r"background_wait_after_fillmixgraph: end elapsed_us=(\d+)") / 1000,
        "put_iops": match_int(text, r"^putmixgraph_put:.*?ops/sec=(\d+)"),
        "put_avg_us": match_float(text, r"^putmixgraph_put:.*?avg_latency_us=([0-9.]+)"),
        "put_p50_us": match_float(text, r"^putmixgraph_put:.*?p50_us=([0-9.]+)"),
        "put_p95_us": match_float(text, r"^putmixgraph_put:.*?p95_us=([0-9.]+)"),
        "put_p99_us": match_float(text, r"^putmixgraph_put:.*?p99_us=([0-9.]+)"),
        "put_wait_ms": match_float(text, r"background_wait_after_putmixgraph: end elapsed_us=(\d+)") / 1000,
        "get_iops": match_int(text, r"^getmixgraph_get:.*?ops/sec=(\d+)"),
        "get_avg_us": match_float(text, r"^getmixgraph_get:.*?avg_latency_us=([0-9.]+)"),
        "get_p50_us": match_float(text, r"^getmixgraph_get:.*?p50_us=([0-9.]+)"),
        "get_p95_us": match_float(text, r"^getmixgraph_get:.*?p95_us=([0-9.]+)"),
        "get_p99_us": match_float(text, r"^getmixgraph_get:.*?p99_us=([0-9.]+)"),
        "mix_get_iops": match_int(text, r"^mixgraph_get:.*?ops/sec=(\d+)"),
        "mix_get_avg_us": match_float(text, r"^mixgraph_get:.*?avg_latency_us=([0-9.]+)"),
        "mix_get_p99_us": match_float(text, r"^mixgraph_get:.*?p99_us=([0-9.]+)"),
        "mix_put_iops": match_int(text, r"^mixgraph_put:.*?ops/sec=(\d+)"),
        "mix_put_avg_us": match_float(text, r"^mixgraph_put:.*?avg_latency_us=([0-9.]+)"),
        "mix_put_p99_us": match_float(text, r"^mixgraph_put:.*?p99_us=([0-9.]+)"),
    }
    row["mix_total_iops"] = row["mix_get_iops"] + row["mix_put_iops"]
    for node in ("node4", "node6"):
        cpu = parse_cpu(run_dir / f"{node}-cpu.log", timeline)
        row.update({f"{node}_{key}": value for key, value in cpu.items()})
    row.update(parse_stalls(run_dir / "db.log", timeline))
    rows.append(row)

rows.sort(key=lambda item: (item["server_cores"], item["mode"]))
fieldnames = list(rows[0].keys())
with (OUT / "summary.csv").open("w", newline="") as handle:
    writer = csv.DictWriter(handle, fieldnames=fieldnames)
    writer.writeheader()
    writer.writerows(rows)

by_key = {(row["mode"], row["server_cores"]): row for row in rows}
comparison_metrics = ["load_iops", "put_iops", "get_iops", "mix_get_iops", "mix_put_iops", "mix_total_iops",
                      "node4_overall_cpu_avg", "node6_overall_cpu_avg"]
comparison_rows = []
for cores in range(2, 17, 2):
    baseline = by_key[("baseline", cores)]
    hotcold = by_key[("hotcold", cores)]
    item = {"server_cores": cores}
    for metric in comparison_metrics:
        item[f"baseline_{metric}"] = baseline[metric]
        item[f"hotcold_{metric}"] = hotcold[metric]
        item[f"change_{metric}_pct"] = (hotcold[metric] / baseline[metric] - 1) * 100
    comparison_rows.append(item)

with (OUT / "comparison.csv").open("w", newline="") as handle:
    writer = csv.DictWriter(handle, fieldnames=list(comparison_rows[0].keys()))
    writer.writeheader()
    writer.writerows(comparison_rows)

with (OUT / "key-results.md").open("w") as handle:
    handle.write("| Server cores | Baseline Put | Hot/cold Put | Change | Baseline Get | Hot/cold Get | Change | Baseline Mix | Hot/cold Mix | Change |\n")
    handle.write("|---:|---:|---:|---:|---:|---:|---:|---:|---:|---:|\n")
    for item in comparison_rows:
        handle.write(
            f"| {item['server_cores']} | {item['baseline_put_iops']:.0f} | {item['hotcold_put_iops']:.0f} | "
            f"{item['change_put_iops_pct']:+.1f}% | {item['baseline_get_iops']:.0f} | "
            f"{item['hotcold_get_iops']:.0f} | {item['change_get_iops_pct']:+.1f}% | "
            f"{item['baseline_mix_total_iops']:.0f} | {item['hotcold_mix_total_iops']:.0f} | "
            f"{item['change_mix_total_iops_pct']:+.1f}% |\n"
        )

print(f"wrote {len(rows)} runs to {OUT / 'summary.csv'}")

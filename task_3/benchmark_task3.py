"""
Task 3 measurements (delta encoding): upload + download time for N = 6, 12, 24
nodes, for both delta options.

    python benchmark_task3.py --nodes 6 12 24 --repeat 100

mode "first"    = option (a), every delta is made against v1
mode "previous" = option (b), every delta is made against the version before

Same procedure as benchmark_task1.py so the numbers can be compared: for every
mode and N a fresh system is started, then `repeat` rounds of upload a dataset
file + download it again (latest version) + compare the bytes. Files go in as
apple_v1..v5, banana_v1.. so with 100 samples every version number gets 20.
After every pass over the dataset everything is deleted.

Upload time = request sent -> 201 back (delta made, 3 replicas acked, fsynced).
Download time = request sent -> last byte received (all blobs fetched + decoded).
The file is read from disk before the timer starts.

The version number matters a lot here (v1 is stored in full, in mode "previous"
v5 needs 5 blobs), so besides the overall summary there is one per version.

Output in results/:
  task3_samples_<mode>_N<n>.csv   every sample
  task3_summary.csv               avg/median/min/max per mode and N
  task3_by_version.csv            the same split up by version number
  task3_gains.csv                 Task 1 time / Task 3 time and Task 2 time / Task 3 time
"""
import argparse
import csv
import glob
import os
import statistics
import time

import requests

from start_system import start_system, stop_system, wait_until_ready

MODES = {"first": "a", "previous": "b"}
METRICS = ["upload", "download", "reference", "encode", "decode"]


def dataset_files(folder):
    files = glob.glob(os.path.join(folder, "*_v*.txt"))
    if not files:
        raise SystemExit(f"no *_vN.txt files in {folder}, unzip DataSet.zip there first")
    # sort by fruit then version number so v1 always goes in before v2 etc.
    def key(path):
        stem = os.path.basename(path)[:-4]
        fruit, ver = stem.rsplit("_v", 1)
        return fruit, int(ver)
    return sorted(files, key=key)


def summarize(values):
    return {
        "avg": round(statistics.mean(values), 5),
        "median": round(statistics.median(values), 5),
        "min": round(min(values), 5),
        "max": round(max(values), 5),
    }


def run(mode, n_nodes, files, repeat, url, out_dir):
    print(f"\n=== mode {mode} (option {MODES[mode]}), N = {n_nodes}: starting system ===")
    procs = start_system(n_nodes, delta_mode=mode)
    session = requests.Session()
    samples = []
    uploaded_bases = set()
    try:
        wait_until_ready(procs, n_nodes, url)
        for i in range(repeat):
            path = files[i % len(files)]
            name = os.path.basename(path)
            with open(path, "rb") as f:
                data = f.read()

            t0 = time.perf_counter()
            r = session.post(f"{url}/files", files={"file": (name, data)})
            upload_s = time.perf_counter() - t0
            if r.status_code != 201:
                raise RuntimeError(f"upload of {name} failed: {r.status_code} {r.text}")
            info = r.json()
            uploaded_bases.add(info["base_name"])

            t0 = time.perf_counter()
            r = session.get(f"{url}/files/{info['base_name']}")
            body = r.content
            download_s = time.perf_counter() - t0
            if r.status_code != 200 or body != data:
                raise RuntimeError(f"download of {info['base_name']} failed or bytes differ")

            samples.append({
                "sample": i + 1, "mode": mode, "N": n_nodes, "file": name,
                "version": info["version"], "size_bytes": len(data),
                "encoding": info["encoding"], "stored_bytes": info["stored_size"],
                "upload_s": upload_s, "download_s": download_s,
                # what the lead reports about its own processing:
                "reference_s": info["reference_s"],      # upload: getting the reference version back
                "encode_s": info["encode_s"],            # upload: xdelta encode
                "decode_s": float(r.headers["X-Decode-Seconds"]),   # download: all xdelta decodes
                "blobs_fetched": int(r.headers["X-Blobs-Fetched"]),
                "fetched_bytes": int(r.headers["X-Fetched-Bytes"]),
            })
            print(f"  [{i + 1}/{repeat}] {name}: up {upload_s:.3f}s  down {download_s:.3f}s  "
                  f"stored {info['stored_size']:>8} B  ({r.headers['X-Blobs-Fetched']} blobs to read it)")

            # end of a pass over the dataset -> clean up
            if (i + 1) % len(files) == 0 or i + 1 == repeat:
                for base in uploaded_bases:
                    session.delete(f"{url}/files/{base}")
                uploaded_bases.clear()
    finally:
        stop_system(procs)

    with open(os.path.join(out_dir, f"task3_samples_{mode}_N{n_nodes}.csv"), "w", newline="") as f:
        writer = csv.DictWriter(f, fieldnames=list(samples[0].keys()))
        writer.writeheader()
        writer.writerows(samples)
    return samples


def write_csv(path, rows):
    with open(path, "w", newline="") as f:
        writer = csv.DictWriter(f, fieldnames=list(rows[0].keys()))
        writer.writeheader()
        writer.writerows(rows)


def load_summary(path):
    """{(N, metric): row} from the task 1 / task 2 summary files, {} if not there."""
    if not os.path.exists(path):
        return {}
    with open(path, newline="") as f:
        return {(int(r["N"]), r["metric"]): r for r in csv.DictReader(f)}


def main():
    parser = argparse.ArgumentParser(description="Task 3 upload/download measurements")
    parser.add_argument("--dataset", default="../dataset")
    parser.add_argument("--nodes", type=int, nargs="+", default=[6, 12, 24])
    parser.add_argument("--modes", nargs="+", choices=list(MODES), default=list(MODES))
    parser.add_argument("--repeat", type=int, default=100)
    parser.add_argument("--url", default="http://127.0.0.1:5000")
    parser.add_argument("--out", default="../results")
    args = parser.parse_args()

    files = dataset_files(args.dataset)
    os.makedirs(args.out, exist_ok=True)

    summary, by_version = [], []
    for mode in args.modes:
        for n in args.nodes:
            samples = run(mode, n, files, args.repeat, args.url, args.out)
            for metric in METRICS:
                key = f"{metric}_s"
                summary.append({"mode": mode, "N": n, "metric": metric, "samples": len(samples),
                                **summarize([s[key] for s in samples])})
                for version in sorted({s["version"] for s in samples}):
                    subset = [s for s in samples if s["version"] == version]
                    by_version.append({
                        "mode": mode, "N": n, "version": version, "metric": metric,
                        "samples": len(subset),
                        "stored_bytes_avg": round(statistics.mean(s["stored_bytes"] for s in subset)),
                        "fetched_bytes_avg": round(statistics.mean(s["fetched_bytes"] for s in subset)),
                        **summarize([s[key] for s in subset])})

    write_csv(os.path.join(args.out, "task3_summary.csv"), summary)
    write_csv(os.path.join(args.out, "task3_by_version.csv"), by_version)

    print("\nmode       N  metric       avg     median   min      max   (seconds)")
    for r in summary:
        print(f"{r['mode']:<9} {r['N']:>3}  {r['metric']:<10} {r['avg']:>7.3f}  {r['median']:>7.3f}  "
              f"{r['min']:>7.3f}  {r['max']:>7.3f}")

    # gains against the earlier tasks, from their summary files (avg times)
    task1 = load_summary(os.path.join(args.out, "task1_summary.csv"))
    task2 = load_summary(os.path.join(args.out, "task2_summary.csv"))
    gains = []
    for r in summary + by_version:
        if r["metric"] not in ("upload", "download"):
            continue
        t1, t2 = task1.get((r["N"], r["metric"])), task2.get((r["N"], r["metric"]))
        gains.append({
            "mode": r["mode"], "N": r["N"], "version": r.get("version", "all"), "metric": r["metric"],
            "task3_avg": r["avg"],
            "task1_avg": float(t1["avg"]) if t1 else "",
            "gain_vs_task1": round(float(t1["avg"]) / r["avg"], 3) if t1 else "",
            "task2_avg": float(t2["avg"]) if t2 else "",
            "gain_vs_task2": round(float(t2["avg"]) / r["avg"], 3) if t2 else "",
        })
    write_csv(os.path.join(args.out, "task3_gains.csv"), gains)

    if task1:
        print("\nGain = Task 1 avg time / Task 3 avg time   (all versions together)")
        print("mode       N  metric     vs task1  vs task2")
        for g in gains:
            if g["version"] == "all":
                print(f"{g['mode']:<9} {g['N']:>3}  {g['metric']:<9} {g['gain_vs_task1']:>7}  {g['gain_vs_task2']:>8}")
    else:
        print("\n(no task1_summary.csv in the results folder, run benchmark_task1.py to get the gains)")
    print(f"\nsaved to {args.out}/")


if __name__ == "__main__":
    main()

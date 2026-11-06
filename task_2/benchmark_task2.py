"""
Task 1 measurements: upload + download time for N = 6, 12, 24 nodes.

    python benchmark_task2.py --dataset dataset --nodes 6 12 24 --repeat 100

For every N it starts a fresh system (start_system.py), then does `repeat`
rounds of: upload a dataset file, download it again (latest version) and check
the bytes are the same. Files are uploaded in order apple_v1..v5, banana_v1..
so the version detection is exercised too. After every pass over the dataset
everything gets deleted so the disk doesn't fill up and the next pass can
upload the same versions again.

Upload time = request sent -> 201 back (i.e. all 3 replicas acked, fsynced).
Download time = request sent -> last byte received.
The file is read from disk before the timer starts, so local disk reads don't count.

Output: results/task2_samples_N<n>.csv and results/task2_summary.csv
"""
import argparse
import csv
import glob
import os
import statistics
import time

import requests

from start_system import start_system, stop_system, wait_until_ready


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
        "avg": statistics.mean(values),
        "median": statistics.median(values),
        "min": min(values),
        "max": max(values),
    }


def run_for_n(n_nodes, files, repeat, url, out_dir, start=True):
    procs = []
    if start:
        print(f"\n=== N = {n_nodes}: starting system ===")
        procs = start_system(n_nodes)
        wait_until_ready(procs, n_nodes, url)

    session = requests.Session()
    samples = []
    uploaded_bases = set()
    try:
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

            samples.append({"sample": i + 1, "file": name, "size_bytes": len(data),
                            "nodes": " ".join(info["nodes"]),
                            "upload_s": upload_s, "download_s": download_s})
            print(f"  [{i + 1}/{repeat}] {name}: up {upload_s:.3f}s  down {download_s:.3f}s")

            # end of a pass over the dataset -> clean up
            if (i + 1) % len(files) == 0 or i + 1 == repeat:
                for base in uploaded_bases:
                    session.delete(f"{url}/files/{base}")
                uploaded_bases.clear()
    finally:
        if start:
            stop_system(procs)

    with open(os.path.join(out_dir, f"task2_samples_N{n_nodes}.csv"), "w", newline="") as f:
        writer = csv.DictWriter(f, fieldnames=list(samples[0].keys()))
        writer.writeheader()
        writer.writerows(samples)

    return {
        "upload": summarize([s["upload_s"] for s in samples]),
        "download": summarize([s["download_s"] for s in samples]),
    }


def main():
    parser = argparse.ArgumentParser(description="Task 1 upload/download measurements")
    parser.add_argument("--dataset", default="../dataset")
    parser.add_argument("--nodes", type=int, nargs="+", default=[6, 12, 24])
    parser.add_argument("--repeat", type=int, default=100)
    parser.add_argument("--url", default="http://127.0.0.1:5000")
    parser.add_argument("--out", default="../results")
    parser.add_argument("--no-start", action="store_true",
                        help="use a system that's already running (then --nodes is just a label)")
    args = parser.parse_args()

    files = dataset_files(args.dataset)
    os.makedirs(args.out, exist_ok=True)

    rows = []
    for n in args.nodes:
        result = run_for_n(n, files, args.repeat, args.url, args.out, start=not args.no_start)
        for metric, stats in result.items():
            rows.append({"N": n, "metric": metric, "samples": args.repeat,
                         **{k: round(v, 4) for k, v in stats.items()}})

    with open(os.path.join(args.out, "task2_summary.csv"), "w", newline="") as f:
        writer = csv.DictWriter(f, fieldnames=list(rows[0].keys()))
        writer.writeheader()
        writer.writerows(rows)

    print("\n  N  metric      avg     median   min      max   (seconds)")
    for r in rows:
        print(f"{r['N']:>3}  {r['metric']:<9} {r['avg']:>7.3f}  {r['median']:>7.3f}  "
              f"{r['min']:>7.3f}  {r['max']:>7.3f}")
    print(f"\nsaved to {args.out}/")


if __name__ == "__main__":
    main()

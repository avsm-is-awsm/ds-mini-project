"""
Task 3, point 5: size of the deltas and encode / decode time for every dataset
file, measured offline (no lead, no nodes, no network).

    python measure_delta.py --repeat 3

For every fruit and every version i >= 2 it makes both deltas:
  option (a)  v1      -> v_i     (mode "first" in the lead)
  option (b)  v_(i-1) -> v_i     (mode "previous")

The encode/decode goes through temp files exactly like in lead_node.py (pyxdelta
only takes file paths), so the times here are what the lead will really spend.

R_enc / R_dec are in bit/s of ORIGINAL data (8*B / time), like in Task 2.

Output: results/task3_deltas.csv and a summary per version on screen.
"""
import argparse
import csv
import glob
import os
import statistics
import tempfile
import time

import pyxdelta


def write(path, data):
    with open(path, "wb") as f:
        f.write(data)


def read(path):
    with open(path, "rb") as f:
        return f.read()


def xdelta_encode(source, target):
    with tempfile.TemporaryDirectory() as tmp:
        src, tgt, patch = (os.path.join(tmp, n) for n in ("source", "target", "patch"))
        write(src, source)
        write(tgt, target)
        if not pyxdelta.run(src, tgt, patch):
            raise RuntimeError("xdelta3 could not create the delta")
        return read(patch)


def xdelta_decode(source, delta):
    with tempfile.TemporaryDirectory() as tmp:
        src, patch, out = (os.path.join(tmp, n) for n in ("source", "patch", "out"))
        write(src, source)
        write(patch, delta)
        if not pyxdelta.decode(src, patch, out):
            raise RuntimeError("xdelta3 could not apply the delta")
        return read(out)


def timed(func, repeat):
    """Run func `repeat` times, return (result, median time)."""
    times = []
    for _ in range(repeat):
        t0 = time.perf_counter()
        result = func()
        times.append(time.perf_counter() - t0)
    return result, statistics.median(times)


def main():
    parser = argparse.ArgumentParser()
    parser.add_argument("--dataset", default="../dataset")
    parser.add_argument("--out", default="../results")
    parser.add_argument("--repeat", type=int, default=3, help="timing repetitions per delta")
    args = parser.parse_args()

    fruits = sorted({os.path.basename(p).rsplit("_v", 1)[0]
                     for p in glob.glob(os.path.join(args.dataset, "*_v*.txt"))})
    if not fruits:
        raise SystemExit(f"no *_vN.txt files in {args.dataset}")
    os.makedirs(args.out, exist_ok=True)

    rows = []
    for fruit in fruits:
        versions = {}
        v = 1
        while os.path.exists(os.path.join(args.dataset, f"{fruit}_v{v}.txt")):
            versions[v] = read(os.path.join(args.dataset, f"{fruit}_v{v}.txt"))
            v += 1

        for i in sorted(versions)[1:]:
            target = versions[i]
            for option, ref in (("a", 1), ("b", i - 1)):
                source = versions[ref]
                delta, enc_s = timed(lambda: xdelta_encode(source, target), args.repeat)
                back, dec_s = timed(lambda: xdelta_decode(source, delta), args.repeat)
                assert back == target
                rows.append({
                    "file": f"{fruit}_v{i}.txt", "version": i, "option": option, "ref_version": ref,
                    "size_bytes": len(target), "delta_bytes": len(delta),
                    "ratio": round(len(delta) / len(target), 4),
                    "encode_s": round(enc_s, 5), "decode_s": round(dec_s, 5),
                    "R_enc_mbps": round(8 * len(target) / enc_s / 1e6, 1),
                    "R_dec_mbps": round(8 * len(target) / dec_s / 1e6, 1),
                })
        print(f"{fruit}: done")

    with open(os.path.join(args.out, "task3_deltas.csv"), "w", newline="") as f:
        writer = csv.DictWriter(f, fieldnames=list(rows[0].keys()))
        writer.writeheader()
        writer.writerows(rows)

    def avg(subset, key):
        return statistics.mean(r[key] for r in subset)

    print("\noption  version  ref   delta size      of B    encode    decode")
    for option in ("a", "b"):
        for version in sorted({r["version"] for r in rows}):
            sub = [r for r in rows if r["option"] == option and r["version"] == version]
            print(f"  ({option})     v{version}     v{sub[0]['ref_version']}   {avg(sub, 'delta_bytes'):>9,.0f} B   "
                  f"{avg(sub, 'ratio'):>5.1%}   {avg(sub, 'encode_s') * 1000:5.0f} ms  {avg(sub, 'decode_s') * 1000:5.0f} ms")
    print(f"\nv1 is always stored in full (B_delta,1 = B)")
    print(f"avg encode {avg(rows, 'encode_s') * 1000:.0f} ms -> R_enc = {avg(rows, 'R_enc_mbps'):,.0f} Mbit/s")
    print(f"avg decode {avg(rows, 'decode_s') * 1000:.0f} ms -> R_dec = {avg(rows, 'R_dec_mbps'):,.0f} Mbit/s")
    print(f"saved to {os.path.join(args.out, 'task3_deltas.csv')}")


if __name__ == "__main__":
    main()

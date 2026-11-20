# Task 2: per-block zlib compression and deduplication

## Implementation

The lead divides the original file into 4096-byte blocks and compresses each
block independently with zlib (level 6). The MD5 index is calculated over the
compressed representation stored on the storage nodes. Because compression is
independent and deterministic, equal source blocks produce equal compressed
blocks and remain deduplicable across files and versions.

A block-index hit reuses the existing storage ID. The lead checks the indexed
replica node list against current heartbeats and tops the replica set back up to
three live nodes when necessary. It does not issue a new storage ID or rewrite
healthy replicas for a normal hit. New files store their ordered block IDs,
original byte length/CRC32, compressed byte count, compression duration, and
compression format. Downloads fetch and CRC-check compressed blocks, decompress
them in order, then validate the reconstructed original file's size and CRC32.

The API upload response includes `new_blocks`, `reused_blocks`,
`compressed_size`, `compression_ratio`, and `compression_time_s`. Downloads
include `X-Decompression-Time-S` and `X-Compressed-Size` headers. The benchmark
also verifies that downloaded bytes exactly match the source.

## Run

From this directory, with the dataset extracted into `../dataset`:

```bash
python benchmark_task2.py --nodes 6 12 24 --repeat 100 --dataset ../dataset \
  --out ../results_task2 --bandwidth-mbps 100 --baseline-dir ../results_task1
```

`--repeat 100` means 100 upload/download observations for **each dataset file**
for each N. Every repeat is uploaded under a new, incremented version number.
Earlier versions stay stored throughout the measurement run, so later repeats
can reuse their blocks. All uploaded versions are deleted after the run. The
data directory is cleared once when each N system starts.

The script writes per-N raw samples, per-file and aggregate summary statistics
(avg/median/min/max), analytical predicted gains (when link bandwidth is
provided), and empirical gains (when Task 1 sample CSVs are provided).

## Analytical model and assumptions

Let `B` be original file bytes, `Bzip` the sum of independently compressed
block bytes, `W` the link bandwidth in bits/s, `Renc` compression throughput in
bits/s, and `Rdec` decompression throughput in bits/s. The same W is assumed
for client-to-lead and lead-to-storage transfers. Three replica transfers are
assumed to proceed in parallel, so their elapsed transfer time is one payload
transfer time rather than three sequential transfers. Request/response and
metadata overhead are ignored in this simplified model.

- Task 1 upload: `T1_up = 2 * 8B / W` (client upload plus one parallel replica transfer).
- Task 2 upload: `T2_up = 8B / W + 8B / Renc + 8Bzip / W`.
- Task 1 download: `T1_down = 2 * 8B / W` (storage-to-lead plus lead-to-client).
- Task 2 download: `T2_down = 16Bzip / W + 8B / Rdec`.
- Predicted gain: `T1 / T2` for upload and download respectively.

The benchmark uses measured compression/decompression durations in these
predictions, which is equivalent to estimating Renc and Rdec from the measured
file sizes and CPU times. The model does not include per-block ZeroMQ, SQLite,
HTTP, or disk latency; compare its predictions with empirical gains rather than
expecting an exact match.

Empirical gain is computed as the Task 1 mean time divided by the Task 2 mean
time for the same file and N. A value above 1 means Task 2 is faster; below 1
means Task 2 is slower. Compare like-for-like sample sets and system settings.

## Validation note

The code is syntax-checked in the provided environment. A live distributed
integration run requires the project's runtime dependencies (gevent, Flask,
ZeroMQ, tinyrpc, APScheduler, protobuf, requests, and boto3 where imported) and
was not possible in this environment because `gevent` is not installed.

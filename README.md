# DS Mini Project - Distributed Storage

A distributed storage system in Python 3: a lead node with a REST API that stores
every file as 3 replicas on N storage nodes, then extended with compression
(Task 2) and VCDIFF delta encoding (Task 3).

| Task | Folder | Status |
|---|---|---|
| 1 - Replication | `task_1/` | done, measured |
| 2 - Compression | `task_2/` | in progress (Chris) |
| 3 - Delta encoding (VCDIFF) | `task_3/` | done, measured (both options) |

Each task folder is a complete copy of the system, so you always `cd` into the
folder of the task you want to run. Results of all tasks go to `results/`.

## Repository layout

```
task_1/                 replication only
task_2/                 replication + compression (in progress)
task_3/                 replication + xdelta3 delta encoding
  lead_node.py          REST API, metadata (SQLite), replication, the task's encoding
  storage_node.py       stores / returns / deletes blobs (same in every task)
  start_system.py       starts the lead + N storage nodes
  client.py             command line client
  messages.proto        lead <-> node messages (messages_pb2.py is generated from it)
  benchmark_taskN.py    upload / download measurements for N = 6, 12, 24
  measure_delta.py      offline delta size + timing measurements (Task 3)
postman/                Postman collection for the REST API
requirements.txt        Python packages for all tasks
```

## Setup

Developed and tested with Python 3.14.

```
python -m venv .venv
.venv\Scripts\activate          (Windows)   /   source .venv/bin/activate
pip install -r requirements.txt
```

`requirements.txt` includes `pyxdelta` (xdelta3 / VCDIFF), which only Task 3 needs.

Unzip `DataSet.zip` into a folder called `dataset/` in the project root (it's in .gitignore, 500MB).

If you change `messages.proto`, regenerate the python file with:

```
protoc --python_out=. messages.proto
```

## Running it

```
cd task_1          (or task_2, task_3)
python start_system.py -n 6
python start_system.py -n 6 --delta-mode previous      (task_3 only, default is first)
```

That starts the lead node (REST API on port 5000, node RPC on 5001) and 6 storage
nodes (zmq on ports 5101-5106). Logs end up in `data/logs/`, the replicas in
`data/nodeXX/`. Every start wipes `data/` unless you pass `--keep`.

Then in another terminal (same folder):

```
python client.py upload ../dataset/apple_v1.txt
python client.py upload ../dataset/apple_v2.txt
python client.py list
python client.py download apple.txt                (latest = v2)
python client.py download apple.txt --version 1
python client.py nodes
python client.py delete apple.txt
```

Or import `postman/DS_MiniProject_Task1.postman_collection.json` in Postman
(for the upload pick a file in the form-data field `file`). The API is the same
for all tasks.

## How it works (Task 1)

```
client/Postman --HTTP--> lead node --zmq (protobuf header + raw bytes)--> 3 of N storage nodes
                            |  ^
                       SQLite  tinyrpc register/heartbeat (every 2s) from every node
```

- **lead_node.py** - Flask REST API served with gevent. On upload it works out the
  version, picks 3 random alive nodes and sends the full file to all 3 in parallel.
  It only answers 201 once all 3 nodes have acked, and a node only acks after the
  file is fsynced to disk, so a 201 means 3 safe copies. If a node fails during an
  upload we try another alive node instead. Downloads read from a random replica
  (alive ones first) and check the crc32 before sending it back.
- **storage_node.py** - zmq REP socket. Stores / returns / deletes blobs by
  `storage_id` (a uuid the lead hands out) and checks the crc32 before writing.
  It registers with the lead over tinyrpc and sends heartbeats from an apscheduler
  job. Blobs go to local disk by default, or to S3 with `--s3-bucket` (boto3, not
  used for the measurements).
- **Version detection** - based on the file name: `apple_v3.txt` is version 3 of
  `apple.txt`. If the name has no `_vN` it just becomes the next version. Uploading
  a version that already exists gives a 409. The upload response says if it was a
  new file (`new_file`) and which versions existed before (`previous_versions`).
- **Node failure** - the lead marks a node dead after 6s with no heartbeat and stops
  picking it for new files. Reads go to the other replicas.

### REST API

| Method | URL | What |
|---|---|---|
| POST | `/files` | upload (multipart form-data, field `file`) |
| GET | `/files` | list files with latest version / number of versions |
| GET | `/files/apple.txt` | latest version |
| GET | `/files/apple_v2.txt` or `/files/apple.txt?version=2` | specific version |
| DELETE | `/files/apple.txt` | delete all versions (`apple_v2.txt` = only that one) |
| GET | `/nodes` | registered nodes + alive status |

## Measurements

All measured on one laptop (localhost), 10 MB files, 100 samples per N.

```
cd task_1 && python benchmark_task1.py          -> results/task1_*.csv
cd task_3 && python measure_delta.py            -> results/task3_deltas.csv       (point 5)
cd task_3 && python benchmark_task3.py          -> results/task3_*.csv (both options, all N)
```

- upload time = request sent -> 201 back (3 replicas stored + fsynced)
- download time = request sent -> last byte received
- the file is read into memory before the timer starts
- run the benchmarks right after each other, otherwise the gains compare
  different states of the laptop. The Task 1 and Task 3 numbers below are from
  one such run.

## Task 1 analysis (notes for the report)

B = file size in bytes, R = link bandwidth in bit/s, same for every link.
Assumptions: the lead stores and forwards (it gets the whole file before sending
it on), and we ignore latency, processing and disk time.

**Upload (point 2).** Client -> lead is `8B/R`. Then the lead sends 3 full copies.
They all go out over the lead's own link, so sending them in parallel or one after
the other takes the same time, `3 * 8B/R`:

    T_up1 = 8B/R + 3 * 8B/R = 32B/R

(If every lead -> node connection had its own R, the 3 copies would overlap and
`T_up = 8B/R + 8B/R = 16B/R`. Worth saying in the report which assumption we use.)

**Download (point 3).** The request is tiny, so we ignore it. The node sends the
file to the lead, then the lead sends it to the client:

    T_down1 = 8B/R + 8B/R = 16B/R

Example: B = 10^7 bytes, R = 1 Gbit/s -> T_up1 = 0.32 s, T_down1 = 0.16 s.

**Results (point 4):**

| N | upload avg / median / min / max | download avg / median / min / max |
|---|---|---|
| 6 | 0.100 / 0.100 / 0.074 / 0.148 | 0.054 / 0.054 / 0.032 / 0.077 |
| 12 | 0.104 / 0.104 / 0.073 / 0.152 | 0.054 / 0.052 / 0.032 / 0.116 |
| 24 | 0.093 / 0.091 / 0.069 / 0.138 | 0.053 / 0.049 / 0.033 / 0.110 |

- N doesn't change much because we always write exactly 3 copies, N only changes
  which nodes get picked.
- The model says upload/download = 32/16 = 2, measured is about 1.9.
- "Effective R" from the measurements is about 3 Gbit/s (R = 32B / T_up). That's
  localhost + Python + disk, not a real network link.

## What changed for Task 3

Again only the lead (`task_3/lead_node.py`), built on the Task 1 version. Deltas
are real VCDIFF made by xdelta3 through the `pyxdelta` package.

- **First version of a file** is stored in full on 3 nodes, exactly like Task 1
  (pyxdelta can't encode without a source, so B_delta,1 = B).
- **Every later version** is stored as a delta. Which version the delta is made
  against depends on `--delta-mode`:
  - `first` = option (a): always against v1. Reading v_i needs v1 + delta_i (2 blobs).
  - `previous` = option (b): against v_(i-1), so it's a chain. Reading v_i needs
    v1 + delta_2 ... delta_i (i blobs).

  ("all previous versions as previous knowledge" in the assignment: v_(i-1) is
  itself built from v1 and all deltas before it, and the assignment says download
  needs "all deltas from 1 to i", so we read option (b) as the chain.)
- **Upload of a new version:** the lead doesn't keep old versions, so it first
  gets the reference version back from the storage nodes (for option b that means
  fetching and decoding the whole chain), makes the delta and replicates the delta
  to 3 nodes.
- **Download:** fetch all blobs of the chain (one after the other), apply the
  deltas in order, check the crc32 of the result, send the full file to the client.
- SQLite keeps for each version: `encoding` (full / vcdiff), `ref_version`,
  `stored_size`, and a crc32 of both the stored blob and the original.
- Versions have to be uploaded in order, and only the newest version can be
  deleted on its own (older ones may be the reference of a delta). Both give a 409.
- The upload response has `encoding`, `ref_version`, `stored_size`, `reference_s`
  (time to get the reference back) and `encode_s`. The download has the headers
  `X-Blobs-Fetched`, `X-Fetched-Bytes` and `X-Decode-Seconds`.

## Task 3 analysis (notes for the report)

B_d,i = stored size of version i (B_d,1 = B), d_i = B_d,i / B. R_enc / R_dec =
xdelta encode / decode speed in bit/s of original data. v1 behaves exactly like
Task 1, so everything below is for i >= 2.

**Option (a), delta against v1:**

    T_up(a)   = 8B/R  +  8*B_d,1/R  +  8B/R_enc  +  3 * 8*B_d,i/R
                client   fetch v1      encode       3 deltas out
    T_down(a) = 8*B_d,1/R + 8*B_d,i/R  +  8B/R_dec  +  8B/R
                fetch v1 and delta        decode       to the client

**Option (b), chain:**

    T_up(b)   = 8B/R + sum_{j=1..i-1} 8*B_d,j/R + (i-2) * 8B/R_dec + 8B/R_enc + 3 * 8*B_d,i/R
                       fetch the whole chain      rebuild v_(i-1)
    T_down(b) = sum_{j=1..i} 8*B_d,j/R + (i-1) * 8B/R_dec + 8B/R

**Gains against Task 1** (with B_d,1 = B):

    G_up(a)   = 4 / (2 + 3*d_i + R/R_enc)
    G_down(a) = 2 / (2 + d_i + R/R_dec)
    G_up(b)   = 4 / (2 + sum_{j=2..i-1} d_j + 3*d_i + R/R_enc + (i-2)*R/R_dec)
    G_down(b) = 2 / (2 + sum_{j=2..i} d_j + (i-1)*R/R_dec)

- The "2" in every denominator is the client hop plus fetching the full v1.
  That is the price of storing v1 uncompressed and not caching it on the lead.
- So the download gain is ALWAYS below 1: we fetch 10 MB (v1) + deltas instead
  of just 10 MB. Delta encoding as we built it can only win on upload time (max
  4 / (2 + 3*d_i), about 1.7-1.85) and on storage.

**Measured inputs (point 5)**, `results/task3_deltas.csv`, averages over the 10 fruits:

| version | (a) delta vs v1 | (b) delta vs previous |
|---|---|---|
| v2 | 316 kB (3.2 %) | 316 kB (3.2 %) |
| v3 | 451 kB (4.5 %) | 226 kB (2.3 %) |
| v4 | 530 kB (5.3 %) | 218 kB (2.2 %) |
| v5 | 583 kB (5.8 %) | 214 kB (2.1 %) |

- encode about 195 ms -> R_enc = about 0.43 Gbit/s, decode about 83 ms -> R_dec =
  about 1.0 Gbit/s (both include the temp files pyxdelta needs).
- (a) deltas grow with every version because v_i shares less and less with v1,
  (b) deltas stay small because each version keeps 90 % of the one before.
- The deltas are much smaller than the 1 MB that changes per version because
  xdelta also compresses the new text it has to add.
- Storage for one fruit with 5 versions (1 replica): Task 1 = 50 MB, Task 3 (a) =
  11.9 MB, Task 3 (b) = 11.0 MB. The deltas alone are only 1.9 MB / 1.0 MB, the
  uncompressed v1 costs the other 10 MB. Each extra version adds only 0.2-0.6 MB.

**Predicted gains vs Task 1** from the formulas:

| R | version | G_up (a) | G_down (a) | G_up (b) | G_down (b) |
|---|---|---|---|---|---|
| 100 Mbit/s | v2 | 1.72 | 0.94 | 1.72 | 0.94 |
| 100 Mbit/s | v5 | 1.66 | 0.93 | 1.49 | 0.80 |
| 1 Gbit/s | v2 | 0.90 | 0.66 | 0.90 | 0.66 |
| 1 Gbit/s | v5 | 0.88 | 0.65 | 0.53 | 0.32 |
| 3 Gbit/s (our localhost "R") | v2 | 0.44 | 0.39 | 0.44 | 0.39 |
| 3 Gbit/s (our localhost "R") | v5 | 0.43 | 0.39 | 0.22 | 0.14 |

**Measured (point 6)**, all versions together, avg / median in seconds:

| option | N | upload | download |
|---|---|---|---|
| (a) first | 6 | 0.276 / 0.299 | 0.129 / 0.136 |
| (a) first | 12 | 0.269 / 0.288 | 0.120 / 0.126 |
| (a) first | 24 | 0.271 / 0.297 | 0.121 / 0.130 |
| (b) previous | 6 | 0.271 / 0.289 | 0.172 / 0.165 |
| (b) previous | 12 | 0.286 / 0.294 | 0.181 / 0.178 |
| (b) previous | 24 | 0.287 / 0.301 | 0.185 / 0.178 |

(min / max and the encode / decode / reference times are in `results/task3_summary.csv`)

Per version it looks very different (N = 6, avg seconds, 20 samples each,
all N are in `results/task3_by_version.csv`):

| version | (a) upload | (a) download | (b) upload | (b) download |
|---|---|---|---|---|
| v1 (full) | 0.119 | 0.058 | 0.101 | 0.044 |
| v2 | 0.264 | 0.129 | 0.227 | 0.113 |
| v3 | 0.323 | 0.162 | 0.288 | 0.171 |
| v4 | 0.323 | 0.149 | 0.340 | 0.232 |
| v5 | 0.352 | 0.148 | 0.400 | 0.301 |

**Gains (point 7)**, Task 1 avg time / Task 3 avg time, all versions together:

| option | N | upload vs Task 1 | download vs Task 1 |
|---|---|---|---|
| (a) | 6 | 0.36 | 0.41 |
| (a) | 12 | 0.39 | 0.45 |
| (a) | 24 | 0.34 | 0.43 |
| (b) | 6 | 0.37 | 0.31 |
| (b) | 12 | 0.36 | 0.30 |
| (b) | 24 | 0.32 | 0.28 |

(per version in `results/task3_gains.csv`. The comparison with Task 2 that point 7
asks for gets added once the Task 2 results are in; `benchmark_task3.py` picks up
`results/task2_summary.csv` automatically and adds the "vs Task 2" columns.)

Per version at N = 6 against Task 1, to compare with the 3 Gbit/s rows above:
(a) v2: G_up 0.38, G_down 0.41 - (a) v5: 0.28, 0.36 - (b) v5: 0.25, 0.18.
Predicted were 0.44 / 0.39, 0.43 / 0.39 and 0.22 / 0.14, so the model is in the
right range and gets the ordering right.

What to say about it:
- On localhost delta encoding is slower than Task 1, for upload and download. The "network" is so fast (3 Gbit/s) that the xdelta processing and
  the extra 10 MB fetch of v1 cost far more than the smaller transfers save.
- Option (a): download time is flat from v2 on (always 2 blobs, 1 decode), upload
  grows slowly because the deltas get bigger.
- Option (b): upload and download both grow by about 60 ms per version, one more
  blob to fetch and one more 10 MB decode. That is the chain. The deltas are
  smaller than in (a), but on a fast link that doesn't matter.
- On a slow link the picture flips for uploads: at 100 Mbit/s the model gives
  G_up = 1.5-1.7 for both options, because sending 3 small deltas instead of 3
  full files saves more than fetching v1 costs. Downloads stay slower than Task 1.
- Easy improvements we did NOT do (worth mentioning): compress v1 too (zlib or
  xdelta's own compression), keep the reference version cached on the lead, and
  fetch the blobs of a chain in parallel.

## Still to do

- Task 2 (Chris), then add the Task 2 comparison to the Task 3 section.
- Write the report (about 10 pages). Everything in the "notes for the report"
  sections above is raw material for it.
- Decide if the result CSVs should be shared (`results/` is in .gitignore).

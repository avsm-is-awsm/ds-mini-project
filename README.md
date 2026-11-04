# DS Mini Project - Distributed Storage

Task 1 (replication) is done. Tasks 2 and 3 will build on top of this.

## Setup

```
python -m venv .venv
.venv\Scripts\activate          (Windows)   /   source .venv/bin/activate
pip install -r requirements.txt
```

Unzip `DataSet.zip` into a folder called `dataset/` in the project root (it's in .gitignore, 500MB).

If you change `messages.proto`, regenerate the python file with:

```
protoc --python_out=. messages.proto
```

## Running it

```
python start_system.py -n 6
```

That starts the lead node (REST API on port 5000, node RPC on 5001) and 6 storage
nodes (zmq on ports 5101-5106). Logs end up in `data/logs/`, the replicas in
`data/nodeXX/`. Every start wipes `data/` unless you pass `--keep`.

Then in another terminal:

```
python client.py upload dataset/apple_v1.txt
python client.py upload dataset/apple_v2.txt
python client.py list
python client.py download apple.txt                (latest = v2)
python client.py download apple.txt --version 1
python client.py nodes
python client.py delete apple.txt
```

Or import `postman/DS_MiniProject_Task1.postman_collection.json` in Postman
(for the upload pick a file in the form-data field `file`).

## How it works

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

## Task 1 measurements

```
python benchmark_task1.py --nodes 6 12 24 --repeat 100
```

For each N it starts a fresh system, uploads the dataset files one by one
(apple_v1..v5, banana_v1.. etc.) and downloads each one straight after, 100 times.
Results: `results/task1_samples_N*.csv` (every sample) and `results/task1_summary.csv`
(avg / median / min / max).

- upload time = request sent -> 201 back (3 replicas stored + fsynced)
- download time = request sent -> last byte received
- the file is read into memory before the timer starts

Everything runs on one machine, so the "network" is localhost.

## Task 1 analysis (notes for the report)

B = file size in bytes, R = link bandwidth in bit/s, same for every link.
Assumptions: the lead stores and forwards (it gets the whole file before sending
it on), and we ignore latency, processing and disk time.

**Upload (point 2).** Client -> lead is `8B/R`. Then the lead sends 3 full copies.
They all go out over the lead's own link, so sending them in parallel or one after
the other takes the same time, `3 * 8B/R`:

    T_up = 8B/R + 3 * 8B/R = 32B/R

(If every lead -> node connection had its own R, the 3 copies would overlap and
`T_up = 8B/R + 8B/R = 16B/R`. Worth saying in the report which assumption we use.)

**Download (point 3).** The request is tiny, so we ignore it. The node sends the
file to the lead, then the lead sends it to the client:

    T_down = 8B/R + 8B/R = 16B/R

(If the lead streamed the bytes through without waiting for the whole file, this
would get close to `8B/R`. Ours doesn't do that.)

Example: B = 10 MB = 10^7 bytes, R = 1 Gbit/s -> 8B/R = 0.08 s, so T_up = 0.32 s, T_down = 0.16 s.

**What to look at in the results (point 4).**
- The number of nodes N shouldn't change much because we always write exactly 3
  copies, N only changes which nodes get picked. Small differences come from having
  more processes on the same machine (more heartbeats, memory, CPU).
- The model says upload/download = 32/16 = 2. Compare that with the measured ratio
  of the averages.
- If you work out the "effective R" from the measured times (R = 16B / T_down etc.)
  you get the throughput of localhost + Python + disk, not a real network link.

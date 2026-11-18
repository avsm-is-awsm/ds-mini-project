"""
Lead node (Task 3 - replication + delta encoding).

Same system as Task 1, but only the first version of a file is stored in full.
Every later version is stored as a VCDIFF delta (xdelta3, through pyxdelta)
against an older version, and the delta is what gets replicated to 3 nodes.

Two modes, picked with --delta-mode when the lead starts:
  first     option (a) from the assignment: the delta of v_i is always made
            against v1. To read v_i we need v1 + one delta.
  previous  option (b): the delta of v_i is made against v_(i-1), so the
            versions form a chain. To read v_i we need v1 + all deltas up to i.

The storage nodes didn't change, they have no idea if a blob is a full file
or a delta.

What lives here:
  - REST API with Flask for the client / Postman (upload, download, list, delete)
  - metadata in SQLite: which file, which version, which 3 nodes hold it, and
    for a delta which version it was made against
  - every upload gets (delta) encoded, then copied to 3 different storage nodes
    over zmq (protobuf header frame + raw bytes frame), all 3 in parallel
  - tinyrpc JSON-RPC endpoint where the storage nodes register / heartbeat
  - an apscheduler job that marks nodes dead if the heartbeats stop

Normally started by start_system.py together with the nodes.
"""
from gevent import monkey
monkey.patch_all()

import argparse
import atexit
import io
import os
import random
import re
import sqlite3
import tempfile
import time
import uuid
import zlib

import gevent
import gevent.lock
import gevent.pywsgi
import gevent.queue
import pyxdelta
import zmq.green as zmq  # green zmq, otherwise one slow node blocks every other request
from apscheduler.schedulers.background import BackgroundScheduler
from flask import Flask, make_response, g, request, send_file
from tinyrpc.dispatch import RPCDispatcher
from tinyrpc.protocols.jsonrpc import JSONRPCProtocol
from tinyrpc.server.gevent import RPCServerGreenlets
from tinyrpc.transports.wsgi import WsgiServerTransport

import messages_pb2

REPLICAS = 3
NODE_TIMEOUT = 6.0        # seconds without heartbeat -> we stop picking that node
ZMQ_TIMEOUT_MS = 30_000  # for store/get, 10MB can take a moment on a slow disk
CONTROL_TIMEOUT_MS = 3_000  # for small things like delete

DELTA_MODE = "first"      # "first" = option (a), "previous" = option (b), see --delta-mode

DB_PATH = "data/lead.db"  # set from the command line in main()

app = Flask(__name__)
app.config["MAX_CONTENT_LENGTH"] = 200 * 1024 * 1024

context = zmq.Context()

# node_id -> {"address": "tcp://...", "last_seen": time, "alive": bool}
# only kept in memory, the nodes register again if the lead restarts
nodes = {}

# node_id -> (REQ socket, lock). One long lived socket per node, see ask_node()
node_sockets = {}


# ---------------------------------------------------------------- database

def get_db():
    db = getattr(g, "_database", None)
    if db is None:
        db = g._database = sqlite3.connect(DB_PATH)
        db.row_factory = sqlite3.Row
    return db


@app.teardown_appcontext
def close_db(exception):
    db = getattr(g, "_database", None)
    if db is not None:
        db.close()


def init_db():
    db = sqlite3.connect(DB_PATH)
    db.executescript("""
        CREATE TABLE IF NOT EXISTS files (
            id          INTEGER PRIMARY KEY AUTOINCREMENT,
            base_name   TEXT NOT NULL,      -- apple.txt
            version     INTEGER NOT NULL,   -- 3
            filename    TEXT NOT NULL,      -- apple_v3.txt
            size        INTEGER NOT NULL,   -- original size (B)
            crc32       INTEGER NOT NULL,   -- of the original bytes
            encoding    TEXT NOT NULL,      -- 'full' or 'vcdiff'
            ref_version INTEGER,            -- vcdiff only: version the delta was made against
            stored_size INTEGER NOT NULL,   -- size of what's on the nodes (B_delta,i)
            stored_crc32 INTEGER NOT NULL,  -- of the stored blob
            storage_id  TEXT NOT NULL,      -- blob name on the nodes
            created     REAL NOT NULL,
            UNIQUE (base_name, version)
        );
        CREATE TABLE IF NOT EXISTS replicas (
            file_id  INTEGER NOT NULL,
            node_id  TEXT NOT NULL
        );
    """)
    db.commit()
    db.close()


# ---------------------------------------------------------------- versions

# apple_v3.txt -> stem "apple", version 3, ext ".txt"
VERSION_RE = re.compile(r"^(?P<stem>.+)_v(?P<version>\d+)(?P<ext>\.[^.]*)?$")


def split_version(filename):
    """apple_v3.txt -> ("apple.txt", 3). No _vN in the name -> (name, None)."""
    m = VERSION_RE.match(filename)
    if m is None:
        return filename, None
    return m.group("stem") + (m.group("ext") or ""), int(m.group("version"))


def versioned_name(base_name, version):
    stem, ext = os.path.splitext(base_name)
    return f"{stem}_v{version}{ext}"


def find_file(db, name):
    """Works with "apple.txt" (latest), "apple_v2.txt" or "apple.txt?version=2"."""
    base_name, version = split_version(name)
    version = request.args.get("version", default=version, type=int)
    if version is None:
        return db.execute(
            "SELECT * FROM files WHERE base_name = ? ORDER BY version DESC LIMIT 1",
            (base_name,)).fetchone()
    return db.execute(
        "SELECT * FROM files WHERE base_name = ? AND version = ?",
        (base_name, version)).fetchone()


# ---------------------------------------------------------------- nodes

def alive_nodes():
    return [node_id for node_id, info in nodes.items() if info["alive"]]


def forget_socket(node_id):
    entry = node_sockets.pop(node_id, None)
    if entry is not None:
        entry[0].close()


def ask_node(node_id, header, data=None, timeout_ms=ZMQ_TIMEOUT_MS):
    """Send one request to a storage node and wait for its answer.
    Returns (StorageReply, payload bytes or None)."""
    # Christian - one REQ socket per node that we keep open, plus a lock so only
    # one request per node is in flight (REQ must strictly go send/recv/send/recv,
    # and the node handles one request at a time anyway).
    # Also: DON'T use sock.poll() with zmq.green. It puts a second gevent watcher
    # on the same zmq fd, and on Windows (libuv) that corrupted the heap and
    # crashed the lead every few uploads. Took me ages to find. RCVTIMEO + recv
    # does the same job using the socket's own watcher.
    if node_id not in node_sockets:
        sock = context.socket(zmq.REQ)
        sock.setsockopt(zmq.LINGER, 0)
        sock.connect(nodes[node_id]["address"])
        node_sockets[node_id] = (sock, gevent.lock.BoundedSemaphore())
    sock, lock = node_sockets[node_id]

    frames = [header.SerializeToString()]
    if data is not None:
        frames.append(data)

    with lock:
        sock.setsockopt(zmq.RCVTIMEO, timeout_ms)
        sock.send_multipart(frames)
        try:
            parts = sock.recv_multipart()
        except zmq.Again:
            # a REQ socket that never got its reply is stuck, throw it away
            forget_socket(node_id)
            raise TimeoutError(f"{node_id} didn't answer within {timeout_ms} ms")

    reply = messages_pb2.StorageReply()
    reply.ParseFromString(parts[0])
    return reply, (parts[1] if len(parts) > 1 else None)


def store_replica(node_id, storage_id, data, crc):
    header = messages_pb2.StorageRequest(
        type=messages_pb2.StorageRequest.STORE,
        storage_id=storage_id, crc32=crc, size=len(data))
    try:
        reply, _ = ask_node(node_id, header, data)
    except Exception as e:
        print(f"[lead] storing {storage_id} on {node_id} failed: {e}")
        return False
    if not reply.ok:
        print(f"[lead] {node_id} refused {storage_id}: {reply.error}")
    return reply.ok


def fetch_replica(node_id, storage_id):
    header = messages_pb2.StorageRequest(
        type=messages_pb2.StorageRequest.GET, storage_id=storage_id)
    try:
        reply, data = ask_node(node_id, header)
    except Exception as e:
        print(f"[lead] reading {storage_id} from {node_id} failed: {e}")
        return None
    return data if reply.ok else None


def delete_replicas(storage_id, node_ids):
    """Best effort, if a node is down the blob just stays there (orphaned).
    Dead nodes are skipped, otherwise we'd sit in the timeout for nothing."""
    header = messages_pb2.StorageRequest(
        type=messages_pb2.StorageRequest.DELETE, storage_id=storage_id)

    def delete_one(node_id):
        try:
            ask_node(node_id, header, timeout_ms=CONTROL_TIMEOUT_MS)
        except Exception as e:
            print(f"[lead] deleting {storage_id} on {node_id} failed: {e}")

    targets = [n for n in node_ids if n in nodes and nodes[n]["alive"]]
    gevent.joinall([gevent.spawn(delete_one, n) for n in targets])


def replicate(storage_id, data, crc):
    """Put full copies on REPLICAS different nodes. Returns the nodes that acked."""
    candidates = alive_nodes()
    random.shuffle(candidates)
    stored = []

    # Christian - if one of the 3 fails we try the next alive node instead of
    # failing the whole upload. Only when we run out of nodes it's an error.
    while len(stored) < REPLICAS and candidates:
        need = REPLICAS - len(stored)
        batch, candidates = candidates[:need], candidates[need:]
        jobs = {n: gevent.spawn(store_replica, n, storage_id, data, crc) for n in batch}
        gevent.joinall(list(jobs.values()))
        stored += [n for n, job in jobs.items() if job.value]

    return stored


# ---------------------------------------------------------------- delta encoding

def _write(path, data):
    with open(path, "wb") as f:
        f.write(data)


def _read(path):
    with open(path, "rb") as f:
        return f.read()


# pyxdelta only takes file paths, so source/target/patch go through a temp folder.
# Those disk writes are part of the encode/decode time we measure, which is fair,
# the xdelta3 command line tool would do exactly the same.

def xdelta_encode(source, target):
    """VCDIFF delta that turns `source` into `target`."""
    with tempfile.TemporaryDirectory() as tmp:
        src, tgt, patch = (os.path.join(tmp, n) for n in ("source", "target", "patch"))
        _write(src, source)
        _write(tgt, target)
        if not pyxdelta.run(src, tgt, patch):
            raise RuntimeError("xdelta3 could not create the delta")
        return _read(patch)


def xdelta_decode(source, delta):
    """Apply a VCDIFF delta to `source`, gives back the target."""
    with tempfile.TemporaryDirectory() as tmp:
        src, patch, out = (os.path.join(tmp, n) for n in ("source", "patch", "out"))
        _write(src, source)
        _write(patch, delta)
        if not pyxdelta.decode(src, patch, out):
            raise RuntimeError("xdelta3 could not apply the delta")
        return _read(out)


def load_chain(db, base_name, version):
    """Everything needed to rebuild `version`, oldest first: the full file and
    then the deltas in the order they have to be applied.
    Mode 'first' gives [v1, delta_i], mode 'previous' gives [v1, delta_2, ..., delta_i].
    Each entry is (file row, nodes holding that blob), so the db isn't needed afterwards."""
    chain = []
    while version is not None:
        row = db.execute("SELECT * FROM files WHERE base_name = ? AND version = ?",
                         (base_name, version)).fetchone()
        if row is None:
            raise LookupError(f"{base_name} version {version} is missing, can't rebuild from it")
        holders = [r["node_id"] for r in
                   db.execute("SELECT node_id FROM replicas WHERE file_id = ?", (row["id"],))]
        chain.append((row, holders))
        version = row["ref_version"]
    chain.reverse()
    return chain


def fetch_blob(row, holders):
    """Get one stored blob from any node that has it and check its crc."""
    # random order spreads the reads over the 3 copies, alive nodes go first
    holders = list(holders)
    random.shuffle(holders)
    holders.sort(key=lambda n: not (n in nodes and nodes[n]["alive"]))

    for node_id in holders:
        if node_id not in nodes:
            continue
        blob = fetch_replica(node_id, row["storage_id"])
        if blob is not None and zlib.crc32(blob) == row["stored_crc32"]:
            return blob, node_id
    raise IOError(f"no replica of {row['filename']} could be read")


def rebuild(chain):
    """Fetch every blob of the chain and apply the deltas. Returns (data, stats)."""
    # Christian - the blobs are fetched one after the other on purpose, that's
    # what the analysis assumes (sum of all B_delta over the lead's link).
    # Fetching them in parallel would be an easy speedup for mode 'previous'.
    stats = {"blobs": 0, "fetched_bytes": 0, "fetch_s": 0.0, "decode_s": 0.0, "nodes": []}
    data = None
    for row, holders in chain:
        t0 = time.perf_counter()
        blob, node_id = fetch_blob(row, holders)
        stats["fetch_s"] += time.perf_counter() - t0
        stats["blobs"] += 1
        stats["fetched_bytes"] += len(blob)
        stats["nodes"].append(node_id)

        if row["encoding"] == "full":
            data = blob
        else:
            t0 = time.perf_counter()
            data = xdelta_decode(data, blob)
            stats["decode_s"] += time.perf_counter() - t0

    wanted = chain[-1][0]
    if zlib.crc32(data) != wanted["crc32"]:
        raise IOError(f"{wanted['filename']} is corrupt after decoding")
    return data, stats


# ---------------------------------------------------------------- REST API

@app.route("/files", methods=["POST"])
def upload_file():
    uploaded = request.files.get("file")
    if uploaded is None or not uploaded.filename:
        return make_response({"error": "send the file as form-data in a field called 'file'"}, 400)

    filename = os.path.basename(uploaded.filename)
    data = uploaded.read()
    base_name, version = split_version(filename)

    # is this a brand new file or a new version of something we already have?
    db = get_db()
    existing = sorted(row["version"] for row in
                      db.execute("SELECT version FROM files WHERE base_name = ?", (base_name,)))
    if version is None:
        version = max(existing, default=0) + 1
    elif version in existing:
        return make_response({"error": f"{base_name} version {version} is already stored"}, 409)
    elif existing and version < existing[-1]:
        # a delta always points back at an older version, so we can't slot one in afterwards
        return make_response({"error": f"versions have to be uploaded in order, "
                                       f"{base_name} is already at version {existing[-1]}"}, 409)

    # new file -> stored in full. New version -> delta against the first version
    # (option a) or against the newest one we have (option b)
    ref_version, chain = None, []
    if existing:
        ref_version = existing[0] if DELTA_MODE == "first" else existing[-1]
        try:
            chain = load_chain(db, base_name, ref_version)
        except LookupError as e:
            return make_response({"error": str(e)}, 500)

    if len(alive_nodes()) < REPLICAS:
        return make_response({"error": f"need {REPLICAS} alive nodes, have {len(alive_nodes())}"}, 503)

    # don't keep the db connection open while we wait for the nodes
    close_db(None)
    g._database = None

    crc = zlib.crc32(data)
    blob, encoding = data, "full"
    reference_s = encode_s = 0.0
    if ref_version is not None:
        # the lead doesn't keep old versions around, it has to get the reference
        # back from the storage nodes first (for option b that means the whole chain)
        t0 = time.perf_counter()
        try:
            reference, _ = rebuild(chain)
        except IOError as e:
            return make_response({"error": f"can't read the reference version: {e}"}, 503)
        reference_s = time.perf_counter() - t0

        t0 = time.perf_counter()
        delta = xdelta_encode(reference, data)
        encode_s = time.perf_counter() - t0
        if len(delta) < len(data):
            blob, encoding = delta, "vcdiff"
        else:
            ref_version = None  # completely different content, a delta would only cost us

    blob_crc = zlib.crc32(blob)
    storage_id = uuid.uuid4().hex
    stored_on = replicate(storage_id, blob, blob_crc)
    if len(stored_on) < REPLICAS:
        delete_replicas(storage_id, stored_on)
        return make_response({"error": f"only {len(stored_on)} of {REPLICAS} replicas could be stored"}, 503)

    db = get_db()
    try:
        cur = db.execute(
            "INSERT INTO files (base_name, version, filename, size, crc32, encoding, ref_version, "
            "stored_size, stored_crc32, storage_id, created) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)",
            (base_name, version, versioned_name(base_name, version), len(data), crc, encoding,
             ref_version, len(blob), blob_crc, storage_id, time.time()))
    except sqlite3.IntegrityError:
        # two uploads of the same version at the same time, the other one won
        delete_replicas(storage_id, stored_on)
        return make_response({"error": f"{base_name} version {version} is already stored"}, 409)
    db.executemany("INSERT INTO replicas (file_id, node_id) VALUES (?, ?)",
                   [(cur.lastrowid, n) for n in stored_on])
    db.commit()

    what = "full" if encoding == "full" else f"delta vs v{ref_version}"
    print(f"[lead] {versioned_name(base_name, version)} ({len(data)} B -> {len(blob)} B, {what}) "
          f"-> {', '.join(stored_on)}")
    return make_response({
        "id": cur.lastrowid,
        "filename": versioned_name(base_name, version),
        "base_name": base_name,
        "version": version,
        "new_file": not existing,
        "previous_versions": existing,
        "size": len(data),
        "delta_mode": DELTA_MODE,
        "encoding": encoding,
        "ref_version": ref_version,
        "stored_size": len(blob),
        "reference_s": reference_s,   # time to get the reference version back from the nodes
        "encode_s": encode_s,
        "nodes": stored_on,
    }, 201)


@app.route("/files", methods=["GET"])
def list_files():
    rows = get_db().execute("""
        SELECT base_name, MAX(version) AS latest_version, COUNT(*) AS versions,
               SUM(size) AS total_size, SUM(stored_size) AS total_stored_size
        FROM files GROUP BY base_name ORDER BY base_name
    """).fetchall()
    return make_response({"files": [dict(r) for r in rows]})


@app.route("/files/<path:name>", methods=["GET"])
def download_file(name):
    db = get_db()
    row = find_file(db, name)
    if row is None:
        return make_response({"error": f"{name} not found"}, 404)

    try:
        chain = load_chain(db, row["base_name"], row["version"])
    except LookupError as e:
        return make_response({"error": str(e)}, 500)
    close_db(None)
    g._database = None

    try:
        data, stats = rebuild(chain)
    except IOError as e:
        return make_response({"error": str(e)}, 503)

    response = send_file(io.BytesIO(data), mimetype="application/octet-stream",
                         as_attachment=True, download_name=row["filename"])
    response.headers["X-File-Version"] = str(row["version"])
    response.headers["X-Served-By"] = ", ".join(stats["nodes"])
    response.headers["X-Encoding"] = row["encoding"]
    response.headers["X-Stored-Size"] = str(row["stored_size"])
    response.headers["X-Blobs-Fetched"] = str(stats["blobs"])
    response.headers["X-Fetched-Bytes"] = str(stats["fetched_bytes"])
    response.headers["X-Decode-Seconds"] = f"{stats['decode_s']:.6f}"
    return response


@app.route("/files/<path:name>", methods=["DELETE"])
def delete_file(name):
    """apple.txt deletes all versions. apple_v3.txt only works for the newest
    version, older ones may be the reference of a delta."""
    db = get_db()
    base_name, version = split_version(name)
    rows = db.execute("SELECT * FROM files WHERE base_name = ? ORDER BY version",
                      (base_name,)).fetchall()
    if version is not None:
        if rows and version != rows[-1]["version"] and any(r["version"] == version for r in rows):
            return make_response({"error": f"only the newest version (v{rows[-1]['version']}) can be "
                                           f"deleted on its own, newer deltas may depend on v{version}"}, 409)
        rows = [r for r in rows if r["version"] == version]
    if not rows:
        return make_response({"error": f"{name} not found"}, 404)

    for row in rows:
        holders = [r["node_id"] for r in
                   db.execute("SELECT node_id FROM replicas WHERE file_id = ?", (row["id"],))]
        delete_replicas(row["storage_id"], holders)
        db.execute("DELETE FROM replicas WHERE file_id = ?", (row["id"],))
        db.execute("DELETE FROM files WHERE id = ?", (row["id"],))
    db.commit()
    return make_response({"deleted": [r["filename"] for r in rows]})


@app.route("/nodes", methods=["GET"])
def list_nodes():
    now = time.time()
    return make_response({
        "alive": len(alive_nodes()),
        "nodes": [{"id": node_id,
                   "address": info["address"],
                   "alive": info["alive"],
                   "last_heartbeat_s_ago": round(now - info["last_seen"], 1)}
                  for node_id, info in sorted(nodes.items())],
    })


# ---------------------------------------------------------------- RPC for the nodes

dispatcher = RPCDispatcher()


@dispatcher.public
def register(node_id, address):
    again = node_id in nodes
    if again and nodes[node_id]["address"] != address:
        forget_socket(node_id)
    nodes[node_id] = {"address": address, "last_seen": time.time(), "alive": True}
    print(f"[lead] {node_id} {'registered again' if again else 'registered'} at {address}")
    return True


@dispatcher.public
def heartbeat(node_id):
    info = nodes.get(node_id)
    if info is None:
        return False  # tells the node to register again
    info["last_seen"] = time.time()
    if not info["alive"]:
        info["alive"] = True
        print(f"[lead] {node_id} is back")
    return True


def check_nodes():
    now = time.time()
    for node_id, info in list(nodes.items()):
        if info["alive"] and now - info["last_seen"] > NODE_TIMEOUT:
            info["alive"] = False
            print(f"[lead] no heartbeat from {node_id} for {NODE_TIMEOUT}s, marking it dead")


def main():
    global DB_PATH, DELTA_MODE

    parser = argparse.ArgumentParser(description="Lead node")
    parser.add_argument("--delta-mode", choices=["first", "previous"], default=DELTA_MODE,
                        help="first = delta against v1 (option a), previous = against the last version (option b)")
    parser.add_argument("--host", default="127.0.0.1")
    parser.add_argument("--port", type=int, default=5000, help="REST API port")
    parser.add_argument("--rpc-port", type=int, default=5001, help="port the nodes register on")
    parser.add_argument("--db", default="data/lead.db")
    parser.add_argument("--fresh", action="store_true", help="start with an empty metadata db")
    parser.add_argument("--access-log", action="store_true", help="print every HTTP request")
    args = parser.parse_args()

    DB_PATH = args.db
    DELTA_MODE = args.delta_mode
    os.makedirs(os.path.dirname(DB_PATH) or ".", exist_ok=True)
    if args.fresh and os.path.exists(DB_PATH):
        os.remove(DB_PATH)
    init_db()

    # tinyrpc server for register/heartbeat, runs next to Flask in the same process
    rpc_transport = WsgiServerTransport(queue_class=gevent.queue.Queue)
    rpc_http = gevent.pywsgi.WSGIServer((args.host, args.rpc_port), rpc_transport.handle, log=None)
    gevent.spawn(rpc_http.serve_forever)
    rpc_server = RPCServerGreenlets(rpc_transport, JSONRPCProtocol(), dispatcher)
    gevent.spawn(rpc_server.serve_forever)

    scheduler = BackgroundScheduler()
    scheduler.add_job(check_nodes, "interval", seconds=2)
    scheduler.start()
    atexit.register(lambda: scheduler.shutdown(wait=False))

    print(f"[lead] REST API on http://{args.host}:{args.port}, node RPC on port {args.rpc_port}, "
          f"delta mode '{DELTA_MODE}'")
    http = gevent.pywsgi.WSGIServer((args.host, args.port), app,
                                    log="default" if args.access_log else None)
    try:
        http.serve_forever()
    except KeyboardInterrupt:
        pass


if __name__ == "__main__":
    main()

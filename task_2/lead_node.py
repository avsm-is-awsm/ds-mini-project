"""
Lead node (Task 1 - full replication).

What lives here:
  - REST API with Flask for the client / Postman (upload, download, list, delete)
  - metadata in SQLite: which file, which version, which 3 nodes hold it
  - every upload gets copied in full to 3 different storage nodes over zmq
    (protobuf header frame + raw bytes frame), all 3 in parallel
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
import time
import uuid
import zlib

import gevent
import gevent.lock
import gevent.pywsgi
import gevent.queue
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
            size        INTEGER NOT NULL,
            crc32       INTEGER NOT NULL,
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
    existing = [row["version"] for row in
                db.execute("SELECT version FROM files WHERE base_name = ?", (base_name,))]
    if version is None:
        version = max(existing, default=0) + 1
    elif version in existing:
        return make_response({"error": f"{base_name} version {version} is already stored"}, 409)

    if len(alive_nodes()) < REPLICAS:
        return make_response({"error": f"need {REPLICAS} alive nodes, have {len(alive_nodes())}"}, 503)

    # don't keep the db connection open while we wait for the nodes
    close_db(None)
    g._database = None

    crc = zlib.crc32(data)
    storage_id = uuid.uuid4().hex
    stored_on = replicate(storage_id, data, crc)
    if len(stored_on) < REPLICAS:
        delete_replicas(storage_id, stored_on)
        return make_response({"error": f"only {len(stored_on)} of {REPLICAS} replicas could be stored"}, 503)

    db = get_db()
    try:
        cur = db.execute(
            "INSERT INTO files (base_name, version, filename, size, crc32, storage_id, created) "
            "VALUES (?, ?, ?, ?, ?, ?, ?)",
            (base_name, version, versioned_name(base_name, version), len(data), crc, storage_id, time.time()))
    except sqlite3.IntegrityError:
        # two uploads of the same version at the same time, the other one won
        delete_replicas(storage_id, stored_on)
        return make_response({"error": f"{base_name} version {version} is already stored"}, 409)
    db.executemany("INSERT INTO replicas (file_id, node_id) VALUES (?, ?)",
                   [(cur.lastrowid, n) for n in stored_on])
    db.commit()

    print(f"[lead] {versioned_name(base_name, version)} ({len(data)} B) -> {', '.join(stored_on)}")
    return make_response({
        "id": cur.lastrowid,
        "filename": versioned_name(base_name, version),
        "base_name": base_name,
        "version": version,
        "new_file": not existing,
        "previous_versions": sorted(existing),
        "size": len(data),
        "nodes": stored_on,
    }, 201)


@app.route("/files", methods=["GET"])
def list_files():
    rows = get_db().execute("""
        SELECT base_name, MAX(version) AS latest_version, COUNT(*) AS versions
        FROM files GROUP BY base_name ORDER BY base_name
    """).fetchall()
    return make_response({"files": [dict(r) for r in rows]})


@app.route("/files/<path:name>", methods=["GET"])
def download_file(name):
    db = get_db()
    row = find_file(db, name)
    if row is None:
        return make_response({"error": f"{name} not found"}, 404)

    holders = [r["node_id"] for r in
               db.execute("SELECT node_id FROM replicas WHERE file_id = ?", (row["id"],))]
    # random order spreads the reads over the 3 copies, alive nodes go first
    random.shuffle(holders)
    holders.sort(key=lambda n: not (n in nodes and nodes[n]["alive"]))

    for node_id in holders:
        if node_id not in nodes:
            continue
        data = fetch_replica(node_id, row["storage_id"])
        if data is not None and zlib.crc32(data) == row["crc32"]:
            break
    else:
        return make_response({"error": f"no replica of {row['filename']} could be read"}, 503)

    response = send_file(io.BytesIO(data), mimetype="application/octet-stream",
                         as_attachment=True, download_name=row["filename"])
    response.headers["X-File-Version"] = str(row["version"])
    response.headers["X-Served-By"] = node_id
    return response


@app.route("/files/<path:name>", methods=["DELETE"])
def delete_file(name):
    """apple.txt deletes all versions, apple_v2.txt only that one."""
    db = get_db()
    base_name, version = split_version(name)
    if version is None:
        rows = db.execute("SELECT * FROM files WHERE base_name = ?", (base_name,)).fetchall()
    else:
        rows = db.execute("SELECT * FROM files WHERE base_name = ? AND version = ?",
                          (base_name, version)).fetchall()
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
    global DB_PATH

    parser = argparse.ArgumentParser(description="Lead node")
    parser.add_argument("--host", default="127.0.0.1")
    parser.add_argument("--port", type=int, default=5000, help="REST API port")
    parser.add_argument("--rpc-port", type=int, default=5001, help="port the nodes register on")
    parser.add_argument("--db", default="data/lead.db")
    parser.add_argument("--fresh", action="store_true", help="start with an empty metadata db")
    parser.add_argument("--access-log", action="store_true", help="print every HTTP request")
    args = parser.parse_args()

    DB_PATH = args.db
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

    print(f"[lead] REST API on http://{args.host}:{args.port}, node RPC on port {args.rpc_port}")
    http = gevent.pywsgi.WSGIServer((args.host, args.port), app,
                                    log="default" if args.access_log else None)
    try:
        http.serve_forever()
    except KeyboardInterrupt:
        pass


if __name__ == "__main__":
    main()

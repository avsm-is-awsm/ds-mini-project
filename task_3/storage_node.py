"""
Storage node.

Every node is its own process with a zmq REP socket. It doesn't know anything
about file names or versions, it just stores / returns / deletes blobs by the
storage_id the lead gives it. On startup it registers with the lead over
tinyrpc and then keeps sending heartbeats so the lead knows it's alive.

    python storage_node.py --id node01 --port 5101

(start_system.py does this for all N nodes, you normally don't run it by hand)
"""
import argparse
import atexit
import os
import re
import time
import zlib

import boto3
import zmq
from apscheduler.schedulers.background import BackgroundScheduler
from botocore.exceptions import ClientError
from tinyrpc import RPCClient
from tinyrpc.protocols.jsonrpc import JSONRPCProtocol
from tinyrpc.transports.http import HttpPostClientTransport

import messages_pb2

HEARTBEAT_INTERVAL = 2  # seconds, lead drops us after ~6s of silence

# storage ids are uuid4().hex from the lead, anything else gets rejected
# so nobody can send us "../../something" as a key
STORAGE_ID_RE = re.compile(r"^[0-9a-f]{32}$")


class LocalDiskBackend:
    """Default backend, one file per blob in the node's own folder."""

    def __init__(self, folder):
        self.folder = folder
        os.makedirs(folder, exist_ok=True)

    def _path(self, key):
        return os.path.join(self.folder, key)

    def put(self, key, data):
        # write to a tmp file + fsync, then rename. We only ack the lead after
        # this returns, so an ack really means the replica is on disk
        tmp = self._path(key) + ".tmp"
        with open(tmp, "wb") as f:
            f.write(data)
            f.flush()
            os.fsync(f.fileno())
        os.replace(tmp, self._path(key))

    def get(self, key):
        with open(self._path(key), "rb") as f:
            return f.read()

    def delete(self, key):
        try:
            os.remove(self._path(key))
        except FileNotFoundError:
            pass


class S3Backend:
    """Same interface but the blobs go to an S3 bucket (or MinIO with --s3-endpoint).
    Not used for the Task 1 numbers, it's here so we can try nodes on AWS later.
    Credentials come from the normal boto3 places (env vars / ~/.aws)."""

    def __init__(self, bucket, prefix, endpoint_url=None):
        self.s3 = boto3.client("s3", endpoint_url=endpoint_url)
        self.bucket = bucket
        self.prefix = prefix.rstrip("/") + "/"
        # fail at startup instead of on the first upload if the bucket is wrong
        self.s3.head_bucket(Bucket=bucket)

    def put(self, key, data):
        self.s3.put_object(Bucket=self.bucket, Key=self.prefix + key, Body=bytes(data))

    def get(self, key):
        try:
            obj = self.s3.get_object(Bucket=self.bucket, Key=self.prefix + key)
        except ClientError as e:
            if e.response["Error"]["Code"] in ("NoSuchKey", "404"):
                raise FileNotFoundError(key)
            raise
        return obj["Body"].read()

    def delete(self, key):
        self.s3.delete_object(Bucket=self.bucket, Key=self.prefix + key)


def handle(frames, backend, node_id):
    """Takes the incoming zmq frames, returns the frames to send back."""
    req = messages_pb2.StorageRequest()
    req.ParseFromString(frames[0].bytes)
    reply = messages_pb2.StorageReply(node_id=node_id)
    payload = None

    try:
        if not STORAGE_ID_RE.match(req.storage_id):
            raise ValueError(f"bad storage id {req.storage_id!r}")

        if req.type == messages_pb2.StorageRequest.STORE:
            data = frames[1].buffer
            if zlib.crc32(data) != req.crc32:
                reply.error = "checksum mismatch, not storing it"
            else:
                backend.put(req.storage_id, data)
                reply.ok = True
                reply.size = len(data)

        elif req.type == messages_pb2.StorageRequest.GET:
            payload = backend.get(req.storage_id)
            reply.ok = True
            reply.size = len(payload)
            reply.crc32 = zlib.crc32(payload)

        elif req.type == messages_pb2.StorageRequest.DELETE:
            backend.delete(req.storage_id)
            reply.ok = True

    except FileNotFoundError:
        reply.error = "not found"
    except Exception as e:
        reply.error = f"{type(e).__name__}: {e}"

    if reply.error:
        print(f"[{node_id}] request failed: {reply.error}")

    out = [reply.SerializeToString()]
    if reply.ok and payload is not None:
        out.append(payload)
    return out


def register_with_lead(lead, node_id, address):
    # the lead might come up a second after us, so just keep trying
    while True:
        try:
            lead.register(node_id, address)
            print(f"[{node_id}] registered with lead")
            return
        except Exception:
            time.sleep(0.5)


def send_heartbeat(lead, node_id, address):
    try:
        if not lead.heartbeat(node_id):
            # lead doesn't know us anymore (it was restarted), register again
            lead.register(node_id, address)
            print(f"[{node_id}] lead forgot about us, registered again")
    except Exception as e:
        print(f"[{node_id}] heartbeat failed: {e}")


def main():
    parser = argparse.ArgumentParser(description="Storage node")
    parser.add_argument("--id", default="node01")
    parser.add_argument("--host", default="127.0.0.1")
    parser.add_argument("--port", type=int, default=5101)
    parser.add_argument("--lead-rpc", default="http://127.0.0.1:5001/")
    parser.add_argument("--data-dir", default=None, help="default: data/<id>")
    parser.add_argument("--s3-bucket", default=None, help="store blobs in S3 instead of local disk")
    parser.add_argument("--s3-endpoint", default=None, help="e.g. http://localhost:9000 for MinIO")
    args = parser.parse_args()

    node_id = args.id
    if args.s3_bucket:
        backend = S3Backend(args.s3_bucket, prefix=node_id, endpoint_url=args.s3_endpoint)
    else:
        backend = LocalDiskBackend(args.data_dir or os.path.join("data", node_id))

    address = f"tcp://{args.host}:{args.port}"
    context = zmq.Context()
    socket = context.socket(zmq.REP)
    socket.bind(address)
    print(f"[{node_id}] listening on {address}")

    lead = RPCClient(
        JSONRPCProtocol(),
        HttpPostClientTransport(args.lead_rpc, timeout=5),
    ).get_proxy()
    register_with_lead(lead, node_id, address)

    scheduler = BackgroundScheduler()
    scheduler.add_job(send_heartbeat, "interval", seconds=HEARTBEAT_INTERVAL,
                      args=[lead, node_id, address])
    scheduler.start()
    atexit.register(lambda: scheduler.shutdown(wait=False))

    try:
        while True:
            frames = socket.recv_multipart(copy=False)
            socket.send_multipart(handle(frames, backend, node_id), copy=False)
    except KeyboardInterrupt:
        pass
    finally:
        socket.close(linger=0)
        context.term()


if __name__ == "__main__":
    main()

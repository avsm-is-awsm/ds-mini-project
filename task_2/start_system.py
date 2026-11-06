"""
Starts the lead + N storage nodes as separate processes on this machine.

    python start_system.py -n 6

Logs go to data/logs/. Ctrl+C stops everything.
The benchmark imports start_system()/stop_system() from here so the startup
code is only in one place.
"""
import argparse
import os
import shutil
import subprocess
import sys
import time

import requests

HERE = os.path.dirname(os.path.abspath(__file__))


def start_system(n_nodes, data_dir="data", port=5000, rpc_port=5001,
                 node_base_port=5100, fresh=True):
    data_dir = os.path.abspath(data_dir)
    if fresh and os.path.isdir(data_dir):
        shutil.rmtree(data_dir)
    log_dir = os.path.join(data_dir, "logs")
    os.makedirs(log_dir, exist_ok=True)

    procs = []

    def launch(name, script, *args):
        with open(os.path.join(log_dir, f"{name}.log"), "w") as log:
            procs.append(subprocess.Popen(
                [sys.executable, "-u", os.path.join(HERE, script), *args],
                cwd=HERE, stdout=log, stderr=subprocess.STDOUT))

    launch("lead", "lead_node.py",
           "--port", str(port), "--rpc-port", str(rpc_port),
           "--db", os.path.join(data_dir, "lead.db"))

    for i in range(1, n_nodes + 1):
        node_id = f"node{i:02d}"
        launch(node_id, "storage_node.py",
               "--id", node_id,
               "--port", str(node_base_port + i),
               "--lead-rpc", f"http://127.0.0.1:{rpc_port}/",
               "--data-dir", os.path.join(data_dir, node_id))
    return procs


def wait_until_ready(procs, n_nodes, lead_url="http://127.0.0.1:5000", timeout=60):
    """Blocks until the lead answers and all n_nodes have registered."""
    deadline = time.time() + timeout
    while time.time() < deadline:
        dead = [p for p in procs if p.poll() is not None]
        if dead:
            raise RuntimeError(f"{len(dead)} process(es) exited during startup, check data/logs")
        try:
            if requests.get(f"{lead_url}/nodes", timeout=1).json()["alive"] >= n_nodes:
                return
        except requests.RequestException:
            pass
        time.sleep(0.3)
    raise TimeoutError(f"system not ready after {timeout}s")


def stop_system(procs):
    for p in procs:
        if p.poll() is not None:
            continue
        if os.name == "nt":
            # the venv python.exe on Windows is only a launcher that starts the real
            # interpreter as a child, terminate() only kills the launcher -> kill the tree
            subprocess.run(["taskkill", "/F", "/T", "/PID", str(p.pid)], capture_output=True)
        else:
            p.terminate()
    for p in procs:
        try:
            p.wait(timeout=5)
        except subprocess.TimeoutExpired:
            p.kill()


def main():
    parser = argparse.ArgumentParser(description="Start the lead node and N storage nodes")
    parser.add_argument("-n", "--nodes", type=int, default=6)
    parser.add_argument("--port", type=int, default=5000)
    parser.add_argument("--data-dir", default="data")
    parser.add_argument("--keep", action="store_true", help="keep files from the last run")
    args = parser.parse_args()

    if args.nodes < 3:
        parser.error("need at least 3 nodes for 3 replicas")

    procs = start_system(args.nodes, data_dir=args.data_dir, port=args.port, fresh=not args.keep)
    try:
        wait_until_ready(procs, args.nodes, f"http://127.0.0.1:{args.port}")
        print(f"System up: lead on http://127.0.0.1:{args.port} with {args.nodes} storage nodes")
        print("Ctrl+C to stop")
        while True:
            time.sleep(1)
            for p in list(procs):
                if p.poll() is not None:
                    print(f"process {p.args[2]} exited with code {p.returncode}")
                    procs.remove(p)
    except KeyboardInterrupt:
        print("stopping...")
    finally:
        stop_system(procs)


if __name__ == "__main__":
    main()

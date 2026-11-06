"""
Small command line client, quicker than clicking around in Postman.

    python client.py upload dataset/apple_v1.txt
    python client.py download apple.txt                 (latest version)
    python client.py download apple.txt --version 2 -o apple_v2_back.txt
    python client.py list
    python client.py delete apple.txt                   (all versions)
    python client.py nodes
"""
import argparse
import json
import os
import time

import requests


def main():
    parser = argparse.ArgumentParser()
    parser.add_argument("--url", default="http://127.0.0.1:5000")
    sub = parser.add_subparsers(dest="cmd", required=True)

    p = sub.add_parser("upload")
    p.add_argument("path")

    p = sub.add_parser("download")
    p.add_argument("name")
    p.add_argument("--version", type=int)
    p.add_argument("-o", "--output")

    sub.add_parser("list")
    sub.add_parser("nodes")

    p = sub.add_parser("delete")
    p.add_argument("name")

    args = parser.parse_args()

    if args.cmd == "upload":
        with open(args.path, "rb") as f:
            data = f.read()
        start = time.perf_counter()
        r = requests.post(f"{args.url}/files", files={"file": (os.path.basename(args.path), data)})
        took = time.perf_counter() - start
        print(r.status_code, json.dumps(r.json(), indent=2))
        print(f"upload took {took:.3f}s")

    elif args.cmd == "download":
        params = {"version": args.version} if args.version else None
        start = time.perf_counter()
        r = requests.get(f"{args.url}/files/{args.name}", params=params)
        took = time.perf_counter() - start
        if r.status_code != 200:
            print(r.status_code, r.text)
            return
        out = args.output or f"downloaded_{os.path.basename(args.name)}"
        with open(out, "wb") as f:
            f.write(r.content)
        print(f"got version {r.headers.get('X-File-Version')} from {r.headers.get('X-Served-By')}, "
              f"{len(r.content)} bytes -> {out} in {took:.3f}s")

    elif args.cmd == "list":
        print(json.dumps(requests.get(f"{args.url}/files").json(), indent=2))

    elif args.cmd == "nodes":
        print(json.dumps(requests.get(f"{args.url}/nodes").json(), indent=2))

    elif args.cmd == "delete":
        r = requests.delete(f"{args.url}/files/{args.name}")
        print(r.status_code, json.dumps(r.json(), indent=2))


if __name__ == "__main__":
    main()

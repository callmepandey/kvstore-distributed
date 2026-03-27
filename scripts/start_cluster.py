#!/usr/bin/env python3
"""
Start a local N-node KV Store cluster.

Each node gets its own TCP port, HTTP port, WAL dir, and snapshot dir.
All nodes are wired as peers of each other.

Usage:
  python scripts/start_cluster.py --nodes 3
  python scripts/start_cluster.py --nodes 5 --base-tcp-port 7200 --base-http-port 8200

Port layout (defaults):
  Node 0: tcp=7100  http=8100
  Node 1: tcp=7101  http=8101
  Node N: tcp=7100+N  http=8100+N
"""

from __future__ import annotations

import argparse
import os
import signal
import subprocess
import sys
import time


def parse_args() -> argparse.Namespace:
    p = argparse.ArgumentParser(description="Start a local KV Store cluster")
    p.add_argument("--nodes", type=int, default=3, help="Number of nodes (default: 3)")
    p.add_argument("--base-tcp-port", type=int, default=7100, help="TCP port for node 0 (default: 7100)")
    p.add_argument("--base-http-port", type=int, default=8100, help="HTTP port for node 0 (default: 8100)")
    p.add_argument("--data-dir", type=str, default="/tmp/kvcluster", help="Base data directory")
    p.add_argument("--log-level", type=str, default="WARNING", choices=["DEBUG", "INFO", "WARNING", "ERROR"])
    p.add_argument("--snapshot-interval", type=int, default=3600, help="Snapshot interval in seconds")
    p.add_argument("--anti-entropy-interval", type=int, default=30, help="Anti-entropy interval in seconds (default: 30)")
    return p.parse_args()


def main() -> None:
    args = parse_args()
    n = args.nodes

    if n < 1:
        print("ERROR: --nodes must be >= 1", file=sys.stderr)
        sys.exit(1)

    tcp_ports  = [args.base_tcp_port  + i for i in range(n)]
    http_ports = [args.base_http_port + i for i in range(n)]

    # repo root = parent of this script's directory
    repo_root = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))
    python = sys.executable

    processes: list[subprocess.Popen] = []

    print(f"\nStarting {n}-node cluster\n{'─' * 40}")

    for i in range(n):
        peers = ",".join(
            f"localhost:{tcp_ports[j]}" for j in range(n) if j != i
        )
        cmd = [
            python, "-m", "src",
            "--node-id",            str(i),
            "--port",               str(tcp_ports[i]),
            "--http-port",          str(http_ports[i]),
            "--data-dir",           args.data_dir,
            "--snapshot-interval",  str(args.snapshot_interval),
            "--log-level",          args.log_level,
        ]
        if peers:
            cmd += ["--peers", peers]
        cmd += ["--anti-entropy-interval", str(args.anti_entropy_interval)]

        proc = subprocess.Popen(cmd, cwd=repo_root)
        processes.append(proc)
        print(f"  Node {i}  tcp={tcp_ports[i]}  http={http_ports[i]}  "
              f"dashboard → http://localhost:{http_ports[i]}")

    quorum_w = n // 2 + 1
    quorum_r = n // 2 + 1
    print(f"\nQuorum  N={n}  W={quorum_w}  R={quorum_r}  "
          f"(tolerates {n - quorum_w} node failure{'s' if n - quorum_w != 1 else ''})")
    print("\nPress Ctrl+C to stop all nodes.\n")

    # Give nodes a moment to bind ports
    time.sleep(1.5)

    def shutdown(sig, frame):
        print("\nStopping cluster…")
        for proc in processes:
            proc.terminate()
        for proc in processes:
            try:
                proc.wait(timeout=5)
            except subprocess.TimeoutExpired:
                proc.kill()
        print("All nodes stopped.")
        sys.exit(0)

    signal.signal(signal.SIGINT, shutdown)
    signal.signal(signal.SIGTERM, shutdown)

    # Wait for any node to exit unexpectedly
    while True:
        for i, proc in enumerate(processes):
            if proc.poll() is not None:
                print(f"\nWARNING: Node {i} exited unexpectedly (code {proc.returncode})")
        time.sleep(2)


if __name__ == "__main__":
    main()

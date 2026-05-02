#!/usr/bin/env python3.14t
"""ByteCaskDB Replicated Task Server — Granian entry point.

Runs a Vikunja-compatible task server with leader/follower replication.
Leader and follower run as separate processes communicating over HTTP.

Usage:
  # Start leader
  python3.14t run_server.py --role leader --port 8100 --data-dir ./leader_data

  # Start follower (separate terminal/machine)
  python3.14t run_server.py --role follower --port 8101 --data-dir ./follower_data --leader-url http://localhost:8100

  # Start follower with bootstrap (copies sealed files first)
  python3.14t run_server.py --role follower --port 8101 --data-dir ./follower_data --leader-url http://localhost:8100 --bootstrap

The vikunja CLI works against either:
  VIKUNJA_URL=http://localhost:8100 vikunja list         # reads from leader
  VIKUNJA_URL=http://localhost:8101 vikunja list         # reads from follower
  VIKUNJA_URL=http://localhost:8101 vikunja add backlog "task"  # proxied to leader
"""

from __future__ import annotations

import argparse
import logging
import sys
import sysconfig

from bytecaskdb import _bytecaskdb as _bc

from bytecaskdb_replication.replicator import ReplicationFollower, ReplicationLeader
from bytecaskdb_replication.server import create_app
from bytecaskdb_replication.storage import TaskStorage

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s [%(levelname)s] %(name)s: %(message)s",
)
log = logging.getLogger("run_server")


def check_free_threaded() -> None:
    if sysconfig.get_config_var("Py_GIL_DISABLED") != 1:
        log.warning(
            "GIL is enabled — replication primitives require free-threaded Python 3.14t. "
            "Run with: python3.14t run_server.py ..."
        )


def open_db(data_dir: str, role: str) -> _bc.DB:
    opts = _bc.Options()
    if role == "follower":
        opts.initial_mode = _bc.Mode.Follower
    return _bc.DB.open(data_dir, opts)


def main() -> None:
    parser = argparse.ArgumentParser(description="ByteCaskDB Replicated Task Server")
    parser.add_argument("--role", choices=["leader", "follower"], default="leader")
    parser.add_argument("--port", type=int, default=8100)
    parser.add_argument("--host", default="0.0.0.0")
    parser.add_argument("--data-dir", default="./task_data")
    parser.add_argument("--leader-url", default=None, help="Leader URL (required for follower)")
    parser.add_argument("--bootstrap", action="store_true",
                        help="Bootstrap follower from leader's manifest before starting")
    parser.add_argument("--workers", type=int, default=1)
    args = parser.parse_args()

    check_free_threaded()

    if args.role == "follower" and not args.leader_url:
        sys.exit("error: --leader-url is required for follower role")

    if args.bootstrap:
        if args.role != "follower":
            sys.exit("error: --bootstrap only applies to follower role")
        ReplicationFollower.bootstrap(args.leader_url, args.data_dir)

    log.info("opening database at %s (role=%s)", args.data_dir, args.role)
    db = open_db(args.data_dir, args.role)
    storage = TaskStorage(db)

    leader: ReplicationLeader | None = None
    follower: ReplicationFollower | None = None

    if args.role == "leader":
        storage.seed_defaults()
        leader = ReplicationLeader(db)
    else:
        follower = ReplicationFollower(db, args.leader_url)
        follower.start()

    app = create_app(
        storage=storage,
        leader=leader,
        follower=follower,
        leader_url=args.leader_url,
    )

    log.info("starting %s on %s:%d", args.role, args.host, args.port)

    from granian import Granian
    from granian.constants import Interfaces

    server = Granian(
        target="bytecaskdb_replication.server",
        address=args.host,
        port=args.port,
        interface=Interfaces.WSGI,
        workers=args.workers,
    )
    server.serve(target_loader=lambda: app, wrap_loader=False)


if __name__ == "__main__":
    main()

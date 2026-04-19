#!/usr/bin/env python3
"""Run the blob storage HTTP server.

Uses Granian with WSGI and free-threaded Python for true thread-level
parallelism: each request handler runs synchronously in its own OS thread,
calling ByteCaskDB directly without async overhead or executor wrappers.
"""

import argparse
import logging
import sys
import warnings

from granian import Granian
from granian.constants import Interfaces

from bytecaskdb_blob.server import create_app


def main():
    if hasattr(sys, "_is_gil_enabled") and sys._is_gil_enabled():
        warnings.warn(
            "The GIL is enabled — free-threaded Python (python3.XXt) is "
            "recommended for best performance.",
            RuntimeWarning,
            stacklevel=1,
        )

    parser = argparse.ArgumentParser(description="ByteCaskDB Blob Server (Granian)")
    parser.add_argument("--data-dir", default="./blob_data",
                        help="Directory for database files (default: ./blob_data)")
    parser.add_argument("--host", default="0.0.0.0", help="Bind host")
    parser.add_argument("--port", type=int, default=8080, help="Bind port")
    parser.add_argument("--chunk-size", type=int, default=4 * 1024 * 1024,
                        help="Chunk size in bytes (default: 4 MiB)")
    parser.add_argument("--workers", type=int, default=1,
                        help="Number of Granian workers (default: 1)")
    parser.add_argument("--blocking-threads", type=int, default=None,
                        help="Blocking threads per worker (default: auto)")
    parser.add_argument("--backpressure", type=int, default=None,
                        help="Max concurrent requests per worker (default: auto)")
    parser.add_argument("--vacuum-idle-interval", type=float, default=30.0,
                        help="Seconds between vacuum passes when idle (default: 30)")
    parser.add_argument("--vacuum-busy-interval", type=float, default=1.0,
                        help="Seconds between vacuum passes when work remains (default: 1)")
    parser.add_argument("--log-level", default="info",
                        choices=["critical", "error", "warning", "info", "debug"],
                        help="Log level (default: info)")
    args = parser.parse_args()

    py_level = getattr(logging, args.log_level.upper(), logging.INFO)

    _handler = logging.StreamHandler(sys.stderr)
    _handler.setFormatter(logging.Formatter("%(levelname)s %(name)s %(message)s"))
    _blob_log = logging.getLogger("bytecaskdb_blob")
    _blob_log.setLevel(py_level)
    _blob_log.addHandler(_handler)
    _blob_log.propagate = False

    _blob_log.debug("bytecaskdb_blob debug logging active (level=%s)", args.log_level)

    app = create_app(data_dir=args.data_dir, chunk_size=args.chunk_size,
                     vacuum_busy_interval=args.vacuum_busy_interval,
                     vacuum_idle_interval=args.vacuum_idle_interval)

    granian_kwargs = {}
    if args.blocking_threads is not None:
        granian_kwargs["blocking_threads"] = args.blocking_threads
    if args.backpressure is not None:
        granian_kwargs["backpressure"] = args.backpressure

    server = Granian(
        target="bytecaskdb_blob.server",  # unused with custom loader
        address=args.host,
        port=args.port,
        interface=Interfaces.WSGI,
        workers=args.workers,
        log_level=args.log_level,
        **granian_kwargs,
    )
    server.on_shutdown(app.close)

    _blob_log.info("starting Granian WSGI server on %s:%d (workers=%d)",
                   args.host, args.port, args.workers)
    server.serve(target_loader=lambda: app, wrap_loader=False)


if __name__ == "__main__":
    main()

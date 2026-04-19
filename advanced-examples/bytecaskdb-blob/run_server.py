#!/usr/bin/env python3
"""Run the blob storage HTTP server."""

import argparse
import logging

import uvicorn

from bytecaskdb_blob.server import create_app


def main():
    parser = argparse.ArgumentParser(description="ByteCaskDB Blob Server")
    parser.add_argument("--data-dir", default="./blob_data",
                        help="Directory for database files (default: ./blob_data)")
    parser.add_argument("--host", default="0.0.0.0", help="Bind host")
    parser.add_argument("--port", type=int, default=8080, help="Bind port")
    parser.add_argument("--chunk-size", type=int, default=4 * 1024 * 1024,
                        help="Chunk size in bytes (default: 4 MiB)")
    parser.add_argument("--log-level", default="error",
                        choices=["critical", "error", "warning", "info", "debug", "trace"],
                        help="Uvicorn log level (default: error)")
    args = parser.parse_args()

    py_level = logging.DEBUG if args.log_level in ("debug", "trace") else getattr(logging, args.log_level.upper(), logging.ERROR)

    # Configure the bytecaskdb_blob logger with its own handler so uvicorn's
    # internal dictConfig() call doesn't override it.
    import sys
    _handler = logging.StreamHandler(sys.stderr)
    _handler.setFormatter(logging.Formatter("%(levelname)s %(name)s %(message)s"))
    _blob_log = logging.getLogger("bytecaskdb_blob")
    _blob_log.setLevel(py_level)
    _blob_log.addHandler(_handler)
    _blob_log.propagate = False

    _blob_log.debug("bytecaskdb_blob debug logging active (level=%s)", args.log_level)

    app = create_app(data_dir=args.data_dir, chunk_size=args.chunk_size)
    uvicorn.run(app, host=args.host, port=args.port, log_level=args.log_level)


if __name__ == "__main__":
    main()

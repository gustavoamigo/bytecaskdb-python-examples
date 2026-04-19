#!/usr/bin/env python3
"""Run the blob storage HTTP server."""

import argparse

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
    args = parser.parse_args()

    app = create_app(data_dir=args.data_dir, chunk_size=args.chunk_size)
    uvicorn.run(app, host=args.host, port=args.port)


if __name__ == "__main__":
    main()

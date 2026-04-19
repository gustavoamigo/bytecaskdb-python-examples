# bytecaskdb-blob

A persistent blob storage engine built on [ByteCaskDB](https://github.com/gustavoamigo/bytecaskdb), with an HTTP API compatible with S3 clients (boto3, rclone, aws cli).

## Features

- **Folder-like hierarchy** via prefix-based keys
- **Chunked storage** for efficient handling of large objects (configurable chunk size, default 4 MiB)
- **Atomic uploads** — last chunk and metadata land together in a single batch
- **Range requests** (`Range: bytes=x-y`) for streaming and resumable downloads
- **Recursive prefix deletion** — delete millions of objects in a single append
- **Multipart uploads** with staging and atomic commit
- **Streaming uploads** via context manager
- **S3-compatible HTTP API** — works with boto3, rclone, and the aws cli

## Quick Start

### Prerequisites

Free-threaded Python (e.g. `python3.14t`) is strongly recommended. The server
uses [Granian](https://github.com/emmett-framework/granian) with WSGI, where
each request runs synchronously in its own OS thread. With free-threaded Python
there is no GIL, so the entire request pipeline — HTTP parsing in Granian's Rust
layer, Python request handling, and ByteCaskDB I/O — runs in true parallelism
across all threads. Without free-threading the GIL serialises the Python
portions, eliminating most of the concurrency benefit.

### Install dependencies

```bash
pip install -r requirements.txt
```

### Start the server

```bash
python run_server.py
```

The server listens on `http://localhost:8080` by default. Use `--data-dir`, `--host`, `--port`, and `--chunk-size` to customize.

### Use with boto3

```python
import boto3
from botocore.config import Config

s3 = boto3.client(
    "s3",
    endpoint_url="http://localhost:8080",
    aws_access_key_id="dummy",
    aws_secret_access_key="dummy",
    config=Config(signature_version="s3v4"),
    region_name="us-east-1",
)

s3.create_bucket(Bucket="my-bucket")
s3.put_object(Bucket="my-bucket", Key="hello.txt", Body=b"Hello, World!")
resp = s3.get_object(Bucket="my-bucket", Key="hello.txt")
print(resp["Body"].read())  # b'Hello, World!'
```

See `example_client.py` for a more complete demo.


## Key Layout

```
blob:{bucket}/{path}:meta          → JSON metadata (size, content_type, etag, ...)
blob:{bucket}/{path}:chunk:{nnnnn} → raw bytes (5-digit zero-padded index)
bucket:{name}:meta                 → bucket metadata
upload:{upload_id}:meta            → multipart upload metadata
```

## Design Notes

| Requirement | ByteCaskDB primitive |
|-------------|---------------------|
| List folder contents | In-memory radix tree prefix walk (no disk I/O) |
| Recursive folder delete | `delete_range` — single append regardless of object count |
| Atomic upload completion | `batch()` — last chunk + meta flip land together |
| Stream large blobs | Ordered iteration via `prefix()` |
| Fast metadata lookup | `:meta` keys read without touching chunk data |
| Multipart abort | `delete_range` on blob prefix + delete upload meta key |

## HTTP API

The server implements a minimal subset of the S3 protocol:

- `GET /` — list buckets
- `PUT / DELETE / HEAD /{bucket}` — bucket operations
- `PUT / GET / DELETE / HEAD /{bucket}/{key}` — object operations
- `GET /{bucket}?list-type=2&prefix=&delimiter=/` — list objects
- `POST /{bucket}/{key}?uploads` — create multipart upload
- `PUT /{bucket}/{key}?partNumber=N&uploadId=X` — upload part
- `POST /{bucket}/{key}?uploadId=X` — complete multipart upload
- `DELETE /{bucket}/{key}?uploadId=X` — abort multipart upload

## Benchmarking with warp

Install [warp](https://github.com/minio/warp) and run a mixed S3 benchmark against the server.

In one terminal, start the server:

```bash
python run_server.py --host "::"
```

In another terminal, create the bucket and run the benchmark:

```bash
# install Go first if you don't have it
sudo dnf install golang -y

# install warp
go install github.com/minio/warp@latest

# make sure ~/go/bin is in your PATH
export PATH=$PATH:~/go/bin

curl -X PUT http://localhost:8080/warp-test && warp mixed --host localhost:8080 --access-key dummy --secret-key dummy --bucket warp-test --concurrent 32 --duration 60s --tls=false --noclear --web  && curl -X DELETE "http://localhost:8080/warp-test?force=true"
```

warp writes result files (`*.csv.zst`) to the current directory — see `.gitignore`.

### Benchmark results

Tested on a single machine with Python 3.14t (free-threaded), Granian WSGI,
32 concurrent connections, 60 s mixed workload (10 MiB objects):

- **CPU:** AMD Ryzen 7 3700X 8-Core
- **RAM:** 32 GiB
- **Disk:** Samsung SSD 860 EVO 500 GB (SATA, ~485 MiB/s seq read, ~475 MiB/s seq write)

| Operation | Throughput | Obj/s | Median latency |
|-----------|-----------|-------|----------------|
| GET       | 338.5 MiB/s | 33.85 | 217 ms |
| PUT       | 108.5 MiB/s | 10.85 | 1282 ms |
| DELETE    | — | 7.14 | 485 ms |
| STAT      | — | 19.80 | 169 ms |
| **Total** | **446.0 MiB/s** | **71.57** | — |

Peak throughput reached 641.8 MiB/s (118.83 obj/s).

## Tests

```bash
# Storage engine tests (no HTTP)
pytest tests/test_storage.py -v

# HTTP server integration tests (uses boto3)
pytest tests/test_server.py -v
```

## Non-goals

- **Distributed storage.** Single-node engine. For multi-node, use MinIO or SeaweedFS.
- **Full S3 parity.** Versioning, ACLs, lifecycle policies, object lock, and replication are not implemented.
- **Authentication.** No signature verification. Run behind a trusted network boundary or add an auth proxy.

## License

MIT

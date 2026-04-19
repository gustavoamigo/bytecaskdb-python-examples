"""Integration tests for the blob HTTP server using boto3."""

import tempfile
import threading
import time

import boto3
import pytest
import uvicorn
from botocore.config import Config
from botocore.exceptions import ClientError

from bytecaskdb_blob.server import create_app


@pytest.fixture(scope="module")
def server():
    """Start the blob server in a background thread for the test module."""
    tmpdir = tempfile.mkdtemp()
    app = create_app(data_dir=tmpdir, chunk_size=256)

    config = uvicorn.Config(app, host="127.0.0.1", port=0, log_level="error")
    server = uvicorn.Server(config)

    # Find a free port
    import socket
    sock = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
    sock.bind(("127.0.0.1", 0))
    port = sock.getsockname()[1]
    sock.close()

    config = uvicorn.Config(app, host="127.0.0.1", port=port, log_level="error")
    server = uvicorn.Server(config)

    thread = threading.Thread(target=server.run, daemon=True)
    thread.start()

    # Wait for server to be ready
    for _ in range(50):
        try:
            s = socket.create_connection(("127.0.0.1", port), timeout=0.1)
            s.close()
            break
        except OSError:
            time.sleep(0.1)

    yield f"http://127.0.0.1:{port}"

    server.should_exit = True
    thread.join(timeout=5)


@pytest.fixture
def s3(server):
    return boto3.client(
        "s3",
        endpoint_url=server,
        aws_access_key_id="test",
        aws_secret_access_key="test",
        config=Config(signature_version="s3v4"),
        region_name="us-east-1",
    )


class TestBucketOperations:
    def test_create_and_list_buckets(self, s3):
        s3.create_bucket(Bucket="test-bucket-1")
        resp = s3.list_buckets()
        names = [b["Name"] for b in resp["Buckets"]]
        assert "test-bucket-1" in names

    def test_delete_bucket(self, s3):
        s3.create_bucket(Bucket="to-delete")
        s3.delete_bucket(Bucket="to-delete")
        resp = s3.list_buckets()
        names = [b["Name"] for b in resp["Buckets"]]
        assert "to-delete" not in names

    def test_head_bucket(self, s3):
        s3.create_bucket(Bucket="head-test")
        s3.head_bucket(Bucket="head-test")


class TestObjectOperations:
    def test_put_and_get(self, s3):
        s3.create_bucket(Bucket="obj-test")
        s3.put_object(Bucket="obj-test", Key="hello.txt", Body=b"Hello!")

        resp = s3.get_object(Bucket="obj-test", Key="hello.txt")
        assert resp["Body"].read() == b"Hello!"

    def test_head_object(self, s3):
        s3.create_bucket(Bucket="head-obj")
        s3.put_object(Bucket="head-obj", Key="f.txt", Body=b"abc",
                      ContentType="text/plain")
        resp = s3.head_object(Bucket="head-obj", Key="f.txt")
        assert resp["ContentLength"] == 3
        assert "ETag" in resp

    def test_delete_object(self, s3):
        s3.create_bucket(Bucket="del-obj")
        s3.put_object(Bucket="del-obj", Key="gone.txt", Body=b"bye")
        s3.delete_object(Bucket="del-obj", Key="gone.txt")

        with pytest.raises(ClientError) as exc_info:
            s3.get_object(Bucket="del-obj", Key="gone.txt")
        assert exc_info.value.response["Error"]["Code"] == "NoSuchKey"

    def test_get_nonexistent(self, s3):
        s3.create_bucket(Bucket="no-obj")
        with pytest.raises(ClientError) as exc_info:
            s3.get_object(Bucket="no-obj", Key="nope")
        assert exc_info.value.response["Error"]["Code"] == "NoSuchKey"

    def test_range_request(self, s3):
        s3.create_bucket(Bucket="range-test")
        s3.put_object(Bucket="range-test", Key="data.bin",
                      Body=b"0123456789")
        resp = s3.get_object(Bucket="range-test", Key="data.bin",
                             Range="bytes=2-5")
        assert resp["Body"].read() == b"2345"

    def test_large_object_chunked(self, s3):
        """Object larger than server chunk_size (256 bytes)."""
        s3.create_bucket(Bucket="big-obj")
        data = b"x" * 1000
        s3.put_object(Bucket="big-obj", Key="big.bin", Body=data)
        resp = s3.get_object(Bucket="big-obj", Key="big.bin")
        assert resp["Body"].read() == data

    def test_user_metadata(self, s3):
        s3.create_bucket(Bucket="meta-test")
        s3.put_object(Bucket="meta-test", Key="m.txt", Body=b"data",
                      Metadata={"author": "test", "version": "1"})
        resp = s3.head_object(Bucket="meta-test", Key="m.txt")
        assert resp["Metadata"]["author"] == "test"
        assert resp["Metadata"]["version"] == "1"


class TestListObjects:
    def test_list_flat(self, s3):
        s3.create_bucket(Bucket="list-flat")
        s3.put_object(Bucket="list-flat", Key="a.txt", Body=b"a")
        s3.put_object(Bucket="list-flat", Key="b.txt", Body=b"b")

        resp = s3.list_objects_v2(Bucket="list-flat")
        keys = [o["Key"] for o in resp.get("Contents", [])]
        assert sorted(keys) == ["a.txt", "b.txt"]

    def test_list_with_delimiter(self, s3):
        s3.create_bucket(Bucket="list-delim")
        s3.put_object(Bucket="list-delim", Key="photos/a.jpg", Body=b"a")
        s3.put_object(Bucket="list-delim", Key="photos/b.jpg", Body=b"b")
        s3.put_object(Bucket="list-delim", Key="docs/r.txt", Body=b"r")

        resp = s3.list_objects_v2(Bucket="list-delim", Delimiter="/")
        prefixes = [p["Prefix"] for p in resp.get("CommonPrefixes", [])]
        assert sorted(prefixes) == ["docs/", "photos/"]

    def test_list_with_prefix(self, s3):
        s3.create_bucket(Bucket="list-pfx")
        s3.put_object(Bucket="list-pfx", Key="logs/2024/jan.log", Body=b"j")
        s3.put_object(Bucket="list-pfx", Key="logs/2024/feb.log", Body=b"f")
        s3.put_object(Bucket="list-pfx", Key="logs/2025/mar.log", Body=b"m")

        resp = s3.list_objects_v2(Bucket="list-pfx", Prefix="logs/2024/",
                                  Delimiter="/")
        keys = [o["Key"] for o in resp.get("Contents", [])]
        assert sorted(keys) == ["logs/2024/feb.log", "logs/2024/jan.log"]


class TestMultipartUpload:
    def test_multipart_lifecycle(self, s3):
        s3.create_bucket(Bucket="mp-test")
        resp = s3.create_multipart_upload(Bucket="mp-test", Key="big.bin")
        upload_id = resp["UploadId"]

        part1 = s3.upload_part(Bucket="mp-test", Key="big.bin",
                               UploadId=upload_id, PartNumber=1,
                               Body=b"part1-" * 50)
        part2 = s3.upload_part(Bucket="mp-test", Key="big.bin",
                               UploadId=upload_id, PartNumber=2,
                               Body=b"part2-" * 50)

        s3.complete_multipart_upload(
            Bucket="mp-test", Key="big.bin", UploadId=upload_id,
            MultipartUpload={
                "Parts": [
                    {"PartNumber": 1, "ETag": part1["ETag"]},
                    {"PartNumber": 2, "ETag": part2["ETag"]},
                ]
            },
        )

        resp = s3.get_object(Bucket="mp-test", Key="big.bin")
        data = resp["Body"].read()
        assert data == b"part1-" * 50 + b"part2-" * 50

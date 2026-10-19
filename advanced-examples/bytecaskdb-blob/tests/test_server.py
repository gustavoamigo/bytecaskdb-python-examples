"""Integration tests for the blob HTTP server using boto3.

Exercises the WSGI server module (``bytecaskdb_blob.server``).
The WSGI app is served via stdlib's ``wsgiref.simple_server`` so that
Granian is not required to run the tests.
"""

import tempfile
import threading
import urllib.request

import boto3
import pytest
from botocore.config import Config
from botocore.exceptions import ClientError

from bytecaskdb_blob.server import create_app


@pytest.fixture(scope="module")
def server():
    """Start the Falcon WSGI blob server in a background thread."""
    from socketserver import ThreadingMixIn
    from wsgiref.simple_server import WSGIServer, WSGIRequestHandler, make_server

    class _ThreadedWSGIServer(ThreadingMixIn, WSGIServer):
        daemon_threads = True

    class _QuietHandler(WSGIRequestHandler):
        def log_message(self, format, *args):
            pass

    tmpdir = tempfile.mkdtemp()
    app = create_app(data_dir=tmpdir, chunk_size=256)

    httpd = make_server("127.0.0.1", 0, app,
                        server_class=_ThreadedWSGIServer,
                        handler_class=_QuietHandler)
    port = httpd.server_address[1]

    thread = threading.Thread(target=httpd.serve_forever, daemon=True)
    thread.start()

    yield f"http://127.0.0.1:{port}"

    httpd.shutdown()
    app.close()
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

    def test_head_bucket_not_found(self, s3):
        with pytest.raises(ClientError) as exc_info:
            s3.head_bucket(Bucket="nonexistent")
        assert exc_info.value.response["Error"]["Code"] in ("404", "NoSuchBucket")

    def test_bucket_location_with_trailing_slash(self, s3, server):
        # minio-go (used by warp) requests GET /bucket/?location=
        s3.create_bucket(Bucket="loc-test")
        with urllib.request.urlopen(f"{server}/loc-test/?location=") as resp:
            assert resp.status == 200
            assert b"LocationConstraint" in resp.read()


class TestObjectOperations:
    def test_put_and_get_object(self, s3):
        s3.create_bucket(Bucket="obj-bucket")
        s3.put_object(Bucket="obj-bucket", Key="hello.txt", Body=b"Hello, World!")
        resp = s3.get_object(Bucket="obj-bucket", Key="hello.txt")
        assert resp["Body"].read() == b"Hello, World!"

    def test_delete_object(self, s3):
        s3.create_bucket(Bucket="del-bucket")
        s3.put_object(Bucket="del-bucket", Key="gone.txt", Body=b"bye")
        s3.delete_object(Bucket="del-bucket", Key="gone.txt")
        with pytest.raises(ClientError) as exc_info:
            s3.get_object(Bucket="del-bucket", Key="gone.txt")
        assert exc_info.value.response["Error"]["Code"] == "NoSuchKey"

    def test_head_object(self, s3):
        s3.create_bucket(Bucket="head-obj-bucket")
        s3.put_object(Bucket="head-obj-bucket", Key="info.txt",
                      Body=b"data", ContentType="text/plain")
        resp = s3.head_object(Bucket="head-obj-bucket", Key="info.txt")
        assert resp["ContentLength"] == 4
        assert resp["ContentType"] == "text/plain"

    def test_get_object_not_found(self, s3):
        s3.create_bucket(Bucket="empty-bucket")
        with pytest.raises(ClientError) as exc_info:
            s3.get_object(Bucket="empty-bucket", Key="nope.txt")
        assert exc_info.value.response["Error"]["Code"] == "NoSuchKey"

    def test_large_object_chunked(self, s3):
        s3.create_bucket(Bucket="big-bucket")
        data = b"X" * 2048
        s3.put_object(Bucket="big-bucket", Key="big.bin", Body=data)
        resp = s3.get_object(Bucket="big-bucket", Key="big.bin")
        assert resp["Body"].read() == data

    def test_range_request(self, s3):
        s3.create_bucket(Bucket="range-bucket")
        s3.put_object(Bucket="range-bucket", Key="range.txt", Body=b"0123456789")
        resp = s3.get_object(Bucket="range-bucket", Key="range.txt",
                             Range="bytes=3-7")
        assert resp["Body"].read() == b"34567"

    def test_user_metadata(self, s3):
        s3.create_bucket(Bucket="meta-bucket")
        s3.put_object(Bucket="meta-bucket", Key="meta.txt", Body=b"hi",
                      Metadata={"author": "test", "version": "1"})
        resp = s3.head_object(Bucket="meta-bucket", Key="meta.txt")
        assert resp["Metadata"]["author"] == "test"
        assert resp["Metadata"]["version"] == "1"


class TestListObjects:
    def test_list_with_prefix(self, s3):
        s3.create_bucket(Bucket="list-bucket")
        s3.put_object(Bucket="list-bucket", Key="docs/a.txt", Body=b"a")
        s3.put_object(Bucket="list-bucket", Key="docs/b.txt", Body=b"b")
        s3.put_object(Bucket="list-bucket", Key="images/c.png", Body=b"c")
        resp = s3.list_objects_v2(Bucket="list-bucket", Prefix="docs/")
        keys = [obj["Key"] for obj in resp.get("Contents", [])]
        assert "docs/a.txt" in keys
        assert "docs/b.txt" in keys
        assert "images/c.png" not in keys

    def test_list_with_delimiter(self, s3):
        s3.create_bucket(Bucket="delim-bucket")
        s3.put_object(Bucket="delim-bucket", Key="a/1.txt", Body=b"1")
        s3.put_object(Bucket="delim-bucket", Key="b/2.txt", Body=b"2")
        s3.put_object(Bucket="delim-bucket", Key="root.txt", Body=b"r")
        resp = s3.list_objects_v2(Bucket="delim-bucket", Delimiter="/")
        prefixes = [p["Prefix"] for p in resp.get("CommonPrefixes", [])]
        assert "a/" in prefixes
        assert "b/" in prefixes


class TestMultipartUpload:
    def test_multipart_lifecycle(self, s3):
        s3.create_bucket(Bucket="mpu-bucket")
        mpu = s3.create_multipart_upload(Bucket="mpu-bucket", Key="big.bin")
        upload_id = mpu["UploadId"]

        part1 = s3.upload_part(Bucket="mpu-bucket", Key="big.bin",
                               UploadId=upload_id, PartNumber=1, Body=b"A" * 512)
        part2 = s3.upload_part(Bucket="mpu-bucket", Key="big.bin",
                               UploadId=upload_id, PartNumber=2, Body=b"B" * 512)

        s3.complete_multipart_upload(
            Bucket="mpu-bucket", Key="big.bin", UploadId=upload_id,
            MultipartUpload={"Parts": [
                {"PartNumber": 1, "ETag": part1["ETag"]},
                {"PartNumber": 2, "ETag": part2["ETag"]},
            ]},
        )

        resp = s3.get_object(Bucket="mpu-bucket", Key="big.bin")
        assert resp["Body"].read() == b"A" * 512 + b"B" * 512

    def test_abort_multipart_upload(self, s3):
        s3.create_bucket(Bucket="mpu-abort-bucket")
        mpu = s3.create_multipart_upload(Bucket="mpu-abort-bucket", Key="aborted.bin")
        upload_id = mpu["UploadId"]

        s3.upload_part(Bucket="mpu-abort-bucket", Key="aborted.bin",
                       UploadId=upload_id, PartNumber=1, Body=b"data")

        s3.abort_multipart_upload(Bucket="mpu-abort-bucket", Key="aborted.bin",
                                  UploadId=upload_id)

        with pytest.raises(ClientError):
            s3.get_object(Bucket="mpu-abort-bucket", Key="aborted.bin")

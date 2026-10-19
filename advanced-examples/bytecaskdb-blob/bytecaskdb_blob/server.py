"""Blob HTTP server with S3-compatible wire protocol (Falcon WSGI).

Designed for Granian with free-threaded Python.  Every request handler runs
synchronously in its own OS thread, calling ByteCaskDB directly without
async wrappers or thread-pool executors.  With free-threaded Python the GIL
is absent, so requests execute in true parallelism.
"""

from __future__ import annotations

import logging
import threading
from datetime import datetime, timezone
from urllib.parse import unquote

import falcon

from . import s3_xml
from .storage import (
    DEFAULT_BUFFER_POOL_BYTES,
    BlobStorage,
    BlobNotFoundError,
    BucketNotFoundError,
    BucketNotEmptyError,
    UploadNotFoundError,
    UploadInProgressError,
    BlobStorageError,
)

log = logging.getLogger(__name__)

__all__ = ["create_app"]


# ── Helpers ──────────────────────────────────────────────────────────────────

def _http_date(ts: float) -> str:
    return datetime.fromtimestamp(ts, tz=timezone.utc).strftime(
        "%a, %d %b %Y %H:%M:%S GMT"
    )


def _decode_aws_chunked(body: bytes) -> bytes:
    result: list[bytes] = []
    pos = 0
    end = len(body)
    while pos < end:
        crlf = body.find(b'\r\n', pos)
        if crlf == -1:
            break
        header = body[pos:crlf].split(b';', 1)[0]
        chunk_size = int(header, 16)
        pos = crlf + 2
        if chunk_size == 0:
            break
        result.append(body[pos:pos + chunk_size])
        pos += chunk_size + 2
    return b''.join(result)


def _is_aws_chunked(req: falcon.Request) -> bool:
    return bool(
        req.get_header("x-amz-decoded-content-length")
        or "aws-chunked" in (req.get_header("content-encoding") or "")
        or "STREAMING-" in (req.get_header("x-amz-content-sha256") or "")
    )


def _get_user_metadata(req: falcon.Request) -> dict[str, str]:
    meta = {}
    for key, value in req.env.items():
        if key.startswith("HTTP_X_AMZ_META_"):
            meta_key = key[16:].replace("_", "-").lower()
            meta[meta_key] = value
    return meta


def _set_user_metadata(resp: falcon.Response, user_metadata: dict[str, str]):
    for k, v in user_metadata.items():
        resp.set_header(f"x-amz-meta-{k}", v)


def _xml_error(resp: falcon.Response, status: str, code: str, message: str):
    resp.status = status
    resp.content_type = "application/xml"
    resp.data = s3_xml.error_xml(code, message)


def _xml_ok(resp: falcon.Response, data: bytes):
    resp.content_type = "application/xml"
    resp.data = data


# ── Resources ────────────────────────────────────────────────────────────────

class RootResource:
    def __init__(self, storage: BlobStorage):
        self.storage = storage

    def on_get(self, req: falcon.Request, resp: falcon.Response):
        buckets = self.storage.list_buckets()
        _xml_ok(resp, s3_xml.list_buckets_xml(buckets))


class BucketResource:
    def __init__(self, storage: BlobStorage):
        self.storage = storage

    def on_put(self, req: falcon.Request, resp: falcon.Response, bucket: str):
        self.storage.create_bucket(bucket)
        resp.status = falcon.HTTP_200

    def on_delete(self, req: falcon.Request, resp: falcon.Response, bucket: str):
        force = req.get_param("force", default="").lower() in ("true", "1")
        try:
            self.storage.delete_bucket(bucket, force=force)
        except BucketNotFoundError:
            _xml_error(resp, falcon.HTTP_404, "NoSuchBucket", f"Bucket not found: {bucket}")
            return
        except BucketNotEmptyError:
            _xml_error(resp, falcon.HTTP_409, "BucketNotEmpty", f"Bucket not empty: {bucket}")
            return
        resp.status = falcon.HTTP_204

    def on_head(self, req: falcon.Request, resp: falcon.Response, bucket: str):
        if not self.storage.bucket_exists(bucket):
            _xml_error(resp, falcon.HTTP_404, "NoSuchBucket", f"Bucket not found: {bucket}")

    def on_get(self, req: falcon.Request, resp: falcon.Response, bucket: str):
        if req.get_param("location") is not None:
            _xml_ok(resp, s3_xml.location_constraint_xml())
            return

        prefix = req.get_param("prefix", default="")
        delimiter = req.get_param("delimiter", default="/")

        common_prefixes, contents = self.storage.list_objects(
            bucket, prefix=prefix, delimiter=delimiter
        )
        _xml_ok(resp, s3_xml.list_objects_xml(bucket, prefix, delimiter, common_prefixes, contents))

    def on_post(self, req: falcon.Request, resp: falcon.Response, bucket: str):
        if req.get_param("delete") is not None:
            self._handle_delete_objects(req, resp, bucket)
            return
        _xml_error(resp, falcon.HTTP_501, "NotImplemented", "Not implemented")

    def _handle_delete_objects(self, req: falcon.Request, resp: falcon.Response, bucket: str):
        body = req.bounded_stream.read()
        try:
            keys = s3_xml.parse_delete_objects(body)
        except ValueError as e:
            _xml_error(resp, falcon.HTTP_400, "MalformedXML", str(e))
            return

        deleted = []
        errors = []
        for key in keys:
            try:
                self.storage.delete_object(bucket, key)
                deleted.append(key)
                log.debug("deleted %s/%s", bucket, key)
            except Exception as exc:
                log.warning("delete failed %s/%s: %s", bucket, key, exc)
                errors.append((key, str(exc)))

        _xml_ok(resp, s3_xml.delete_result_xml(deleted, errors))


class ObjectResource:
    def __init__(self, storage: BlobStorage):
        self.storage = storage

    def on_put(self, req: falcon.Request, resp: falcon.Response, bucket: str, key: str):
        key = unquote(key)
        part_number = req.get_param("partNumber")
        upload_id = req.get_param("uploadId")

        if part_number and upload_id:
            body = req.bounded_stream.read()
            if _is_aws_chunked(req):
                body = _decode_aws_chunked(body)
            log.debug("PUT_PART %s/%s part=%s upload=%s body_size=%d",
                      bucket, key, part_number, upload_id, len(body))
            try:
                etag = self.storage.upload_part(upload_id, int(part_number), body)
            except UploadNotFoundError:
                _xml_error(resp, falcon.HTTP_404, "NoSuchUpload", "Upload not found")
                return
            resp.set_header("ETag", f'"{etag}"')
            return

        content_type = req.content_type or "application/octet-stream"
        is_chunked = _is_aws_chunked(req)
        content_length = req.content_length
        user_metadata = _get_user_metadata(req)

        if not is_chunked and content_length is not None:
            meta = self.storage.put_object_stream(
                bucket, key, req.bounded_stream, content_length,
                content_type=content_type, user_metadata=user_metadata,
            )
            log.debug("PUT(stream) %s/%s body_size=%d stored_size=%d etag=%s",
                      bucket, key, content_length, meta["size"], meta["etag"])
        else:
            body = req.bounded_stream.read()
            if is_chunked:
                body = _decode_aws_chunked(body)
            meta = self.storage.put_object(
                bucket, key, body,
                content_type=content_type, user_metadata=user_metadata,
            )
            log.debug("PUT(buffered) %s/%s body_size=%d stored_size=%d etag=%s",
                      bucket, key, len(body), meta["size"], meta["etag"])

        resp.set_header("ETag", f'"{meta["etag"]}"')

    def on_get(self, req: falcon.Request, resp: falcon.Response, bucket: str, key: str):
        key = unquote(key)
        try:
            meta = self.storage.head_object(bucket, key)
        except BlobNotFoundError:
            _xml_error(resp, falcon.HTTP_404, "NoSuchKey", f"Object not found: {key}")
            return

        log.debug("GET %s/%s stored_size=%d chunk_count=%d chunk_size=%d",
                  bucket, key, meta["size"], meta["chunk_count"], meta.get("chunk_size", 0))

        resp.content_type = meta["content_type"]
        resp.set_header("ETag", f'"{meta["etag"]}"')
        resp.set_header("Last-Modified", _http_date(
            meta.get("completed_at", meta.get("created_at", 0))
        ))
        _set_user_metadata(resp, meta.get("user_metadata", {}))

        range_header = req.get_header("Range") or ""
        if range_header.startswith("bytes="):
            range_spec = range_header[6:]
            parts = range_spec.split("-")
            start = int(parts[0]) if parts[0] else 0
            end = int(parts[1]) if parts[1] else meta["size"] - 1
            end = min(end, meta["size"] - 1)

            data = self.storage.get_range(bucket, key, start, end)
            resp.status = falcon.HTTP_206
            resp.set_header("Content-Range", f"bytes {start}-{end}/{meta['size']}")
            resp.content_length = len(data)
            resp.data = data
            return

        resp.content_length = meta["size"]
        resp.stream = self.storage.stream_object(bucket, key)

    def on_delete(self, req: falcon.Request, resp: falcon.Response, bucket: str, key: str):
        key = unquote(key)
        upload_id = req.get_param("uploadId")

        if upload_id:
            try:
                self.storage.abort_multipart_upload(upload_id)
            except UploadNotFoundError:
                _xml_error(resp, falcon.HTTP_404, "NoSuchUpload", "Upload not found")
                return
            resp.status = falcon.HTTP_204
            return

        self.storage.delete_object(bucket, key)
        resp.status = falcon.HTTP_204

    def on_head(self, req: falcon.Request, resp: falcon.Response, bucket: str, key: str):
        key = unquote(key)
        try:
            meta = self.storage.head_object(bucket, key)
        except BlobNotFoundError:
            resp.status = falcon.HTTP_404
            return

        log.debug("HEAD %s/%s stored_size=%d chunk_count=%d chunk_size=%d",
                  bucket, key, meta["size"], meta["chunk_count"], meta.get("chunk_size", 0))

        resp.content_type = meta["content_type"]
        resp.set_header("ETag", f'"{meta["etag"]}"')
        resp.content_length = meta["size"]
        resp.set_header("Last-Modified", _http_date(
            meta.get("completed_at", meta.get("created_at", 0))
        ))
        _set_user_metadata(resp, meta.get("user_metadata", {}))

    def on_post(self, req: falcon.Request, resp: falcon.Response, bucket: str, key: str):
        key = unquote(key)

        if req.get_param("uploads") is not None:
            content_type = req.content_type or "application/octet-stream"
            try:
                upload_id = self.storage.create_multipart_upload(bucket, key, content_type)
            except UploadInProgressError:
                _xml_error(resp, falcon.HTTP_409, "OperationAborted",
                           f"A multipart upload already exists for {bucket}/{key}")
                return
            _xml_ok(resp, s3_xml.initiate_multipart_xml(bucket, key, upload_id))
            return

        upload_id = req.get_param("uploadId")
        if upload_id:
            try:
                meta = self.storage.complete_multipart_upload(upload_id)
            except UploadNotFoundError:
                _xml_error(resp, falcon.HTTP_404, "NoSuchUpload", "Upload not found")
                return
            except BlobStorageError as e:
                _xml_error(resp, falcon.HTTP_500, "InternalError", str(e))
                return

            log.debug("COMPLETE_MPU %s/%s stored_size=%d chunk_count=%d chunk_size=%d etag=%s",
                      bucket, key, meta["size"], meta["chunk_count"],
                      meta.get("chunk_size", 0), meta["etag"])
            _xml_ok(resp, s3_xml.complete_multipart_xml(bucket, key, meta["etag"]))
            return

        _xml_error(resp, falcon.HTTP_501, "NotImplemented", "Not implemented")


# ── Vacuum loop ──────────────────────────────────────────────────────────────

def _vacuum_loop(storage: BlobStorage, busy_interval: float, idle_interval: float, stop: threading.Event) -> None:
    while not stop.is_set():
        try:
            more_work = storage._db.vacuum()
        except Exception:
            log.exception("vacuum: error during pass")
            more_work = False
        stop.wait(timeout=busy_interval if more_work else idle_interval)


# ── App factory ──────────────────────────────────────────────────────────────

class BlobFalconApp:
    """Falcon WSGI application wrapping BlobStorage.

    Keeps references to storage and the vacuum thread so that ``close()``
    can tear them down cleanly.
    """

    def __init__(self, storage: BlobStorage, vacuum_busy_interval: float,
                 vacuum_idle_interval: float):
        self.storage = storage
        self._stop = threading.Event()
        self._vacuum = threading.Thread(
            target=_vacuum_loop,
            args=(storage, vacuum_busy_interval, vacuum_idle_interval, self._stop),
            name="bytecaskdb-vacuum",
            daemon=True,
        )
        self._vacuum.start()
        log.debug("vacuum thread started (busy=%.1fs idle=%.0fs)",
                  vacuum_busy_interval, vacuum_idle_interval)

        self.app = falcon.App()
        self.app.add_route("/", RootResource(storage))
        bucket_resource = BucketResource(storage)
        self.app.add_route("/{bucket}", bucket_resource)
        self.app.add_route("/{bucket}/", bucket_resource)
        self.app.add_route("/{bucket}/{key:path}", ObjectResource(storage))

    def __call__(self, environ, start_response):
        return self.app(environ, start_response)

    def close(self):
        self._stop.set()
        self._vacuum.join(timeout=5)
        del self.storage
        log.debug("storage closed")


def create_app(data_dir: str = "./blob_data",
               chunk_size: int = 4 * 1024 * 1024,
               vacuum_busy_interval: float = 0.5,
               vacuum_idle_interval: float = 30.0,
               buffer_pool_bytes: int = DEFAULT_BUFFER_POOL_BYTES,
               direct_io: bool = True) -> BlobFalconApp:
    """Create the blob server Falcon WSGI application.

    Returns a WSGI callable.  The caller should invoke ``app.close()`` on
    shutdown (Granian hooks handle this automatically in *run_server_falcon*).
    """
    storage = BlobStorage(data_dir, chunk_size=chunk_size,
                          buffer_pool_bytes=buffer_pool_bytes, direct_io=direct_io)
    return BlobFalconApp(storage, vacuum_busy_interval, vacuum_idle_interval)

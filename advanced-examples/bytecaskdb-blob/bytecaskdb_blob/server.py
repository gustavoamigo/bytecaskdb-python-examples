"""Blob HTTP server with S3-compatible wire protocol (WSGI).

Designed for Granian with free-threaded Python.  Every request handler runs
synchronously in its own OS thread, calling ByteCaskDB directly without
async wrappers or thread-pool executors.  With free-threaded Python the GIL
is absent, so requests execute in true parallelism.
"""

from __future__ import annotations

import logging
import re
import threading
import xml.etree.ElementTree as ET
from datetime import datetime, timezone
from io import BytesIO
from urllib.parse import parse_qs, unquote

log = logging.getLogger(__name__)

from .storage import (
    BlobStorage,
    BlobNotFoundError,
    BucketNotFoundError,
    BucketNotEmptyError,
    UploadNotFoundError,
    UploadInProgressError,
    BlobStorageError,
)

__all__ = ["create_app"]


# ── XML helpers ──────────────────────────────────────────────────────────────

def _xml_bytes(root: ET.Element) -> bytes:
    return ET.tostring(root, xml_declaration=True, encoding="UTF-8")


def _xml_response(root: ET.Element, start_response, status: int = 200):
    body = _xml_bytes(root)
    start_response(
        f"{status} {_STATUS_PHRASES.get(status, '')}",
        [("Content-Type", "application/xml"),
         ("Content-Length", str(len(body)))],
    )
    return [body]


def _error_xml(code: str, message: str, status_code: int, start_response):
    root = ET.Element("Error")
    ET.SubElement(root, "Code").text = code
    ET.SubElement(root, "Message").text = message
    return _xml_response(root, start_response, status_code)


_STATUS_PHRASES = {
    200: "OK",
    204: "No Content",
    206: "Partial Content",
    400: "Bad Request",
    404: "Not Found",
    409: "Conflict",
    500: "Internal Server Error",
    501: "Not Implemented",
}


def _iso_time(ts: float) -> str:
    return datetime.fromtimestamp(ts, tz=timezone.utc).strftime(
        "%Y-%m-%dT%H:%M:%S.000Z"
    )


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


def _read_body(environ: dict) -> bytes:
    content_length = environ.get("CONTENT_LENGTH", "") or "0"
    length = int(content_length)
    if length > 0:
        return environ["wsgi.input"].read(length)
    return b""


def _maybe_decode_aws_chunked(environ: dict, body: bytes) -> bytes:
    headers = _get_headers(environ)
    if headers.get("x-amz-decoded-content-length") or \
            "aws-chunked" in headers.get("content-encoding", "") or \
            "STREAMING-" in headers.get("x-amz-content-sha256", ""):
        return _decode_aws_chunked(body)
    return body


def _get_headers(environ: dict) -> dict[str, str]:
    """Extract HTTP headers from WSGI environ into a lowercase dict."""
    headers: dict[str, str] = {}
    for key, value in environ.items():
        if key.startswith("HTTP_"):
            name = key[5:].replace("_", "-").lower()
            headers[name] = value
    if "CONTENT_TYPE" in environ:
        headers["content-type"] = environ["CONTENT_TYPE"]
    if "CONTENT_LENGTH" in environ:
        headers["content-length"] = environ["CONTENT_LENGTH"]
    return headers


# ── Route patterns ───────────────────────────────────────────────────────────

_RE_OBJECT = re.compile(r"^/([^/]+)/(.+)$")
_RE_BUCKET = re.compile(r"^/([^/]+)/?$")


def _parse_path(path: str):
    """Return (bucket, key) or (bucket, None) or (None, None)."""
    m = _RE_OBJECT.match(path)
    if m:
        return m.group(1), unquote(m.group(2))
    m = _RE_BUCKET.match(path)
    if m:
        return m.group(1), None
    return None, None


# ── Handlers ─────────────────────────────────────────────────────────────────

def _list_buckets(storage: BlobStorage, environ, start_response):
    buckets = storage.list_buckets()
    root = ET.Element("ListAllMyBucketsResult")
    owner = ET.SubElement(root, "Owner")
    ET.SubElement(owner, "ID").text = "bytecaskdb"
    ET.SubElement(owner, "DisplayName").text = "bytecaskdb"
    buckets_el = ET.SubElement(root, "Buckets")
    for b in buckets:
        bkt = ET.SubElement(buckets_el, "Bucket")
        ET.SubElement(bkt, "Name").text = b["name"]
        ET.SubElement(bkt, "CreationDate").text = _iso_time(b["created_at"])
    return _xml_response(root, start_response)


def _handle_bucket(storage: BlobStorage, environ, start_response, bucket: str):
    method = environ["REQUEST_METHOD"]
    qs = parse_qs(environ.get("QUERY_STRING", ""), keep_blank_values=True)

    if method == "PUT":
        storage.create_bucket(bucket)
        start_response("200 OK", [("Content-Length", "0")])
        return [b""]

    if method == "DELETE":
        force = qs.get("force", [""])[0].lower() in ("true", "1")
        try:
            storage.delete_bucket(bucket, force=force)
        except BucketNotFoundError:
            return _error_xml("NoSuchBucket", f"Bucket not found: {bucket}", 404, start_response)
        except BucketNotEmptyError:
            return _error_xml("BucketNotEmpty", f"Bucket not empty: {bucket}", 409, start_response)
        start_response("204 No Content", [("Content-Length", "0")])
        return [b""]

    if method == "HEAD":
        if storage.bucket_exists(bucket):
            start_response("200 OK", [("Content-Length", "0")])
            return [b""]
        return _error_xml("NoSuchBucket", f"Bucket not found: {bucket}", 404, start_response)

    if method == "GET":
        if "location" in qs:
            root = ET.Element("LocationConstraint")
            root.text = ""
            return _xml_response(root, start_response)
        return _list_objects(storage, environ, start_response, bucket)

    if method == "POST":
        if "delete" in qs:
            return _handle_delete_objects(storage, environ, start_response, bucket)

    return _error_xml("NotImplemented", "Not implemented", 501, start_response)


def _handle_delete_objects(storage: BlobStorage, environ, start_response, bucket: str):
    body = _read_body(environ)
    try:
        xml_root = ET.fromstring(body)
    except ET.ParseError as e:
        return _error_xml("MalformedXML", str(e), 400, start_response)

    deleted = []
    errors = []
    for obj in xml_root.findall("Object") or xml_root.findall("{*}Object"):
        key_el = obj.find("Key") or obj.find("{*}Key")
        if key_el is None:
            continue
        key = key_el.text or ""
        try:
            storage.delete_object(bucket, key)
            deleted.append(key)
            log.debug("deleted %s/%s", bucket, key)
        except Exception as exc:
            log.warning("delete failed %s/%s: %s", bucket, key, exc)
            errors.append((key, str(exc)))

    root = ET.Element("DeleteResult")
    for key in deleted:
        d = ET.SubElement(root, "Deleted")
        ET.SubElement(d, "Key").text = key
    for key, msg in errors:
        e = ET.SubElement(root, "Error")
        ET.SubElement(e, "Key").text = key
        ET.SubElement(e, "Code").text = "InternalError"
        ET.SubElement(e, "Message").text = msg
    return _xml_response(root, start_response)


def _list_objects(storage: BlobStorage, environ, start_response, bucket: str):
    qs = parse_qs(environ.get("QUERY_STRING", ""), keep_blank_values=True)
    prefix = qs.get("prefix", [""])[0]
    delimiter = qs.get("delimiter", ["/"])[0]

    common_prefixes, contents = storage.list_objects(bucket, prefix=prefix, delimiter=delimiter)

    root = ET.Element("ListBucketResult")
    ET.SubElement(root, "Name").text = bucket
    ET.SubElement(root, "Prefix").text = prefix
    ET.SubElement(root, "Delimiter").text = delimiter
    ET.SubElement(root, "MaxKeys").text = "1000"
    ET.SubElement(root, "IsTruncated").text = "false"
    ET.SubElement(root, "KeyCount").text = str(len(contents) + len(common_prefixes))

    for obj in contents:
        c = ET.SubElement(root, "Contents")
        ET.SubElement(c, "Key").text = obj["key"]
        ET.SubElement(c, "Size").text = str(obj["size"])
        ET.SubElement(c, "ETag").text = f'"{obj["etag"]}"'
        ET.SubElement(c, "LastModified").text = _iso_time(
            obj.get("completed_at", obj.get("created_at", 0))
        )
        ET.SubElement(c, "StorageClass").text = "STANDARD"

    for pfx in common_prefixes:
        cp = ET.SubElement(root, "CommonPrefixes")
        ET.SubElement(cp, "Prefix").text = pfx

    return _xml_response(root, start_response)


def _handle_object(storage: BlobStorage, environ, start_response, bucket: str, key: str):
    method = environ["REQUEST_METHOD"]

    if method == "PUT":
        return _handle_put_object(storage, environ, start_response, bucket, key)
    if method == "GET":
        return _handle_get_object(storage, environ, start_response, bucket, key)
    if method == "DELETE":
        return _handle_delete_object(storage, environ, start_response, bucket, key)
    if method == "HEAD":
        return _handle_head_object(storage, environ, start_response, bucket, key)
    if method == "POST":
        return _handle_post_object(storage, environ, start_response, bucket, key)

    return _error_xml("NotImplemented", "Not implemented", 501, start_response)


def _handle_put_object(storage: BlobStorage, environ, start_response, bucket: str, key: str):
    qs = parse_qs(environ.get("QUERY_STRING", ""), keep_blank_values=True)
    part_number = qs.get("partNumber", [None])[0]
    upload_id = qs.get("uploadId", [None])[0]

    if part_number and upload_id:
        body = _maybe_decode_aws_chunked(environ, _read_body(environ))
        log.debug("PUT_PART %s/%s part=%s upload=%s body_size=%d",
                  bucket, key, part_number, upload_id, len(body))
        try:
            etag = storage.upload_part(upload_id, int(part_number), body)
        except UploadNotFoundError:
            return _error_xml("NoSuchUpload", "Upload not found", 404, start_response)
        start_response("200 OK", [("ETag", f'"{etag}"'), ("Content-Length", "0")])
        return [b""]

    body = _maybe_decode_aws_chunked(environ, _read_body(environ))
    headers = _get_headers(environ)
    content_type = headers.get("content-type", "application/octet-stream")

    user_metadata = {}
    for h, v in headers.items():
        if h.startswith("x-amz-meta-"):
            meta_key = h[len("x-amz-meta-"):]
            user_metadata[meta_key] = v

    meta = storage.put_object(bucket, key, body,
                              content_type=content_type,
                              user_metadata=user_metadata)
    log.debug("PUT %s/%s body_size=%d stored_size=%d etag=%s",
              bucket, key, len(body), meta["size"], meta["etag"])
    start_response("200 OK", [("ETag", f'"{meta["etag"]}"'), ("Content-Length", "0")])
    return [b""]


def _handle_get_object(storage: BlobStorage, environ, start_response, bucket: str, key: str):
    try:
        meta = storage.head_object(bucket, key)
    except BlobNotFoundError:
        return _error_xml("NoSuchKey", f"Object not found: {key}", 404, start_response)

    log.debug("GET %s/%s stored_size=%d chunk_count=%d chunk_size=%d",
              bucket, key, meta["size"], meta["chunk_count"], meta.get("chunk_size", 0))

    resp_headers = [
        ("Content-Type", meta["content_type"]),
        ("ETag", f'"{meta["etag"]}"'),
        ("Last-Modified", _http_date(
            meta.get("completed_at", meta.get("created_at", 0))
        )),
    ]

    for k, v in meta.get("user_metadata", {}).items():
        resp_headers.append((f"x-amz-meta-{k}", v))

    headers_dict = _get_headers(environ)
    range_header = headers_dict.get("range", "")
    if range_header.startswith("bytes="):
        range_spec = range_header[6:]
        parts = range_spec.split("-")
        start = int(parts[0]) if parts[0] else 0
        end = int(parts[1]) if parts[1] else meta["size"] - 1
        end = min(end, meta["size"] - 1)

        data = storage.get_range(bucket, key, start, end)
        resp_headers.append(("Content-Length", str(len(data))))
        resp_headers.append(("Content-Range", f"bytes {start}-{end}/{meta['size']}"))
        start_response("206 Partial Content", resp_headers)
        return [data]

    # Full object — stream chunks directly
    resp_headers.append(("Content-Length", str(meta["size"])))
    start_response("200 OK", resp_headers)
    return storage.stream_object(bucket, key)


def _handle_delete_object(storage: BlobStorage, environ, start_response, bucket: str, key: str):
    qs = parse_qs(environ.get("QUERY_STRING", ""), keep_blank_values=True)
    upload_id = qs.get("uploadId", [None])[0]

    if upload_id:
        try:
            storage.abort_multipart_upload(upload_id)
        except UploadNotFoundError:
            return _error_xml("NoSuchUpload", "Upload not found", 404, start_response)
        start_response("204 No Content", [("Content-Length", "0")])
        return [b""]

    storage.delete_object(bucket, key)
    start_response("204 No Content", [("Content-Length", "0")])
    return [b""]


def _handle_head_object(storage: BlobStorage, environ, start_response, bucket: str, key: str):
    try:
        meta = storage.head_object(bucket, key)
    except BlobNotFoundError:
        start_response("404 Not Found", [("Content-Length", "0")])
        return [b""]

    log.debug("HEAD %s/%s stored_size=%d chunk_count=%d chunk_size=%d",
              bucket, key, meta["size"], meta["chunk_count"], meta.get("chunk_size", 0))

    resp_headers = [
        ("Content-Type", meta["content_type"]),
        ("ETag", f'"{meta["etag"]}"'),
        ("Content-Length", str(meta["size"])),
        ("Last-Modified", _http_date(
            meta.get("completed_at", meta.get("created_at", 0))
        )),
    ]
    for k, v in meta.get("user_metadata", {}).items():
        resp_headers.append((f"x-amz-meta-{k}", v))

    start_response("200 OK", resp_headers)
    return [b""]


def _handle_post_object(storage: BlobStorage, environ, start_response, bucket: str, key: str):
    qs = parse_qs(environ.get("QUERY_STRING", ""), keep_blank_values=True)

    if "uploads" in qs:
        headers = _get_headers(environ)
        content_type = headers.get("content-type", "application/octet-stream")
        try:
            upload_id = storage.create_multipart_upload(bucket, key, content_type)
        except UploadInProgressError:
            return _error_xml("OperationAborted",
                              f"A multipart upload already exists for {bucket}/{key}",
                              409, start_response)

        root = ET.Element("InitiateMultipartUploadResult")
        ET.SubElement(root, "Bucket").text = bucket
        ET.SubElement(root, "Key").text = key
        ET.SubElement(root, "UploadId").text = upload_id
        return _xml_response(root, start_response)

    upload_id = qs.get("uploadId", [None])[0]
    if upload_id:
        try:
            meta = storage.complete_multipart_upload(upload_id)
        except UploadNotFoundError:
            return _error_xml("NoSuchUpload", "Upload not found", 404, start_response)
        except BlobStorageError as e:
            return _error_xml("InternalError", str(e), 500, start_response)

        log.debug("COMPLETE_MPU %s/%s stored_size=%d chunk_count=%d chunk_size=%d etag=%s",
                  bucket, key, meta["size"], meta["chunk_count"],
                  meta.get("chunk_size", 0), meta["etag"])

        root = ET.Element("CompleteMultipartUploadResult")
        ET.SubElement(root, "Bucket").text = bucket
        ET.SubElement(root, "Key").text = key
        ET.SubElement(root, "ETag").text = f'"{meta["etag"]}"'
        return _xml_response(root, start_response)

    return _error_xml("NotImplemented", "Not implemented", 501, start_response)


# ── Vacuum loop (identical to the async version) ─────────────────────────────

def _vacuum_loop(storage: BlobStorage, busy_interval: float, idle_interval: float, stop: threading.Event) -> None:
    while not stop.is_set():
        try:
            more_work = storage._db.vacuum()
        except Exception:
            log.exception("vacuum: error during pass")
            more_work = False
        stop.wait(timeout=busy_interval if more_work else idle_interval)


# ── WSGI app factory ────────────────────────────────────────────────────────

class _BlobWSGIApp:
    """Thin WSGI application wrapping BlobStorage.

    The app keeps its own references to storage and the vacuum thread so
    that ``close()`` can tear them down cleanly.
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

    # ── WSGI entry point ─────────────────────────────────────────────────
    def __call__(self, environ, start_response):
        path = environ.get("PATH_INFO", "/")
        method = environ["REQUEST_METHOD"]

        # Root — list buckets
        if path == "/":
            if method == "GET":
                return _list_buckets(self.storage, environ, start_response)
            return _error_xml("NotImplemented", "Not implemented", 501, start_response)

        # Try object first (longer match), then bucket
        bucket, key = _parse_path(path)
        if bucket is None:
            return _error_xml("NotImplemented", "Not implemented", 501, start_response)

        if key is not None:
            return _handle_object(self.storage, environ, start_response, bucket, key)

        return _handle_bucket(self.storage, environ, start_response, bucket)

    # ── Cleanup ──────────────────────────────────────────────────────────
    def close(self):
        self._stop.set()
        self._vacuum.join(timeout=5)
        del self.storage
        log.debug("storage closed")


def create_app(data_dir: str = "./blob_data",
               chunk_size: int = 4 * 1024 * 1024,
               vacuum_busy_interval: float = 0.5,
               vacuum_idle_interval: float = 30.0) -> _BlobWSGIApp:
    """Create the blob server WSGI application for use with Granian.

    Returns a WSGI callable.  The caller should invoke ``app.close()`` on
    shutdown (Granian hooks handle this automatically in *run_server_granian*).
    """
    storage = BlobStorage(data_dir, chunk_size=chunk_size)
    return _BlobWSGIApp(storage, vacuum_busy_interval, vacuum_idle_interval)

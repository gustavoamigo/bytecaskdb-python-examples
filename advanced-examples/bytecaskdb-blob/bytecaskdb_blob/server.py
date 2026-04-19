"""Blob HTTP server with S3-compatible wire protocol.

Implements a minimal subset of the S3 API sufficient for boto3 and rclone.
All internal naming uses "blob" terminology; S3 protocol details (XML
responses, S3-style headers) are confined to this module.
"""

from __future__ import annotations

import logging
import xml.etree.ElementTree as ET
from datetime import datetime, timezone
from urllib.parse import unquote

log = logging.getLogger(__name__)

from starlette.applications import Starlette
from starlette.requests import Request
from starlette.responses import Response, StreamingResponse
from starlette.routing import Route

from .storage import (
    BlobStorage,
    BlobNotFoundError,
    BucketNotFoundError,
    BucketNotEmptyError,
    UploadNotFoundError,
    BlobStorageError,
)

__all__ = ["create_app"]


# ── XML helpers ──────────────────────────────────────────────────────────────

def _xml_response(root: ET.Element, status_code: int = 200) -> Response:
    body = ET.tostring(root, xml_declaration=True, encoding="UTF-8")
    return Response(body, status_code=status_code,
                    media_type="application/xml")


def _error_xml(code: str, message: str, status_code: int) -> Response:
    root = ET.Element("Error")
    ET.SubElement(root, "Code").text = code
    ET.SubElement(root, "Message").text = message
    return _xml_response(root, status_code)


def _iso_time(ts: float) -> str:
    return datetime.fromtimestamp(ts, tz=timezone.utc).strftime(
        "%Y-%m-%dT%H:%M:%S.000Z"
    )


def _http_date(ts: float) -> str:
    """RFC 7231 HTTP-date, e.g. Mon, 02 Jan 2006 15:04:05 GMT."""
    return datetime.fromtimestamp(ts, tz=timezone.utc).strftime(
        "%a, %d %b %Y %H:%M:%S GMT"
    )


def _decode_aws_chunked(body: bytes) -> bytes:
    """Strip aws-chunked framing from a streaming SigV4 body.

    Each chunk has the form:  SIZE_HEX[;chunk-signature=...]\r\nDATA\r\n
    The body is terminated by a zero-size chunk.
    """
    result: list[bytes] = []
    pos = 0
    end = len(body)
    while pos < end:
        crlf = body.find(b'\r\n', pos)
        if crlf == -1:
            break
        header = body[pos:crlf].split(b';', 1)[0]  # strip any ;chunk-signature=...
        chunk_size = int(header, 16)
        pos = crlf + 2
        if chunk_size == 0:
            break
        result.append(body[pos:pos + chunk_size])
        pos += chunk_size + 2  # skip trailing \r\n
    return b''.join(result)


def _maybe_decode_aws_chunked(request: Request, body: bytes) -> bytes:
    """Decode body if the request uses aws-chunked transfer encoding."""
    if request.headers.get("x-amz-decoded-content-length") or \
            "aws-chunked" in request.headers.get("content-encoding", "") or \
            "STREAMING-" in request.headers.get("x-amz-content-sha256", ""):
        return _decode_aws_chunked(body)
    return body


# ── Handlers ─────────────────────────────────────────────────────────────────

async def list_buckets(request: Request) -> Response:
    storage: BlobStorage = request.app.state.storage
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
    return _xml_response(root)


async def handle_bucket(request: Request) -> Response:
    storage: BlobStorage = request.app.state.storage
    bucket = request.path_params["bucket"]

    if request.method == "PUT":
        storage.create_bucket(bucket)
        return Response(status_code=200)

    if request.method == "DELETE":
        try:
            storage.delete_bucket(bucket)
        except BucketNotFoundError:
            return _error_xml("NoSuchBucket", f"Bucket not found: {bucket}", 404)
        except BucketNotEmptyError:
            return _error_xml("BucketNotEmpty", f"Bucket not empty: {bucket}", 409)
        return Response(status_code=204)

    if request.method == "HEAD":
        if storage.bucket_exists(bucket):
            return Response(status_code=200)
        return _error_xml("NoSuchBucket", f"Bucket not found: {bucket}", 404)

    if request.method == "GET":
        # GetBucketLocation
        if "location" in request.query_params:
            root = ET.Element("LocationConstraint")
            root.text = ""
            return _xml_response(root)
        # ListObjectsV2
        return await list_objects(request, storage, bucket)

    if request.method == "POST":
        # DeleteObjects
        if "delete" in request.query_params:
            return await handle_delete_objects(request, storage, bucket)

    return _error_xml("NotImplemented", "Not implemented", 501)


async def handle_delete_objects(request: Request, storage: BlobStorage,
                                bucket: str) -> Response:
    body = await request.body()
    try:
        xml_root = ET.fromstring(body)
    except ET.ParseError as e:
        return _error_xml("MalformedXML", str(e), 400)

    ns = {"s3": ""}
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
    return _xml_response(root)


async def list_objects(request: Request, storage: BlobStorage,
                       bucket: str) -> Response:
    prefix = request.query_params.get("prefix", "")
    # Use delimiter from query param; only default to "/" when not provided at all
    delimiter = request.query_params.get("delimiter", None)
    if delimiter is None:
        delimiter = "/"

    common_prefixes, contents = storage.list_objects(
        bucket, prefix=prefix, delimiter=delimiter
    )

    root = ET.Element("ListBucketResult")
    ET.SubElement(root, "Name").text = bucket
    ET.SubElement(root, "Prefix").text = prefix
    ET.SubElement(root, "Delimiter").text = delimiter
    ET.SubElement(root, "MaxKeys").text = "1000"
    ET.SubElement(root, "IsTruncated").text = "false"
    ET.SubElement(root, "KeyCount").text = str(
        len(contents) + len(common_prefixes)
    )

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

    return _xml_response(root)


async def handle_object(request: Request) -> Response:
    storage: BlobStorage = request.app.state.storage
    bucket = request.path_params["bucket"]
    key = request.path_params["key"]

    if request.method == "PUT":
        return await handle_put_object(request, storage, bucket, key)

    if request.method == "GET":
        return await handle_get_object(request, storage, bucket, key)

    if request.method == "DELETE":
        return await handle_delete_object(request, storage, bucket, key)

    if request.method == "HEAD":
        return await handle_head_object(request, storage, bucket, key)

    if request.method == "POST":
        return await handle_post_object(request, storage, bucket, key)

    return _error_xml("NotImplemented", "Not implemented", 501)


async def handle_put_object(request: Request, storage: BlobStorage,
                            bucket: str, key: str) -> Response:
    # UploadPart?
    part_number = request.query_params.get("partNumber")
    upload_id = request.query_params.get("uploadId")

    if part_number and upload_id:
        body = _maybe_decode_aws_chunked(request, await request.body())
        log.debug("PUT_PART %s/%s part=%s upload=%s body_size=%d",
                  bucket, key, part_number, upload_id, len(body))
        try:
            etag = storage.upload_part(upload_id, int(part_number), body)
        except UploadNotFoundError:
            return _error_xml("NoSuchUpload", "Upload not found", 404)
        return Response(status_code=200, headers={"ETag": f'"{etag}"'})

    # Regular PutObject
    body = _maybe_decode_aws_chunked(request, await request.body())
    content_type = request.headers.get("content-type", "application/octet-stream")

    # Collect x-amz-meta-* headers
    user_metadata = {}
    for h, v in request.headers.items():
        if h.lower().startswith("x-amz-meta-"):
            meta_key = h[len("x-amz-meta-"):]
            user_metadata[meta_key] = v

    meta = storage.put_object(bucket, key, body,
                              content_type=content_type,
                              user_metadata=user_metadata)
    log.debug("PUT %s/%s body_size=%d stored_size=%d etag=%s",
              bucket, key, len(body), meta["size"], meta["etag"])
    return Response(status_code=200, headers={"ETag": f'"{meta["etag"]}"'})


async def handle_get_object(request: Request, storage: BlobStorage,
                            bucket: str, key: str) -> Response:
    try:
        meta = storage.head_object(bucket, key)
    except BlobNotFoundError:
        return _error_xml("NoSuchKey", f"Object not found: {key}", 404)

    log.debug("GET %s/%s stored_size=%d chunk_count=%d chunk_size=%d",
              bucket, key, meta["size"], meta["chunk_count"], meta.get("chunk_size", 0))

    headers = {
        "Content-Type": meta["content_type"],
        "ETag": f'"{meta["etag"]}"',
        "Content-Length": str(meta["size"]),
        "Last-Modified": _http_date(
            meta.get("completed_at", meta.get("created_at", 0))
        ),
    }

    # Add user metadata headers
    for k, v in meta.get("user_metadata", {}).items():
        headers[f"x-amz-meta-{k}"] = v

    # Range request?
    range_header = request.headers.get("range")
    if range_header and range_header.startswith("bytes="):
        range_spec = range_header[6:]
        parts = range_spec.split("-")
        start = int(parts[0]) if parts[0] else 0
        end = int(parts[1]) if parts[1] else meta["size"] - 1
        end = min(end, meta["size"] - 1)

        data = storage.get_range(bucket, key, start, end)
        headers["Content-Length"] = str(len(data))
        headers["Content-Range"] = f"bytes {start}-{end}/{meta['size']}"
        return Response(data, status_code=206, headers=headers)

    # Full object — stream it
    return StreamingResponse(
        storage.stream_object(bucket, key),
        status_code=200,
        headers=headers,
        media_type=meta["content_type"],
    )


async def handle_delete_object(request: Request, storage: BlobStorage,
                               bucket: str, key: str) -> Response:
    upload_id = request.query_params.get("uploadId")
    if upload_id:
        try:
            storage.abort_multipart_upload(upload_id)
        except UploadNotFoundError:
            return _error_xml("NoSuchUpload", "Upload not found", 404)
        return Response(status_code=204)

    storage.delete_object(bucket, key)
    return Response(status_code=204)


async def handle_head_object(request: Request, storage: BlobStorage,
                             bucket: str, key: str) -> Response:
    try:
        meta = storage.head_object(bucket, key)
    except BlobNotFoundError:
        return Response(status_code=404)

    log.debug("HEAD %s/%s stored_size=%d chunk_count=%d chunk_size=%d",
              bucket, key, meta["size"], meta["chunk_count"], meta.get("chunk_size", 0))

    headers = {
        "Content-Type": meta["content_type"],
        "ETag": f'"{meta["etag"]}"',
        "Content-Length": str(meta["size"]),
        "Last-Modified": _http_date(
            meta.get("completed_at", meta.get("created_at", 0))
        ),
    }
    for k, v in meta.get("user_metadata", {}).items():
        headers[f"x-amz-meta-{k}"] = v

    return Response(status_code=200, headers=headers)


async def handle_post_object(request: Request, storage: BlobStorage,
                             bucket: str, key: str) -> Response:
    # CreateMultipartUpload?
    if "uploads" in request.query_params:
        content_type = request.headers.get("content-type", "application/octet-stream")
        upload_id = storage.create_multipart_upload(bucket, key, content_type)

        root = ET.Element("InitiateMultipartUploadResult")
        ET.SubElement(root, "Bucket").text = bucket
        ET.SubElement(root, "Key").text = key
        ET.SubElement(root, "UploadId").text = upload_id
        return _xml_response(root)

    # CompleteMultipartUpload?
    upload_id = request.query_params.get("uploadId")
    if upload_id:
        try:
            meta = storage.complete_multipart_upload(upload_id)
        except UploadNotFoundError:
            return _error_xml("NoSuchUpload", "Upload not found", 404)
        except BlobStorageError as e:
            return _error_xml("InternalError", str(e), 500)

        log.debug("COMPLETE_MPU %s/%s stored_size=%d chunk_count=%d chunk_size=%d etag=%s",
                  bucket, key, meta["size"], meta["chunk_count"],
                  meta.get("chunk_size", 0), meta["etag"])

        root = ET.Element("CompleteMultipartUploadResult")
        ET.SubElement(root, "Bucket").text = bucket
        ET.SubElement(root, "Key").text = key
        ET.SubElement(root, "ETag").text = f'"{meta["etag"]}"'
        return _xml_response(root)

    return _error_xml("NotImplemented", "Not implemented", 501)


# ── App factory ──────────────────────────────────────────────────────────────

def create_app(data_dir: str = "./blob_data",
               chunk_size: int = 4 * 1024 * 1024) -> Starlette:
    """Create the blob server ASGI application."""

    routes = [
        Route("/", list_buckets, methods=["GET"]),
        Route("/{bucket}", handle_bucket,
              methods=["PUT", "DELETE", "GET", "HEAD", "POST"]),
        Route("/{bucket}/", handle_bucket,
              methods=["PUT", "DELETE", "GET", "HEAD", "POST"]),
        Route("/{bucket}/{key:path}", handle_object,
              methods=["PUT", "GET", "DELETE", "HEAD", "POST"]),
    ]

    app = Starlette(routes=routes)
    app.state.storage = BlobStorage(data_dir, chunk_size=chunk_size)
    return app

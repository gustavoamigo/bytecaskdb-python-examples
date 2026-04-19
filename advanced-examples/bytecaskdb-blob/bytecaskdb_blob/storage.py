"""BlobStorage — persistent blob storage engine built on ByteCaskDB.

Key layout:
    blob:{bucket}/{path}:meta          -> JSON metadata
    blob:{bucket}/{path}:chunk:{nnnnn} -> raw bytes (5-digit zero-padded)
    bucket:{name}:meta                 -> JSON bucket metadata
    upload:{upload_id}:meta            -> JSON multipart upload metadata
    upload:{upload_id}:part:{nnnnn}    -> staged multipart part bytes
"""

from __future__ import annotations

import hashlib
import json
import time
import uuid
from typing import Generator

import bytecaskdb as bc

__all__ = ["BlobStorage", "BlobNotFoundError", "BucketNotFoundError", "BucketNotEmptyError", "UploadNotFoundError", "UploadInProgressError"]

DEFAULT_CHUNK_SIZE = 4 * 1024 * 1024  # 4 MiB


# ── Exceptions ───────────────────────────────────────────────────────────────

class BlobStorageError(Exception):
    """Base for all BlobStorage errors."""


class BlobNotFoundError(BlobStorageError):
    """Raised when a blob object is not found."""


class BucketNotFoundError(BlobStorageError):
    """Raised when a bucket is not found."""


class BucketNotEmptyError(BlobStorageError):
    """Raised when trying to delete a non-empty bucket."""


class UploadNotFoundError(BlobStorageError):
    """Raised when a multipart upload is not found."""


class UploadInProgressError(BlobStorageError):
    """Raised when trying to start a multipart upload while another is in progress."""


# ── Helpers ──────────────────────────────────────────────────────────────────

def _serialize(obj: dict) -> bytes:
    return json.dumps(obj, separators=(",", ":")).encode()


def _deserialize(data: bytes) -> dict:
    return json.loads(data)


def _md5(data: bytes) -> str:
    return hashlib.md5(data).hexdigest()


def _blob_prefix(bucket: str, path: str) -> bytes:
    return f"blob:{bucket}/{path}:".encode()


def _meta_key(bucket: str, path: str) -> bytes:
    return f"blob:{bucket}/{path}:meta".encode()


def _chunk_key(bucket: str, path: str, n: int) -> bytes:
    return f"blob:{bucket}/{path}:chunk:{n:05d}".encode()


def _bucket_meta_key(name: str) -> bytes:
    return f"bucket:{name}:meta".encode()


def _upload_meta_key(upload_id: str) -> bytes:
    return f"upload:{upload_id}:meta".encode()


def _upload_part_key(upload_id: str, part_number: int) -> bytes:
    return f"upload:{upload_id}:part:{part_number:05d}".encode()


def _upload_prefix(upload_id: str) -> bytes:
    return f"upload:{upload_id}:".encode()


# ── Streaming upload context manager ─────────────────────────────────────────

class _StreamingUpload:
    """Accumulates streaming writes and commits atomically on exit."""

    def __init__(self, storage: BlobStorage, bucket: str, path: str,
                 content_type: str = "application/octet-stream") -> None:
        self._storage = storage
        self._bucket = bucket
        self._path = path
        self._content_type = content_type
        self._chunks: list[bytes] = []
        self._buffer = bytearray()
        self._chunk_size = storage.chunk_size
        self._md5 = hashlib.md5()

    def write(self, data: bytes) -> None:
        self._buffer.extend(data)
        self._md5.update(data)
        while len(self._buffer) >= self._chunk_size:
            chunk = bytes(self._buffer[:self._chunk_size])
            self._chunks.append(chunk)
            self._buffer = self._buffer[self._chunk_size:]

    def _commit(self) -> None:
        # Flush remaining buffer as the last chunk
        if self._buffer:
            self._chunks.append(bytes(self._buffer))
            self._buffer.clear()

        total_size = sum(len(c) for c in self._chunks)
        etag = self._md5.hexdigest()

        meta = {
            "size": total_size,
            "content_type": self._content_type,
            "chunk_count": len(self._chunks),
            "chunk_size": self._chunk_size,
            "status": "uploading",
            "etag": etag,
            "created_at": time.time(),
            "completed_at": time.time(),
            "user_metadata": {},
        }

        # Write chunks first (outside transaction to avoid memory accumulation)
        for i, chunk in enumerate(self._chunks):
            self._storage._db[_chunk_key(self._bucket, self._path, i)] = chunk

        # Atomically flip to complete
        meta["status"] = "complete"
        self._storage._db[_meta_key(self._bucket, self._path)] = _serialize(meta)


# ── BlobStorage ──────────────────────────────────────────────────────────────

class BlobStorage:
    """Persistent blob storage engine built on ByteCaskDB."""

    def __init__(self, path: str, chunk_size: int = DEFAULT_CHUNK_SIZE,
                 **open_kwargs) -> None:
        self._db = bc.DB.open(path, **open_kwargs)
        self.chunk_size = chunk_size

    # ── Buckets ───────────────────────────────────────────────────────────────

    def create_bucket(self, name: str) -> None:
        meta = {"name": name, "created_at": time.time()}
        self._db[_bucket_meta_key(name)] = _serialize(meta)

    def list_buckets(self) -> list[dict]:
        buckets = []
        for key, value in self._db.prefix(b"bucket:"):
            if key.endswith(b":meta"):
                buckets.append(_deserialize(value))
        return buckets

    def delete_bucket(self, name: str, force: bool = False) -> None:
        key = _bucket_meta_key(name)
        if self._db.get(key) is None:
            raise BucketNotFoundError(f"Bucket not found: {name}")

        # Check if bucket has objects
        blob_prefix = f"blob:{name}/".encode()
        for _ in self._db.prefix(blob_prefix):
            if not force:
                raise BucketNotEmptyError(f"Bucket not empty: {name}")
            break

        with self._db.batch() as b:
            del b[key]
        if force:
            # Delete all objects in the bucket
            self._db.delete_range(
                f"blob:{name}/".encode(),
                f"blob:{name}0".encode(),  # '0' > '/' in ASCII
            )

    def bucket_exists(self, name: str) -> bool:
        return self._db.get(_bucket_meta_key(name)) is not None

    # ── Put / Get / Delete ────────────────────────────────────────────────────

    def put_object(self, bucket: str, path: str, data: bytes,
                   content_type: str = "application/octet-stream",
                   user_metadata: dict | None = None) -> dict:
        """Upload an object in one shot. Returns metadata dict."""
        chunks = []
        offset = 0
        while offset < len(data):
            chunks.append(data[offset:offset + self.chunk_size])
            offset += self.chunk_size
        if not chunks:
            chunks = [b""]

        etag = _md5(data)
        now = time.time()
        meta = {
            "size": len(data),
            "content_type": content_type,
            "chunk_count": len(chunks),
            "chunk_size": self.chunk_size,
            "status": "uploading",
            "etag": etag,
            "created_at": now,
            "completed_at": now,
            "user_metadata": user_metadata or {},
        }

        # Write chunks first (outside transaction to avoid memory accumulation)
        for i, chunk in enumerate(chunks):
            self._db[_chunk_key(bucket, path, i)] = chunk

        # Atomically flip to complete
        meta["status"] = "complete"
        self._db[_meta_key(bucket, path)] = _serialize(meta)

        return meta

    def get_object(self, bucket: str, path: str) -> bytes:
        """Download a complete object. Raises BlobNotFoundError if not found."""
        meta = self.head_object(bucket, path)
        parts = []
        for i in range(meta["chunk_count"]):
            chunk = self._db.get(_chunk_key(bucket, path, i))
            if chunk is None:
                raise BlobNotFoundError(f"Missing chunk {i} for {bucket}/{path}")
            parts.append(chunk)
        return b"".join(parts)

    def stream_object(self, bucket: str, path: str) -> Generator[bytes, None, None]:
        """Yield object data chunk by chunk."""
        meta = self.head_object(bucket, path)
        for i in range(meta["chunk_count"]):
            chunk = self._db.get(_chunk_key(bucket, path, i))
            if chunk is None:
                raise BlobNotFoundError(f"Missing chunk {i} for {bucket}/{path}")
            yield chunk

    def get_range(self, bucket: str, path: str, start: int, end: int) -> bytes:
        """Read a byte range [start, end] (inclusive). Returns the bytes."""
        meta = self.head_object(bucket, path)
        chunk_size = meta["chunk_size"]
        size = meta["size"]

        # Clamp to valid range
        end = min(end, size - 1)
        if start > end or start < 0:
            return b""

        start_chunk = start // chunk_size
        end_chunk = end // chunk_size

        parts = []
        for chunk_n in range(start_chunk, end_chunk + 1):
            chunk = self._db.get(_chunk_key(bucket, path, chunk_n))
            if chunk is None:
                raise BlobNotFoundError(f"Missing chunk {chunk_n} for {bucket}/{path}")

            chunk_start = chunk_n * chunk_size
            # Offsets within this chunk
            lo = max(start - chunk_start, 0)
            hi = min(end - chunk_start + 1, len(chunk))
            parts.append(chunk[lo:hi])

        return b"".join(parts)

    def delete_object(self, bucket: str, path: str) -> None:
        """Delete an object and all its chunks."""
        prefix = _blob_prefix(bucket, path)
        # Range covers "blob:{bucket}/{path}:" through "blob:{bucket}/{path};",
        # since ';' > ':' in ASCII.
        end = prefix[:-1] + b";"
        self._db.delete_range(prefix, end)

    def delete_prefix(self, bucket: str, prefix: str) -> None:
        """Recursively delete all objects under a prefix."""
        start = f"blob:{bucket}/{prefix}".encode()
        # Find the end boundary: increment the last byte of the prefix
        end = start[:-1] + bytes([start[-1] + 1]) if start else b""
        self._db.delete_range(start, end)

    def head_object(self, bucket: str, path: str) -> dict:
        """Return object metadata. Raises BlobNotFoundError if not found."""
        raw = self._db.get(_meta_key(bucket, path))
        if raw is None:
            raise BlobNotFoundError(f"Object not found: {bucket}/{path}")
        meta = _deserialize(raw)
        if meta.get("status") != "complete":
            raise BlobNotFoundError(f"Object not complete: {bucket}/{path}")
        return meta

    def object_exists(self, bucket: str, path: str) -> bool:
        """Check if an object exists and is complete."""
        try:
            self.head_object(bucket, path)
            return True
        except BlobNotFoundError:
            return False

    # ── Listing ───────────────────────────────────────────────────────────────

    def list_objects(self, bucket: str, prefix: str = "",
                     delimiter: str = "/") -> tuple[list[str], list[dict]]:
        """List objects with delimiter support.

        Returns (common_prefixes, contents) where:
          - common_prefixes: list of "folder" prefixes at this level
          - contents: list of metadata dicts for objects at this level
        """
        seen_prefixes: set[str] = set()
        contents: list[dict] = []
        full_prefix = f"blob:{bucket}/{prefix}".encode()
        meta_suffix = b":meta"

        for key, _ in self._db.prefix(full_prefix):
            if not key.endswith(meta_suffix):
                continue

            # Extract relative path after the full prefix
            # key format: blob:{bucket}/{prefix}{remainder}:meta
            key_str = key.decode()
            blob_path = key_str[len("blob:") + len(bucket) + 1:-len(":meta")]
            remainder = blob_path[len(prefix):]

            if delimiter and delimiter in remainder:
                folder = remainder[:remainder.index(delimiter) + len(delimiter)]
                seen_prefixes.add(prefix + folder)
            else:
                # Read meta to check status
                raw = self._db.get(key)
                if raw:
                    meta = _deserialize(raw)
                    if meta.get("status") == "complete":
                        meta["key"] = blob_path
                        contents.append(meta)

        return sorted(seen_prefixes), contents

    # ── Streaming upload ──────────────────────────────────────────────────────

    def upload(self, bucket: str, path: str,
               content_type: str = "application/octet-stream") -> _StreamingUploadContext:
        """Context manager for streaming uploads.

        Usage::

            with storage.upload("photos", "big.jpg") as up:
                for chunk in stream:
                    up.write(chunk)
        """
        return _StreamingUploadContext(self, bucket, path, content_type)

    # ── Multipart uploads ─────────────────────────────────────────────────────

    def create_multipart_upload(self, bucket: str, path: str,
                                content_type: str = "application/octet-stream") -> str:
        """Start a multipart upload. Returns upload_id.

        Raises UploadInProgressError if there's already an active upload for this key.
        """
        # Check if there's already an active upload for this key
        for key, value in self._db.prefix(b"upload:"):
            if key.endswith(b":meta"):
                meta = _deserialize(value)
                if meta.get("bucket") == bucket and meta.get("path") == path:
                    raise UploadInProgressError(
                        f"Upload already in progress for {bucket}/{path}. "
                        f"Abort upload {meta['upload_id']} first."
                    )

        upload_id = uuid.uuid7().hex
        meta = {
            "upload_id": upload_id,
            "bucket": bucket,
            "path": path,
            "content_type": content_type,
            "created_at": time.time(),
            "parts": {},  # part_number -> {"size": bytes, "etag": "..."}
        }

        self._db[_upload_meta_key(upload_id)] = _serialize(meta)
        return upload_id

    def upload_part(self, upload_id: str, part_number: int, data: bytes) -> str:
        """Upload a part directly to final blob location. Returns the part's ETag."""
        meta_raw = self._db.get(_upload_meta_key(upload_id))
        if meta_raw is None:
            raise UploadNotFoundError(f"Upload not found: {upload_id}")

        meta = _deserialize(meta_raw)
        bucket = meta["bucket"]
        path = meta["path"]
        etag = _md5(data)

        # Write part directly to final blob chunk location
        # Use part number as chunk number (parts are 1-based, chunks are 0-based)
        chunk_key = _chunk_key(bucket, path, part_number - 1)
        self._db[chunk_key] = data

        # Update upload metadata with part info
        meta["parts"][str(part_number)] = {
            "size": len(data),
            "etag": etag,
        }
        self._db[_upload_meta_key(upload_id)] = _serialize(meta)

        return etag

    def complete_multipart_upload(self, upload_id: str) -> dict:
        """Complete a multipart upload. Creates blob metadata (no data copying needed)."""
        meta_raw = self._db.get(_upload_meta_key(upload_id))
        if meta_raw is None:
            raise UploadNotFoundError(f"Upload not found: {upload_id}")

        upload_meta = _deserialize(meta_raw)
        bucket = upload_meta["bucket"]
        path = upload_meta["path"]
        content_type = upload_meta["content_type"]
        parts = upload_meta["parts"]

        if not parts:
            raise BlobStorageError(f"No parts uploaded for {upload_id}")

        # Calculate total size and composite ETag
        part_numbers = sorted(int(pn) for pn in parts.keys())
        total_size = sum(parts[str(pn)]["size"] for pn in part_numbers)

        # Composite ETag: md5(concat of part md5s)-part_count
        part_md5s = b"".join(
            bytes.fromhex(parts[str(pn)]["etag"])
            for pn in part_numbers
        )
        etag = f"{hashlib.md5(part_md5s).hexdigest()}-{len(parts)}"

        # Create blob metadata (this makes the object visible)
        blob_meta = {
            "size": total_size,
            "content_type": content_type,
            "chunk_count": len(parts),
            "chunk_size": self.chunk_size,  # not really relevant for multipart
            "status": "complete",
            "etag": etag,
            "created_at": upload_meta["created_at"],
            "completed_at": time.time(),
            "user_metadata": {},
        }

        # Atomically create blob metadata and clean up upload metadata
        with self._db.batch(sync=True) as b:
            b[_meta_key(bucket, path)] = _serialize(blob_meta)
            del b[_upload_meta_key(upload_id)]

        return blob_meta

    def abort_multipart_upload(self, upload_id: str) -> None:
        """Abort a multipart upload. Clean up upload metadata and any uploaded chunks."""
        meta_raw = self._db.get(_upload_meta_key(upload_id))
        if meta_raw is None:
            raise UploadNotFoundError(f"Upload not found: {upload_id}")

        upload_meta = _deserialize(meta_raw)
        bucket = upload_meta["bucket"]
        path = upload_meta["path"]

        # Remove upload metadata
        self._db.delete(_upload_meta_key(upload_id))

        # Clean up any uploaded chunks
        blob_prefix = _blob_prefix(bucket, path)
        self._db.delete_range(blob_prefix, blob_prefix[:-1] + b";")


# ── Streaming upload context ─────────────────────────────────────────────────

class _StreamingUploadContext:
    def __init__(self, storage: BlobStorage, bucket: str, path: str,
                 content_type: str) -> None:
        self._upload = _StreamingUpload(storage, bucket, path, content_type)

    def __enter__(self) -> _StreamingUpload:
        return self._upload

    def __exit__(self, exc_type, exc_val, exc_tb) -> None:
        if exc_type is None:
            self._upload._commit()

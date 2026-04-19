"""Tests for the BlobStorage engine (no HTTP)."""

import tempfile
import pytest
from bytecaskdb_blob import (
    BlobStorage,
    BlobNotFoundError,
    BucketNotFoundError,
    BucketNotEmptyError,
    UploadNotFoundError,
    UploadInProgressError,
)


@pytest.fixture
def storage():
    with tempfile.TemporaryDirectory() as tmpdir:
        yield BlobStorage(tmpdir, chunk_size=64)


# ── Buckets ──────────────────────────────────────────────────────────────────

class TestBuckets:
    def test_create_and_list(self, storage):
        storage.create_bucket("photos")
        storage.create_bucket("videos")
        buckets = storage.list_buckets()
        names = {b["name"] for b in buckets}
        assert names == {"photos", "videos"}

    def test_bucket_exists(self, storage):
        assert not storage.bucket_exists("photos")
        storage.create_bucket("photos")
        assert storage.bucket_exists("photos")

    def test_delete_empty_bucket(self, storage):
        storage.create_bucket("temp")
        storage.delete_bucket("temp")
        assert not storage.bucket_exists("temp")

    def test_delete_nonexistent_bucket(self, storage):
        with pytest.raises(BucketNotFoundError):
            storage.delete_bucket("nope")

    def test_delete_nonempty_bucket_raises(self, storage):
        storage.create_bucket("photos")
        storage.put_object("photos", "a.jpg", b"data")
        with pytest.raises(BucketNotEmptyError):
            storage.delete_bucket("photos")

    def test_delete_nonempty_bucket_force(self, storage):
        storage.create_bucket("photos")
        storage.put_object("photos", "a.jpg", b"data")
        storage.delete_bucket("photos", force=True)
        assert not storage.bucket_exists("photos")
        assert not storage.object_exists("photos", "a.jpg")


# ── Put / Get / Delete ───────────────────────────────────────────────────────

class TestObjects:
    def test_put_and_get(self, storage):
        storage.create_bucket("b")
        storage.put_object("b", "hello.txt", b"Hello, World!")
        assert storage.get_object("b", "hello.txt") == b"Hello, World!"

    def test_get_missing(self, storage):
        with pytest.raises(BlobNotFoundError):
            storage.get_object("b", "missing")

    def test_put_overwrites(self, storage):
        storage.create_bucket("b")
        storage.put_object("b", "key", b"v1")
        storage.put_object("b", "key", b"v2")
        assert storage.get_object("b", "key") == b"v2"

    def test_delete_object(self, storage):
        storage.create_bucket("b")
        storage.put_object("b", "key", b"data")
        storage.delete_object("b", "key")
        assert not storage.object_exists("b", "key")

    def test_head_object(self, storage):
        storage.create_bucket("b")
        storage.put_object("b", "f.txt", b"abc", content_type="text/plain")
        meta = storage.head_object("b", "f.txt")
        assert meta["size"] == 3
        assert meta["content_type"] == "text/plain"
        assert meta["status"] == "complete"
        assert "etag" in meta

    def test_empty_object(self, storage):
        storage.create_bucket("b")
        storage.put_object("b", "empty", b"")
        assert storage.get_object("b", "empty") == b""

    def test_object_exists(self, storage):
        storage.create_bucket("b")
        assert not storage.object_exists("b", "nope")
        storage.put_object("b", "yes", b"data")
        assert storage.object_exists("b", "yes")


# ── Chunking ─────────────────────────────────────────────────────────────────

class TestChunking:
    def test_large_object_chunked(self, storage):
        """Object larger than chunk_size (64 bytes) is split into chunks."""
        storage.create_bucket("b")
        data = b"x" * 200  # 200 bytes > 64 chunk size → 4 chunks
        meta = storage.put_object("b", "big", data)
        assert meta["chunk_count"] == 4  # ceil(200/64) = 4
        assert storage.get_object("b", "big") == data

    def test_exact_chunk_boundary(self, storage):
        storage.create_bucket("b")
        data = b"y" * 128  # exactly 2 chunks
        meta = storage.put_object("b", "exact", data)
        assert meta["chunk_count"] == 2
        assert storage.get_object("b", "exact") == data


# ── Streaming ────────────────────────────────────────────────────────────────

class TestStreaming:
    def test_stream_object(self, storage):
        storage.create_bucket("b")
        data = b"a" * 150
        storage.put_object("b", "f", data)
        reassembled = b"".join(storage.stream_object("b", "f"))
        assert reassembled == data

    def test_streaming_upload(self, storage):
        storage.create_bucket("b")
        with storage.upload("b", "streamed.bin") as up:
            up.write(b"a" * 100)
            up.write(b"b" * 100)
        result = storage.get_object("b", "streamed.bin")
        assert result == b"a" * 100 + b"b" * 100


# ── Range requests ───────────────────────────────────────────────────────────

class TestRangeRequests:
    def test_range_within_single_chunk(self, storage):
        storage.create_bucket("b")
        storage.put_object("b", "f", b"0123456789")
        assert storage.get_range("b", "f", 2, 5) == b"2345"

    def test_range_across_chunks(self, storage):
        storage.create_bucket("b")
        data = bytes(range(200))  # 200 bytes, chunk_size=64
        storage.put_object("b", "f", data)
        # Range spanning chunks 0 and 1
        assert storage.get_range("b", "f", 60, 70) == data[60:71]

    def test_range_entire_object(self, storage):
        storage.create_bucket("b")
        data = b"hello world"
        storage.put_object("b", "f", data)
        assert storage.get_range("b", "f", 0, 100) == data

    def test_range_clamped_to_size(self, storage):
        storage.create_bucket("b")
        storage.put_object("b", "f", b"short")
        assert storage.get_range("b", "f", 0, 999) == b"short"


# ── Listing ──────────────────────────────────────────────────────────────────

class TestListing:
    def test_list_flat(self, storage):
        storage.create_bucket("b")
        storage.put_object("b", "a.txt", b"a")
        storage.put_object("b", "b.txt", b"b")
        prefixes, contents = storage.list_objects("b")
        keys = [c["key"] for c in contents]
        assert sorted(keys) == ["a.txt", "b.txt"]
        assert prefixes == []

    def test_list_with_delimiter(self, storage):
        storage.create_bucket("b")
        storage.put_object("b", "photos/2024/a.jpg", b"a")
        storage.put_object("b", "photos/2024/b.jpg", b"b")
        storage.put_object("b", "photos/2025/c.jpg", b"c")
        storage.put_object("b", "docs/readme.txt", b"r")

        # Top-level listing
        prefixes, contents = storage.list_objects("b")
        assert sorted(prefixes) == ["docs/", "photos/"]
        assert contents == []

        # photos/ listing
        prefixes, contents = storage.list_objects("b", prefix="photos/")
        assert sorted(prefixes) == ["photos/2024/", "photos/2025/"]
        assert contents == []

        # photos/2024/ listing
        prefixes, contents = storage.list_objects("b", prefix="photos/2024/")
        assert prefixes == []
        keys = [c["key"] for c in contents]
        assert sorted(keys) == ["photos/2024/a.jpg", "photos/2024/b.jpg"]

    def test_delete_prefix(self, storage):
        storage.create_bucket("b")
        storage.put_object("b", "logs/2024/jan.log", b"j")
        storage.put_object("b", "logs/2024/feb.log", b"f")
        storage.put_object("b", "logs/2025/mar.log", b"m")
        storage.delete_prefix("b", "logs/2024/")
        assert not storage.object_exists("b", "logs/2024/jan.log")
        assert not storage.object_exists("b", "logs/2024/feb.log")
        assert storage.object_exists("b", "logs/2025/mar.log")


# ── Multipart uploads ────────────────────────────────────────────────────────

class TestMultipart:
    def test_multipart_lifecycle(self, storage):
        storage.create_bucket("b")
        uid = storage.create_multipart_upload("b", "big.bin")
        storage.upload_part(uid, 1, b"part1-" * 20)
        storage.upload_part(uid, 2, b"part2-" * 20)
        meta = storage.complete_multipart_upload(uid)
        assert meta["status"] == "complete"
        data = storage.get_object("b", "big.bin")
        assert data == b"part1-" * 20 + b"part2-" * 20

    def test_multipart_etag_format(self, storage):
        storage.create_bucket("b")
        uid = storage.create_multipart_upload("b", "mp.bin")
        storage.upload_part(uid, 1, b"a" * 50)
        storage.upload_part(uid, 2, b"b" * 50)
        meta = storage.complete_multipart_upload(uid)
        # Composite etag: {hash}-{part_count}
        assert meta["etag"].endswith("-2")

    def test_abort_multipart(self, storage):
        storage.create_bucket("b")
        uid = storage.create_multipart_upload("b", "aborted.bin")
        storage.upload_part(uid, 1, b"data")
        storage.abort_multipart_upload(uid)
        assert not storage.object_exists("b", "aborted.bin")

    def test_upload_part_missing_upload(self, storage):
        with pytest.raises(UploadNotFoundError):
            storage.upload_part("nonexistent", 1, b"data")

    def test_complete_missing_upload(self, storage):
        with pytest.raises(UploadNotFoundError):
            storage.complete_multipart_upload("nonexistent")

    def test_concurrent_multipart_uploads_same_key_blocked(self, storage):
        """Test that concurrent multipart uploads to same key are blocked."""
        storage.create_bucket("b")

        # Start first multipart upload
        uid1 = storage.create_multipart_upload("b", "same.bin")

        # Try to start second upload to same key - should fail
        with pytest.raises(UploadInProgressError):
            storage.create_multipart_upload("b", "same.bin")

        # After completing first upload, can start new one
        storage.upload_part(uid1, 1, b"data1")
        storage.complete_multipart_upload(uid1)

        # Now second upload should work
        uid2 = storage.create_multipart_upload("b", "same.bin")
        storage.upload_part(uid2, 1, b"data2")
        storage.complete_multipart_upload(uid2)
        assert storage.get_object("b", "same.bin") == b"data2"

    def test_concurrent_multipart_uploads_same_key_after_abort(self, storage):
        """Test that after aborting upload, new one can be started."""
        storage.create_bucket("b")

        uid1 = storage.create_multipart_upload("b", "test.bin")
        storage.upload_part(uid1, 1, b"data")

        # Can't start new upload while first is active
        with pytest.raises(UploadInProgressError):
            storage.create_multipart_upload("b", "test.bin")

        # After abort, can start new upload
        storage.abort_multipart_upload(uid1)
        uid2 = storage.create_multipart_upload("b", "test.bin")
        storage.upload_part(uid2, 1, b"new-data")
        storage.complete_multipart_upload(uid2)
        assert storage.get_object("b", "test.bin") == b"new-data"

    def test_multipart_no_blob_metadata_until_complete(self, storage):
        """Verify blob metadata is only created on completion."""
        storage.create_bucket("b")
        uid = storage.create_multipart_upload("b", "test.bin")
        storage.upload_part(uid, 1, b"data")

        # Object shouldn't exist until completion
        assert not storage.object_exists("b", "test.bin")

        # Complete it
        storage.complete_multipart_upload(uid)
        assert storage.object_exists("b", "test.bin")

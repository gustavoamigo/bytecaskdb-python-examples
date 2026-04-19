# ByteCaskDB Python API Reference

> Version **0.1.0**

*Auto-generated from `python3 -c "import bytecaskdb; help(bytecaskdb)"`*

---

## Contents

- [ByteCaskError](#bytecaskerror)
- [ConflictError](#conflicterror)
- [DB](#db)
- [DbDegraded](#dbdegraded)
- [DegradedError](#degradederror)
- [Options](#options)
- [ReadOptions](#readoptions)
- [Snapshot](#snapshot)
- [VacuumOptions](#vacuumoptions)
- [WriteOptions](#writeoptions)

> **Iterator types** (`EntryIterator`, `KeyIterator`, `ReverseEntryIterator`, `ReverseKeyIterator`) implement the standard Python iterator protocol (`__iter__` / `__next__`) and are returned by the `iter_from`, `keys_from`, `riter_from`, and `rkeys_from` methods.

---

## ByteCaskError

Base for all bytecaskdb_ext errors.

---

## ConflictError

Raised by a Transaction when apply_batch detects a write conflict.

---

## DB

Pythonic wrapper around bytecaskdb.DB.

Open with DB.open(path) or DB.open(path, max_file_bytes=..., ...).

### Properties

| Property | Description |
|----------|-------------|
| `degraded_reason` |  |
| `is_degraded` |  |

### Methods

#### `__contains__(key: bytes) -> bool`

Return True if *key* exists. No disk I/O.

#### `__delitem__(key: bytes) -> None`

Delete *key*. Silently succeeds if the key does not exist.

#### `__getitem__(key: bytes) -> bytes`

Return value for *key*; raises KeyError if not found.

#### `__setitem__(key: bytes, value: bytes) -> None`

Write key → value with default options (sync=True).

#### `batch(*, sync: bool = True, solo: bool = False) -> _BatchContext`

Context manager for an atomic batch of writes/deletes.
Changes are committed on ``__exit__``.  No conflict detection.
Example::
    with db.batch() as b:
        b[b"key1"] = b"value1"
        del b[b"key2"]
        b.delete_range(b"log:001", b"log:010")

#### `delete(key: bytes, *, sync: bool = True, solo: bool = False) -> bool`

Delete *key*. Returns True if the key existed.

#### `delete_range(from_key: bytes, to_key: bytes, *, sync: bool = True, solo: bool = False) -> None`

Delete all keys in [from_key, to_key) with a single disk append.

#### `get(key: bytes, default: bytes | None = None, *, verify_checksums: bool = False) -> bytes | None`

Return value for *key*, or *default* if not found.

#### `items(start: bytes = b'', *, verify_checksums: bool = False)`

Iterate (key, value) pairs in ascending order from *start*.

#### `keys(start: bytes = b'', *, verify_checksums: bool = False)`

Iterate keys in ascending order from *start*. No disk I/O.

#### `prefix(pfx: bytes, *, verify_checksums: bool = False)`

Iterate (key, value) pairs whose key starts with *pfx*,
ascending.

#### `put(key: bytes, value: bytes, *, sync: bool = True, solo: bool = False) -> None`

Write key → value.

#### `resume() -> None`

Attempt recovery from a degraded state.

#### `ritems(start: bytes = b'', *, verify_checksums: bool = False)`

Iterate (key, value) pairs in descending order from *start*.
When *start* is b'' (default), begins at the last key.

#### `rkeys(start: bytes = b'', *, verify_checksums: bool = False)`

Iterate keys in descending order from *start*. No disk I/O.

#### `rprefix(pfx: bytes, *, verify_checksums: bool = False)`

Iterate (key, value) pairs whose key starts with *pfx*,
descending.

#### `snapshot() -> Snapshot`

Return a frozen read-only view of the database at this instant.

#### `transaction(*, sync: bool = True, solo: bool = False) -> _TransactionContext`

Context manager for a snapshot-backed transaction.
Reads come from a snapshot taken at entry.  Writes are staged and
committed atomically on ``__exit__``.  Raises ``ConflictError`` if
a concurrent modification was detected.
Example::
    with db.transaction() as txn:
        stock = int(txn[b"stock"])
        txn[b"stock"] = str(stock - 1).encode()

#### `vacuum(*, fragmentation_threshold: float | None = None, absorb_threshold: int | None = None) -> bool`

Run one vacuum pass. Return True if a file was vacuumed.
Call in a loop to compact all eligible files::
    while db.vacuum():
        pass

#### `open(path: str, *, max_file_bytes: int | None = None, recovery_threads: int | None = None, fail_recovery_on_crc_errors: bool | None = None) -> 'DB'`

---

## DbDegraded

---

## DegradedError

Raised when the engine enters a degraded state.

---

## Options

Configuration for DB.open().

### Properties

| Property | Description |
|----------|-------------|
| `fail_recovery_on_crc_errors` | If True (default), CRC errors during recovery raise. |
| `max_file_bytes` | Active file rotation threshold in bytes (default 64 MiB). |
| `recovery_threads` | Number of threads for parallel hint-file replay (default 4). |

---

## ReadOptions

Per-read options for get, iter_from, etc.

### Properties

| Property | Description |
|----------|-------------|
| `verify_checksums` | If True, CRC-verify each value read from disk (default False). |

---

## Snapshot

Read-only frozen view of the database.

Supports the same dict-like reads and iteration as DB.
Use as a context manager to ensure resources are released promptly.

### Methods

#### `__contains__(key: bytes) -> bool`

#### `__getitem__(key: bytes) -> bytes`

#### `get(key: bytes, default: bytes | None = None) -> bytes | None`

#### `items(start: bytes = b'')`

Iterate (key, value) pairs in ascending order from *start*.

#### `keys(start: bytes = b'')`

Iterate keys in ascending order from *start*. No disk I/O.

#### `prefix(pfx: bytes)`

Iterate (key, value) pairs whose key starts with *pfx*,
ascending.

#### `ritems(start: bytes = b'')`

Iterate (key, value) pairs in descending order from *start*.
When *start* is b'' (default), begins at the last key.

#### `rkeys(start: bytes = b'')`

Iterate keys in descending order from *start*. No disk I/O.

#### `rprefix(pfx: bytes)`

Iterate (key, value) pairs whose key starts with *pfx*,
descending.

---

## VacuumOptions

Options for DB.vacuum().

### Properties

| Property | Description |
|----------|-------------|
| `absorb_threshold` | Max live bytes for absorption into active file (default 1 MiB). |
| `fragmentation_threshold` | Minimum fragmentation ratio for a file to be eligible. |

---

## WriteOptions

Per-write options for put, del_, apply_batch, etc.

### Properties

| Property | Description |
|----------|-------------|
| `solo` | If True, bypass group commit (for benchmarking). |
| `sync` | If True (default), call fdatasync after write. |

---

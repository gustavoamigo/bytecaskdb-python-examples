# ByteCaskDB Python API Reference

> Version **0.1.0**

*Auto-generated from `python3 -c "import bytecaskdb; help(bytecaskdb)"`*

---

## Contents

- [DB](#db)
- [DbDegraded](#dbdegraded)
- [Options](#options)
- [ReadOptions](#readoptions)
- [Snapshot](#snapshot)
- [VacuumOptions](#vacuumoptions)
- [WriteOptions](#writeoptions)
- [WritePlan](#writeplan)

> **Iterator types** (`EntryIterator`, `KeyIterator`, `ReverseEntryIterator`, `ReverseKeyIterator`) implement the standard Python iterator protocol (`__iter__` / `__next__`) and are returned by the `iter_from`, `keys_from`, `riter_from`, and `rkeys_from` methods.

---

## DB

ByteCaskDB database handle.

Open or create a database with DB.open(path).

### Properties

| Property | Description |
|----------|-------------|
| `degraded_reason` | Diagnostic string describing why the engine degraded, or empty. |
| `is_degraded` | True if the engine is in a degraded state. |

### Methods

#### `apply_batch(self, plan: WritePlan, opts: WriteOptions | None = None) -> bool`

Apply plan atomically. Return True if committed, False on conflict.

#### `contains_key(self, key: bytes) -> bool`

Return True if key exists. No disk I/O.

#### `del_(self, key: bytes, opts: WriteOptions | None = None) -> bool`

Delete key. Return True if it existed.

#### `del_range(self, from_key: bytes, to_key: bytes, opts: WriteOptions | None = None) -> None`

Delete all keys in [from_key, to_key) with a single disk append.

#### `get(self, key: bytes, opts: ReadOptions | None = None) -> object`

Return the value for key, or None if not found.

#### `iter_from(self, from_key: bytes = b'', opts: ReadOptions | None = None) -> EntryIterator`

Iterate (key, value) pairs in ascending order from from_key.

#### `keys_from(self, from_key: bytes = b'', opts: ReadOptions | None = None) -> KeyIterator`

Iterate keys in ascending order. No disk I/O.

#### `put(self, key: bytes, value: bytes, opts: WriteOptions | None = None) -> None`

Write key -> value. Overwrites any existing value.

#### `resume(self) -> None`

Attempt recovery from a degraded state.

#### `riter_from(self, from_key: bytes = b'', opts: ReadOptions | None = None) -> ReverseEntryIterator`

Iterate (key, value) pairs in descending order from from_key.

#### `rkeys_from(self, from_key: bytes = b'', opts: ReadOptions | None = None) -> ReverseKeyIterator`

Iterate keys in descending order. No disk I/O.

#### `snapshot(self) -> Snapshot`

Return a frozen, read-only view of the database at this instant.

#### `vacuum(self, opts: VacuumOptions | None = None) -> bool`

Run one vacuum pass. Return True if a file was vacuumed.

#### *static*  `open(path: str | os.PathLike, opts: Options | None = None) -> DB`

Open or create a database at path.

---

## DbDegraded

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

Frozen, read-only view of the database at a point in time.

### Methods

#### `contains_key(self, key: bytes) -> bool`

Return True if key exists. No disk I/O.

#### `get(self, key: bytes) -> object`

Return the value for key, or None if not found.

#### `iter_from(self, from_key: bytes = b'') -> EntryIterator`

Iterate (key, value) pairs in ascending order from from_key.

#### `keys_from(self, from_key: bytes = b'') -> KeyIterator`

Iterate keys in ascending order. No disk I/O.

#### `riter_from(self, from_key: bytes = b'') -> ReverseEntryIterator`

Iterate (key, value) pairs in descending order from from_key.

#### `rkeys_from(self, from_key: bytes = b'') -> ReverseKeyIterator`

Iterate keys in descending order. No disk I/O.

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

## WritePlan

Atomic write plan for DB.apply_batch().

Construct without arguments for a simple unconditional batch.
Construct with a Snapshot to enable ensure_unchanged guards and
automatic write-write conflict detection.

### Properties

| Property | Description |
|----------|-------------|
| `has_snapshot` | True if this plan was constructed with a snapshot. |

### Methods

#### `del_(self, key: bytes) -> None`

Stage a key deletion.

#### `del_range(self, from_key: bytes, to_key: bytes) -> None`

Stage a range deletion: all keys in [from_key, to_key).

#### `ensure_absent(self, key: bytes) -> None`

Guard: key must be absent at commit time.

#### `ensure_present(self, key: bytes) -> None`

Guard: key must exist at commit time.

#### `ensure_range_unchanged(self, from_key: bytes, to_key: bytes) -> None`

Guard: no key in [from_key, to_key) changed since the snapshot.

#### `ensure_unchanged(self, key: bytes) -> None`

Guard: key must not have changed since the snapshot.

#### `put(self, key: bytes, value: bytes) -> None`

Stage a key-value write.

---

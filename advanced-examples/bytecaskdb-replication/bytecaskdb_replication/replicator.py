"""Portable replication engine for ByteCaskDB.

This module is generic — it operates on raw _bytecaskdb.DB handles and knows
nothing about the application layer (tasks, blobs, etc.). It provides:

  - ReplicationLeader: WSGI handlers that followers call to fetch data
  - ReplicationFollower: background thread that replicates from a leader over HTTP

Wire format: newline-delimited JSON (NDJSON). Each DataEntry is serialized as:
    {"seq": int, "type": str, "key": base64, "value": base64}
"""

from __future__ import annotations

import base64
import json
import logging
import os
import shutil
import threading
import time
from pathlib import Path
from urllib.parse import parse_qs

import httpx

from bytecaskdb import _bytecaskdb as _bc

log = logging.getLogger(__name__)

BATCH_SIZE = 512


def _encode_entry(entry: _bc.DataEntry) -> bytes:
    obj = {
        "seq": entry.sequence,
        "type": entry.entry_type.name,
        "key": base64.b64encode(entry.key).decode(),
        "value": base64.b64encode(entry.value).decode(),
    }
    return json.dumps(obj, separators=(",", ":")).encode() + b"\n"


def _decode_entry(line: bytes) -> _bc.DataEntry:
    obj = json.loads(line)
    return _bc.DataEntry(
        sequence=obj["seq"],
        entry_type=_bc.EntryType[obj["type"]],
        key=base64.b64decode(obj["key"]),
        value=base64.b64decode(obj["value"]),
    )


class ReplicationLeader:
    """Leader-side replication logic.

    Provides methods that return plain data — framework-agnostic.
    Mount via Falcon resources, raw WSGI, or any other HTTP layer.
    """

    def __init__(self, db: _bc.DB) -> None:
        self._db = db

    def get_sequence(self, timeout_ms: int = 0) -> dict:
        seq = self._db.current_sequence(timeout_ms)
        return {"sequence": seq}

    def get_changes(self, from_seq: int = 0) -> bytes:
        snap = self._db.snapshot()
        it = self._db.changes_since(snap, from_seq)
        chunks = []
        for entry in it:
            chunks.append(_encode_entry(entry))
        return b"".join(chunks)

    def get_status(self) -> dict:
        return {
            "role": "leader",
            "sequence": self._db.current_sequence(0),
            "mode": self._db.mode.name,
        }

    def get_manifest(self) -> tuple[str, bytes]:
        """Return (boundary, body) for the multipart manifest response."""
        manifest = self._db.create_manifest()
        file_list = []
        for fi in manifest.files:
            file_list.append({
                "file_id": fi.file_id,
                "data_name": os.path.basename(fi.data_path),
                "hint_name": os.path.basename(fi.hint_path),
            })
        meta = {
            "through_sequence": manifest.through_sequence,
            "files": file_list,
        }

        boundary = "BCREPL"
        parts: list[bytes] = []
        parts.append(f"--{boundary}\r\n".encode())
        parts.append(b"Content-Type: application/json\r\n\r\n")
        parts.append(json.dumps(meta).encode() + b"\r\n")

        for fi in manifest.files:
            for path in (fi.data_path, fi.hint_path):
                if not os.path.exists(path):
                    continue
                name = os.path.basename(path)
                parts.append(f"--{boundary}\r\n".encode())
                parts.append(f"Content-Disposition: attachment; filename=\"{name}\"\r\n".encode())
                parts.append(b"Content-Type: application/octet-stream\r\n\r\n")
                with open(path, "rb") as f:
                    parts.append(f.read())
                parts.append(b"\r\n")

        parts.append(f"--{boundary}--\r\n".encode())
        return boundary, b"".join(parts)


class ReplicationFollower:
    """Background replication loop that pulls changes from a leader over HTTP.

    The follower long-polls the leader's sequence endpoint, fetches new entries
    as NDJSON, reconstructs DataEntry objects, and calls db.ingest().
    """

    def __init__(self, db: _bc.DB, leader_url: str) -> None:
        self._db = db
        self._leader_url = leader_url.rstrip("/")
        self._running = False
        self._thread: threading.Thread | None = None
        self._leader_seq: int = 0
        self._last_error: str = ""

    @property
    def leader_sequence(self) -> int:
        return self._leader_seq

    @property
    def follower_sequence(self) -> int:
        return self._db.current_sequence(0)

    @property
    def lag(self) -> int:
        return max(0, self._leader_seq - self.follower_sequence)

    @property
    def status(self) -> dict:
        return {
            "role": "follower",
            "mode": self._db.mode.name,
            "local_sequence": self.follower_sequence,
            "leader_sequence": self._leader_seq,
            "lag": self.lag,
            "last_error": self._last_error,
        }

    def start(self) -> None:
        self._running = True
        self._thread = threading.Thread(target=self._loop, daemon=True, name="replication")
        self._thread.start()
        log.info("replication follower started, leader=%s", self._leader_url)

    def stop(self) -> None:
        self._running = False
        if self._thread:
            self._thread.join(timeout=5)

    def _loop(self) -> None:
        while self._running:
            try:
                self._replicate_once()
            except Exception as e:
                self._last_error = str(e)
                log.warning("replication error: %s", e)
                time.sleep(2)

    @staticmethod
    def bootstrap(leader_url: str, data_dir: str) -> int:
        """Bootstrap a fresh follower by fetching the leader's manifest.

        Downloads all sealed data/hint files into data_dir. Returns the
        through_sequence so the caller can open the DB and start tailing.
        Must be called BEFORE opening the follower DB on data_dir.
        """
        leader_url = leader_url.rstrip("/")
        log.info("bootstrapping from %s into %s", leader_url, data_dir)

        dest = Path(data_dir)
        dest.mkdir(parents=True, exist_ok=True)

        with httpx.Client(timeout=120) as client:
            resp = client.get(f"{leader_url}/replication/manifest")
            resp.raise_for_status()

        boundary = b"--BCREPL"
        parts = resp.content.split(boundary)
        meta = None
        for part in parts:
            part = part.strip()
            if not part or part == b"--":
                continue
            header_end = part.find(b"\r\n\r\n")
            if header_end < 0:
                continue
            header = part[:header_end].decode(errors="replace")
            body = part[header_end + 4:].rstrip(b"\r\n")

            if "application/json" in header:
                meta = json.loads(body)
            elif "filename=" in header:
                fname_start = header.find('filename="') + 10
                fname_end = header.find('"', fname_start)
                filename = header[fname_start:fname_end]
                file_path = dest / filename
                file_path.write_bytes(body)
                log.info("bootstrap: wrote %s (%d bytes)", filename, len(body))

        if meta is None:
            raise RuntimeError("manifest response missing metadata")

        log.info("bootstrap complete, through_sequence=%d", meta["through_sequence"])
        return meta["through_sequence"]

    def _replicate_once(self) -> None:
        with httpx.Client(timeout=35) as client:
            resp = client.get(
                f"{self._leader_url}/replication/sequence",
                params={"timeout_ms": "5000"},
            )
            resp.raise_for_status()
            self._leader_seq = resp.json()["sequence"]

            local_seq = self._db.current_sequence(0)
            if local_seq >= self._leader_seq:
                return

            resp = client.get(
                f"{self._leader_url}/replication/changes",
                params={"from_seq": str(local_seq)},
            )
            resp.raise_for_status()

            entries: list[_bc.DataEntry] = []
            for line in resp.content.split(b"\n"):
                line = line.strip()
                if not line:
                    continue
                entries.append(_decode_entry(line))

            if entries:
                for i in range(0, len(entries), BATCH_SIZE):
                    batch = entries[i : i + BATCH_SIZE]
                    self._db.ingest(batch)

            self._last_error = ""

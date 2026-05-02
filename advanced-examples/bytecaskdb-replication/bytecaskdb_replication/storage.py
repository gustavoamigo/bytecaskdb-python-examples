"""Kanban/task storage on ByteCaskDB.

Key schema:
    bucket:{id}         → JSON {"id", "title", "position"}
    task:{id}           → JSON {"id", "title", "description", "done", "bucket_id", "project_id", "created", "updated"}
    meta:next_task_id   → int as UTF-8 string
    meta:next_bucket_id → int as UTF-8 string
"""

from __future__ import annotations

import json
from datetime import datetime, timezone

from bytecaskdb import _bytecaskdb as _bc


class TaskNotFound(Exception):
    pass


class BucketNotFound(Exception):
    pass


class TaskStorage:
    def __init__(self, db: _bc.DB) -> None:
        self._db = db

    def _next_id(self, key: bytes) -> int:
        raw = self._db.get(key)
        if raw is None:
            next_val = 1
        else:
            next_val = int(raw) + 1
        self._db.put(key, str(next_val).encode())
        return next_val

    def _now_iso(self) -> str:
        return datetime.now(timezone.utc).isoformat()

    def seed_defaults(self) -> None:
        if self._db.get(b"meta:next_bucket_id") is not None:
            return
        for title in ("Backlog", "In Progress", "Done"):
            self.create_bucket(title)

    def list_buckets(self) -> list[dict]:
        buckets = []
        it = self._db.iter_from(b"bucket:")
        for key, value in it:
            if not key.startswith(b"bucket:"):
                break
            buckets.append(json.loads(value))
        buckets.sort(key=lambda b: b["position"])
        return buckets

    def get_bucket(self, bucket_id: int) -> dict:
        raw = self._db.get(f"bucket:{bucket_id}".encode())
        if raw is None:
            raise BucketNotFound(bucket_id)
        return json.loads(raw)

    def find_bucket_by_slug(self, slug: str) -> dict | None:
        for b in self.list_buckets():
            if b["title"].lower().replace(" ", "-") == slug:
                return b
        return None

    def create_bucket(self, title: str) -> dict:
        bucket_id = self._next_id(b"meta:next_bucket_id")
        buckets = self.list_buckets()
        position = max((b["position"] for b in buckets), default=0) + 1
        bucket = {
            "id": bucket_id,
            "title": title,
            "position": position,
            "created": self._now_iso(),
        }
        self._db.put(f"bucket:{bucket_id}".encode(), json.dumps(bucket).encode())
        return bucket

    def list_tasks(self, bucket_id: int | None = None) -> list[dict]:
        tasks = []
        it = self._db.iter_from(b"task:")
        for key, value in it:
            if not key.startswith(b"task:"):
                break
            task = json.loads(value)
            if bucket_id is not None and task.get("bucket_id") != bucket_id:
                continue
            tasks.append(task)
        return tasks

    def get_task(self, task_id: int) -> dict:
        raw = self._db.get(f"task:{task_id}".encode())
        if raw is None:
            raise TaskNotFound(task_id)
        return json.loads(raw)

    def create_task(
        self,
        title: str,
        bucket_id: int,
        project_id: int = 1,
        description: str = "",
        done: bool = False,
    ) -> dict:
        task_id = self._next_id(b"meta:next_task_id")
        now = self._now_iso()
        task = {
            "id": task_id,
            "title": title,
            "description": description,
            "done": done,
            "bucket_id": bucket_id,
            "project_id": project_id,
            "created": now,
            "updated": now,
        }
        self._db.put(f"task:{task_id}".encode(), json.dumps(task).encode())
        return task

    def update_task(self, task_id: int, **fields) -> dict:
        task = self.get_task(task_id)
        for k, v in fields.items():
            if k in task and v is not None:
                task[k] = v
        task["updated"] = self._now_iso()
        self._db.put(f"task:{task_id}".encode(), json.dumps(task).encode())
        return task

    def move_task(self, task_id: int, bucket_id: int) -> dict:
        return self.update_task(task_id, bucket_id=bucket_id)

    def delete_task(self, task_id: int) -> None:
        self._db.del_(f"task:{task_id}".encode())

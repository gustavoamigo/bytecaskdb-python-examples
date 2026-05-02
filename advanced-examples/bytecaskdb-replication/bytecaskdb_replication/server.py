"""Falcon WSGI application: Vikunja-compatible REST API + replication endpoints.

Routes:
  Vikunja API (compatible with the vikunja CLI script):
    GET  /api/v1/projects/{pid}/views/{vid}/buckets
    GET  /api/v1/projects/{pid}/views/{vid}/tasks
    GET  /api/v1/tasks/{tid}
    PUT  /api/v1/projects/{pid}/tasks
    POST /api/v1/tasks/{tid}
    POST /api/v1/projects/{pid}/views/{vid}/buckets/{bid}/tasks

  Replication:
    GET  /replication/sequence
    GET  /replication/changes
    GET  /replication/manifest
    GET  /replication/status
"""

from __future__ import annotations

import falcon
import httpx

from .replicator import ReplicationFollower, ReplicationLeader
from .storage import BucketNotFound, TaskNotFound, TaskStorage


# ---------------------------------------------------------------------------
# Error handlers
# ---------------------------------------------------------------------------

def _handle_task_not_found(req, resp, ex, params):
    resp.status = falcon.HTTP_404
    resp.media = {"message": f"task {ex} not found"}


def _handle_bucket_not_found(req, resp, ex, params):
    resp.status = falcon.HTTP_404
    resp.media = {"message": f"bucket {ex} not found"}


# ---------------------------------------------------------------------------
# Middleware: follower write proxy
# ---------------------------------------------------------------------------

class WriteProxyMiddleware:
    """Transparently proxies write requests to the leader when in follower mode."""

    def __init__(self, leader_url: str) -> None:
        self._leader_url = leader_url.rstrip("/")

    def process_request(self, req: falcon.Request, resp: falcon.Response) -> None:
        if req.method not in ("PUT", "POST", "DELETE"):
            return
        if not req.path.startswith("/api/"):
            return

        url = f"{self._leader_url}{req.path}"
        if req.query_string:
            url += f"?{req.query_string}"

        body = req.bounded_stream.read()
        headers = {}
        if req.content_type:
            headers["Content-Type"] = req.content_type

        with httpx.Client(timeout=30) as client:
            upstream = client.request(req.method, url, content=body, headers=headers)

        resp.status = str(upstream.status_code)
        resp.content_type = upstream.headers.get("content-type", "application/json")
        resp.data = upstream.content
        resp.complete = True


# ---------------------------------------------------------------------------
# Vikunja task API resources
# ---------------------------------------------------------------------------

class BucketsResource:
    def __init__(self, storage: TaskStorage) -> None:
        self._storage = storage

    def on_get(self, req: falcon.Request, resp: falcon.Response, pid: int, vid: int) -> None:
        resp.media = self._storage.list_buckets()


class TasksViewResource:
    def __init__(self, storage: TaskStorage) -> None:
        self._storage = storage

    def on_get(self, req: falcon.Request, resp: falcon.Response, pid: int, vid: int) -> None:
        page = req.get_param_as_int("page", default=1)
        if page > 1:
            resp.media = []
            return
        buckets = self._storage.list_buckets()
        result = []
        for bucket in buckets:
            tasks = self._storage.list_tasks(bucket_id=bucket["id"])
            for t in tasks:
                t["_bucket_id"] = bucket["id"]
                t["_bucket_title"] = bucket["title"]
            result.append({"id": bucket["id"], "title": bucket["title"], "tasks": tasks})
        resp.media = result


class TaskResource:
    def __init__(self, storage: TaskStorage) -> None:
        self._storage = storage

    def on_get(self, req: falcon.Request, resp: falcon.Response, tid: int) -> None:
        task = self._storage.get_task(tid)
        bucket = self._storage.get_bucket(task["bucket_id"])
        task["_bucket_id"] = bucket["id"]
        task["_bucket_title"] = bucket["title"]
        resp.media = task

    def on_post(self, req: falcon.Request, resp: falcon.Response, tid: int) -> None:
        body = req.get_media()
        task = self._storage.update_task(tid, **body)
        resp.media = task


class CreateTaskResource:
    def __init__(self, storage: TaskStorage) -> None:
        self._storage = storage

    def on_put(self, req: falcon.Request, resp: falcon.Response, pid: int) -> None:
        body = req.get_media()
        bucket_id = body.get("bucket_id")
        if bucket_id is None:
            buckets = self._storage.list_buckets()
            bucket_id = buckets[0]["id"] if buckets else 1
        task = self._storage.create_task(
            title=body["title"],
            bucket_id=bucket_id,
            project_id=body.get("project_id", 1),
            description=body.get("description", ""),
            done=body.get("done", False),
        )
        resp.status = falcon.HTTP_201
        resp.media = task


class MoveTaskResource:
    def __init__(self, storage: TaskStorage) -> None:
        self._storage = storage

    def on_post(self, req: falcon.Request, resp: falcon.Response, pid: int, vid: int, bid: int) -> None:
        body = req.get_media()
        task = self._storage.move_task(body["task_id"], bid)
        resp.media = task


# ---------------------------------------------------------------------------
# Replication resources
# ---------------------------------------------------------------------------

class ReplicationSequenceResource:
    def __init__(self, leader: ReplicationLeader) -> None:
        self._leader = leader

    def on_get(self, req: falcon.Request, resp: falcon.Response) -> None:
        timeout_ms = req.get_param_as_int("timeout_ms", default=0)
        resp.media = self._leader.get_sequence(timeout_ms)


class ReplicationChangesResource:
    def __init__(self, leader: ReplicationLeader) -> None:
        self._leader = leader

    def on_get(self, req: falcon.Request, resp: falcon.Response) -> None:
        from_seq = req.get_param_as_int("from_seq", default=0)
        resp.content_type = "application/x-ndjson"
        resp.data = self._leader.get_changes(from_seq)


class ReplicationManifestResource:
    def __init__(self, leader: ReplicationLeader) -> None:
        self._leader = leader

    def on_get(self, req: falcon.Request, resp: falcon.Response) -> None:
        boundary, body = self._leader.get_manifest()
        resp.content_type = f"multipart/mixed; boundary={boundary}"
        resp.data = body


class ReplicationStatusResource:
    def __init__(self, leader: ReplicationLeader | None, follower: ReplicationFollower | None) -> None:
        self._leader = leader
        self._follower = follower

    def on_get(self, req: falcon.Request, resp: falcon.Response) -> None:
        if self._follower:
            resp.media = self._follower.status
        elif self._leader:
            resp.media = self._leader.get_status()
        else:
            resp.media = {"role": "standalone", "sequence": 0}


# ---------------------------------------------------------------------------
# App factory
# ---------------------------------------------------------------------------

def create_app(
    storage: TaskStorage,
    leader: ReplicationLeader | None = None,
    follower: ReplicationFollower | None = None,
    leader_url: str | None = None,
) -> falcon.App:
    middleware = []
    if follower and leader_url:
        middleware.append(WriteProxyMiddleware(leader_url))

    app = falcon.App(middleware=middleware)
    app.add_error_handler(TaskNotFound, _handle_task_not_found)
    app.add_error_handler(BucketNotFound, _handle_bucket_not_found)

    app.add_route("/api/v1/projects/{pid:int}/views/{vid:int}/buckets", BucketsResource(storage))
    app.add_route("/api/v1/projects/{pid:int}/views/{vid:int}/tasks", TasksViewResource(storage))
    app.add_route("/api/v1/tasks/{tid:int}", TaskResource(storage))
    app.add_route("/api/v1/projects/{pid:int}/tasks", CreateTaskResource(storage))
    app.add_route("/api/v1/projects/{pid:int}/views/{vid:int}/buckets/{bid:int}/tasks", MoveTaskResource(storage))

    if leader:
        app.add_route("/replication/sequence", ReplicationSequenceResource(leader))
        app.add_route("/replication/changes", ReplicationChangesResource(leader))
        app.add_route("/replication/manifest", ReplicationManifestResource(leader))

    app.add_route("/replication/status", ReplicationStatusResource(leader, follower))

    return app

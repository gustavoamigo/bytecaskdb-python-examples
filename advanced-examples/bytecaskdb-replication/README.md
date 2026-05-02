# ByteCaskDB Replicated Task Server

A Vikunja-compatible task/kanban server with **leader/follower replication** using
ByteCaskDB's replication primitives. The existing `vikunja` CLI works unmodified
against either node.

## Architecture

```
┌─────────────────────┐         NDJSON/HTTP         ┌─────────────────────┐
│      Leader         │◄────────────────────────────│      Follower       │
│  (reads + writes)   │  changes_since → ingest     │   (reads + proxy)   │
│  port 8100          │                             │   port 8101         │
└─────────────────────┘                             └─────────────────────┘
         ▲                                                    ▲
         │                                                    │
    vikunja CLI                                          vikunja CLI
    (any operation)                                   (writes proxied to leader)
```

**Replication flow:**
1. Follower long-polls leader's `/replication/sequence` endpoint
2. When new data is available, fetches entries via `/replication/changes?from_seq=N`
3. Entries are serialized as NDJSON (base64-encoded keys/values)
4. Follower reconstructs `DataEntry` objects and calls `db.ingest()`
5. Reads are served locally from the follower's replicated state

## Requirements

- **Python 3.14t** (free-threaded build — replication primitives require it)
- ByteCaskDB with replication support
- Granian (WSGI server)
- httpx (HTTP client for replication + write proxy)

```bash
pip install -r requirements.txt
```

## Usage

### Start the leader

```bash
python3.14t run_server.py --role leader --port 8100 --data-dir ./leader_data
```

### Start a follower (separate terminal or machine)

```bash
# Without bootstrap (replicates full history from seq 0 via changes_since):
python3.14t run_server.py --role follower --port 8101 --data-dir ./follower_data \
    --leader-url http://localhost:8100

# With bootstrap (downloads sealed files via create_manifest, then tails):
python3.14t run_server.py --role follower --port 8101 --data-dir ./follower_data \
    --leader-url http://localhost:8100 --bootstrap
```

Bootstrap is faster for large databases — it ships the sealed data/hint files
directly instead of replaying every entry through `changes_since`.

### Use the vikunja CLI against either node

```bash
export VIKUNJA_URL=http://localhost:8100  # or :8101 for follower
export VIKUNJA_TOKEN=dummy               # auth is not enforced

vikunja list
vikunja add backlog "New task"
vikunja add in-progress "Working on something"
vikunja done 1
vikunja show 1
```

Writes against the follower are transparently proxied to the leader, then
replicated back within seconds.

### Check replication status

```bash
curl http://localhost:8101/replication/status
# {"role":"follower","mode":"Follower","local_sequence":14,"leader_sequence":14,"lag":0,"last_error":""}
```

## Replication Primitives Used

| Primitive | Role | Purpose |
|-----------|------|---------|
| `current_sequence(timeout_ms)` | Leader | Long-poll: block until new durable data |
| `snapshot()` | Leader | Capture consistent read boundary |
| `changes_since(snap, from_seq)` | Leader | Stream entries since follower's position |
| `create_manifest()` | Leader | Bootstrap: rotate + return sealed files |
| `ingest(entries)` | Follower | Apply leader's entries preserving sequences |
| `Mode.Follower` | Follower | Block direct writes, allow only ingest |

## Key Design Decisions

- **NDJSON wire format** — human-readable, debuggable with curl, easy incremental parsing
- **Write proxy on follower** — clients don't need to know who the leader is
- **Portable replicator module** — `replicator.py` is generic; can be reused with the blob server or any bytecaskdb-backed app
- **No bootstrap needed** — followers replicate from sequence 0 using `changes_since`; works as long as the leader retains its history (no vacuum of needed files)

## File Structure

```
bytecaskdb_replication/
    __init__.py
    replicator.py   ← portable: ReplicationLeader + ReplicationFollower
    storage.py      ← task/kanban storage on bytecaskdb
    server.py       ← WSGI app: Vikunja API + replication endpoints
run_server.py       ← Granian entry point
```

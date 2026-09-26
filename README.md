# Stream Compaction Coordinator

Stream Compaction Coordinator is a Go backend for managing append-only event
streams whose physical storage is divided into half-open segments. It records
segment boundaries and states, accepts record-batch metadata, coordinates
compaction plans, collects replica acknowledgements, and restores a safe
stream watermark after restart.

The service owns control metadata rather than event payloads. A compaction
worker performs the physical merge outside the coordinator, then reports the
result checksum and byte count. The coordinator validates ownership, offset
continuity, generation, state, and durability before retiring source
segments.

## Requirements

- Go 1.26 or newer
- A writable local directory for the state snapshot

No external database, message system, or online dependency is used.

## Build and run

```bash
go build -o .build/segmentd ./cmd/segmentd
.build/segmentd serve
```

The default listener is `127.0.0.1:8080`. The default state snapshot is
`.segmentd/state.json`.

```bash
curl -s http://127.0.0.1:8080/v1/healthz
```

Graceful shutdown starts on `SIGINT` or `SIGTERM`.

## Runtime settings

| Variable | Default | Purpose |
| --- | --- | --- |
| `SEGMENTD_ADDR` | `127.0.0.1:8080` | TCP listener address |
| `SEGMENTD_STATE_PATH` | `.segmentd/state.json` | Durable snapshot path |
| `SEGMENTD_MAX_REQUEST_BYTES` | `1048576` | Largest decoded JSON request |
| `SEGMENTD_REQUEST_TIMEOUT` | `5s` | Per-request deadline |
| `SEGMENTD_SHUTDOWN_TIMEOUT` | `10s` | Graceful shutdown bound |
| `SEGMENTD_MAX_APPEND_RECORDS` | `10000` | Largest accepted record count |
| `SEGMENTD_MAX_APPEND_BYTES` | `16777216` | Largest accepted batch byte count |

## API overview

Create a stream:

```text
POST /v1/streams
{
  "namespace": "field-lab",
  "name": "edge-events",
  "threshold_bytes": 64
}
```

Append batch metadata:

```text
POST /v1/streams/{stream_id}/records
{
  "expected_revision": 1,
  "record_count": 4,
  "payload_bytes": 40
}
```

When the active segment reaches the configured threshold, it becomes sealed
and a new open segment starts at the same end offset. The accepted request
increments the stream revision once, even when the request causes rollover.

Create, start, and finish a compaction plan:

```text
POST /v1/streams/{stream_id}/compaction-plans
{
  "source_segment_ids": ["seg_a", "seg_b"],
  "expected_stream_revision": 5
}

POST /v1/compaction-plans/{plan_id}/start
{}

POST /v1/compaction-plans/{plan_id}/finish
{
  "result_checksum": "sha256:...",
  "result_size_bytes": 8192
}
```

A finish request creates one sealed result segment, retires the source
segments, updates the compaction floor, and commits all changes in one state
snapshot.

Acknowledge a replica:

```text
POST /v1/segments/{segment_id}/replica-acks
{
  "node_id": "edge-a",
  "generation": 1,
  "offset": 64
}
```

Read endpoints:

```text
GET /v1/streams/{stream_id}
GET /v1/streams/{stream_id}/segments?states=sealed&limit=100&after=seg_id
GET /v1/streams/{stream_id}/watermark
GET /v1/segments/{segment_id}
GET /v1/compaction-plans/{plan_id}
GET /v1/healthz
```

Administrative recovery can be requested again after startup:

```text
POST /v1/admin/recover
{}
```

Every error uses one JSON object:

```json
{
  "error": {
    "code": "stale_revision",
    "message": "stream revision does not match",
    "details": {
      "expected": 3,
      "current": 4
    }
  }
}
```

## Workflow checks

The initialization baseline has no test suite. Tests are intentionally
deferred to a later verification task stage. Until then, these bounded
production checks exercise the real HTTP handler and file-backed state:

```bash
go run ./cmd/segmentd verify append-rollover
go run ./cmd/segmentd verify compaction-lineage
go run ./cmd/segmentd verify compaction-recovery
go run ./cmd/segmentd verify replica-watermark
```

Each command creates a temporary state directory, performs its workflow, and
prints the workflow name, assertion count, and status. The commands do not
contact a network service and clean up their temporary data.

## Directory structure

```text
cmd/segmentd/              process entry point
internal/config/           environment parsing and bounds
internal/domain/           entities, ranges, transitions, validation
internal/storage/          snapshot codec, atomic commit, catalog queries
internal/application/      workflow orchestration and recovery
internal/httpapi/          JSON routes, decoding, errors, middleware
internal/runtimecheck/     production workflow checks
internal/observability/    structured operation records
internal/idgen/            opaque entity identifiers
internal/clock/            system and deterministic time sources
```

## Persistence and recovery

Every accepted operation clones the current state, applies one workflow,
validates all invariants, writes a temporary sibling snapshot, synchronizes
it, renames it over the current snapshot, and synchronizes the parent
directory. The in-memory state is replaced only after the write succeeds.

On startup, the service loads the snapshot, aborts plans left in `running`
state, returns their source segments to `sealed`, validates all references,
and recomputes each stream watermark. A damaged snapshot fails startup instead
of serving an uncertain state.

Compaction results are held to a strict reference closure: every source segment
must resolve, belong to the same stream, be retired, and match exactly one
completed plan's record, and no other completed plan may claim the same
sources. A snapshot whose compacted segment references missing, foreign,
unretired, or multiply claimed sources is rejected before the listener starts,
and the snapshot file is left untouched.

## Deferred verification boundary

The later task stage will add package tests, HTTP workflow tests, restart
coverage, and race checks. The production checks listed above are the current
release gate and should remain valid after that stage adds a broader suite.

## Intentionally omitted

- Event payload storage and byte transfer
- Remote replication and quorum coordination
- Authentication and authorization
- Multi-process coordination
- External databases and online services
- Visual user interfaces

# Stream Compaction Coordinator

## Purpose

Stream Compaction Coordinator is a self-contained backend service that keeps
append-only event streams bounded by compacting sealed storage segments. It is
designed for teams that operate an event capture tier and need a deterministic
control plane for segment rollover, compaction plans, replica progress, and
restart recovery.

The service owns metadata, not event payloads. A caller supplies byte counts
and a checksum for compacted output; the coordinator validates ownership,
offset continuity, state transitions, and replica safety before advancing the
stream watermark.

## Users

- **Stream operator** registers streams and inspects their current boundaries.
- **Ingest worker** appends record batches to an open segment.
- **Compaction worker** proposes a range, executes the physical work outside
  the coordinator, and reports the resulting segment.
- **Replica agent** acknowledges a durable offset for a segment.
- **Recovery process** loads the durable snapshot and refuses to expose an
  unsafe watermark after a partial write.

## Core entities

### Stream

A logical append-only event sequence. It has a namespace, name, active segment,
sealed segment identifiers, a durable watermark, a compaction floor, a
monotonic revision, and timestamps.

### Segment

A half-open byte-offset interval `[base_offset, end_offset)` belonging to one
stream and one generation. States are:

```text
open -> sealed -> compacting -> retired
                 \-> sealed (aborted plan)
```

A compacted segment records its source segment identifiers and cannot later be
used as a source in another plan. Replica acknowledgements never move
backwards.

### Compaction plan

A durable request to replace a contiguous list of sealed source segments with
one compacted segment. Plan states are:

```text
proposed -> running -> completed
                    \-> aborted
```

A plan captures the expected stream revision and source segment generations.
Completion is idempotent only when the same result payload is presented.

### Replica acknowledgement

A node-specific high-water mark for one segment. The coordinator rejects
offsets outside the segment interval and accepts repeated values as no-ops.
The stream watermark advances only when all current source segments have at
least one acknowledgement at or beyond their sealed end.

### Audit event

An append-only operational record for accepted mutations. Audit events contain
the action, stream identifier, entity identifier, revision, and timestamp.

## Workflows

### 1. Register, append, and rotate

Entry points:

```text
create /v1/streams
create /v1/streams/{stream_id}/records
read   /v1/streams/{stream_id}
```

1. Validate the namespace, stream name, segment threshold, and replica policy.
2. Create a stream with an empty open segment at offset zero.
3. Validate append batch size and the caller's expected stream revision.
4. Advance the active segment end offset and byte count atomically.
5. If the configured threshold is reached, seal the active segment and create
   the next open segment at the previous end offset.
6. Persist the new snapshot and audit event before returning the new revision.

Failure behavior:

- malformed identifiers or names return a stable validation error;
- append requests against a stale stream revision return a conflict;
- integer overflow, zero-sized batches, and oversized batches are rejected;
- persistence failures keep the in-memory state unchanged.

### 2. Plan and finish compaction

Entry points:

```text
create /v1/streams/{stream_id}/compaction-plans
create /v1/compaction-plans/{plan_id}/start
create /v1/compaction-plans/{plan_id}/finish
read   /v1/compaction-plans/{plan_id}
```

1. Validate that every source segment belongs to the stream and is sealed.
2. Require contiguous half-open ranges and a non-empty candidate list.
3. Freeze the candidate identifiers and stream revision in a proposed plan.
4. Start the plan, moving every source segment to `compacting`.
5. Finish with a checksum, output byte count, and generated segment interval.
6. In one storage mutation: create the compacted segment, retire source
   segments, update the stream watermark, and mark the plan completed.
7. Recover interrupted running plans by returning them to `sealed` source
   state and marking the plan aborted during startup.

Failure behavior:

- gaps, overlaps, duplicate sources, and open sources return validation errors;
- starting or finishing an unknown plan returns 404;
- an invalid finish payload keeps the plan running with all sources recoverable;
- a repeated identical finish request returns the already completed plan.

### 3. Acknowledge replicas and recover the watermark

Entry points:

```text
create /v1/segments/{segment_id}/replica-acks
read   /v1/streams/{stream_id}/watermark
create /v1/admin/recover
```

1. Validate segment ownership, node identifier, segment generation, and
   acknowledged offset.
2. Ignore duplicate offsets and reject decreasing offsets.
3. Advance a segment to durable only when at least one acknowledgement reaches
   its sealed end.
4. Recompute the stream watermark as the smallest durable end offset among
   live sealed and compacted segments.
5. On recovery, reload the snapshot, validate all ranges and references, abort
   running plans, and recompute the watermark before serving requests.

Failure behavior:

- acknowledgements for retired segments are rejected;
- offsets beyond the segment end or below its base return validation errors;
- a damaged snapshot returns a recovery error and no service is started;
- watermark reads never report past the smallest safe source boundary.

## Modules and dependency direction

```text
cmd/segmentd
  -> config
  -> httpapi -> application -> domain
                    |             ^
                    v             |
                  storage --------+
  -> runtimecheck -> httpapi/application/storage
```

| Module | Responsibility |
| --- | --- |
| `config` | Environment parsing, bounded durations, defaults |
| `domain` | Entities, range arithmetic, state transitions, validation |
| `storage` | Snapshot codec, atomic persistence, catalog queries |
| `application` | Workflow orchestration, concurrency boundary, recovery |
| `httpapi` | JSON entry points, error mapping, request limits |
| `runtimecheck` | Bounded production-path workflow probes |
| `observability` | Structured audit and operation events |
| `idgen` | Collision-resistant entity identifiers |
| `clock` | Injectable time source for deterministic checks |

The domain package does not import storage or transport. The application
package coordinates domain and storage operations under one mutation gate. HTTP
handlers perform transport validation only and delegate business decisions to
the application service.

## State and invariants

- All segment ranges are half-open and must satisfy `0 <= base < end`.
- Segments in one stream cannot overlap.
- The active segment is always open; sealed source segments are never active.
- Stream revision increments once per accepted mutation.
- A stream watermark is monotonic and never exceeds the smallest live
  acknowledged end offset.
- A completed compaction plan has exactly one compacted result segment.
- A compacted segment's sources all resolve, belong to one stream, are
  retired, match exactly one completed plan's record, and are claimed as
  sources by no other completed plan.
- Source segments are retired only when the result segment is durably stored.
- A restart either restores the last complete snapshot or fails closed.
- Concurrent append and compaction requests serialize at one mutation gate.

## Public interface

All request and response bodies are JSON. Identifiers are opaque strings.
Offsets are signed 64-bit values because values above the accepted maximum are
rejected before arithmetic.

Common error shape:

```json
{
  "error": {
    "code": "invalid_range",
    "message": "source segment ranges must be contiguous",
    "details": {
      "stream_id": "str_example"
    }
  }
}
```

The service exposes bounded request sizes, request timeouts, and graceful
shutdown. It does not connect to external systems.

## Persistence

The storage layer keeps one project-specific snapshot at
`SEGMENTD_STATE_PATH` (default `.segmentd/state.json`). Writes use a temporary
sibling, `fsync`, rename, and directory `fsync`. A process-level mutex protects
the in-memory catalog while a mutation is cloned, validated, persisted, and
committed. Reads return immutable snapshot values.

## Runtime commands

```text
go build ./cmd/segmentd
go run ./cmd/segmentd serve
go run ./cmd/segmentd verify append-rollover
go run ./cmd/segmentd verify compaction-lineage
go run ./cmd/segmentd verify compaction-recovery
go run ./cmd/segmentd verify replica-watermark
```

The `verify` commands construct a temporary storage directory and exercise the real HTTP
handler through `httptest`. They are production smoke checks, not a test suite.

## Validation plan and deferred tests

Tests are intentionally deferred in this initialization baseline. The later
verification stage will add package-level tests, HTTP workflow tests, restart
tests, and race checks. Before that stage, the three `verify` commands provide
bounded end-to-end checks over the production entry points and persistence
path.

## Deliberate bug-injection surface

The healthy baseline keeps the following behavior separated behind small
interfaces so future task variants can alter one concern without creating a
toy patch:

- half-open interval boundary calculations;
- threshold comparison and segment rollover;
- state transition guards;
- revision and idempotency checks;
- replica acknowledgement monotonicity;
- watermark reduction over source segments;
- plan recovery on restart;
- atomic snapshot publication;
- JSON error mapping and status selection;
- request-size and overflow validation;
- concurrent mutation serialization.

## Out of scope

- Event payload storage, network replication, object storage, or quorum coordination.
- Authentication and authorization.
- Multi-process coordination and external databases.
- Metrics dashboards or visual user interfaces.

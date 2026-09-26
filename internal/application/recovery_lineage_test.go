package application_test

import (
	"bytes"
	"context"
	"encoding/json"
	"net/http"
	"net/http/httptest"
	"os"
	"path/filepath"
	"testing"
	"time"

	"github.com/1260124186-cc/solo-0017-stream-compaction/internal/application"
	"github.com/1260124186-cc/solo-0017-stream-compaction/internal/clock"
	"github.com/1260124186-cc/solo-0017-stream-compaction/internal/domain"
	"github.com/1260124186-cc/solo-0017-stream-compaction/internal/httpapi"
	"github.com/1260124186-cc/solo-0017-stream-compaction/internal/idgen"
	"github.com/1260124186-cc/solo-0017-stream-compaction/internal/observability"
	"github.com/1260124186-cc/solo-0017-stream-compaction/internal/storage"
)

type compactionSnapshot struct {
	state        *domain.State
	streamID     string
	planID       string
	resultID     string
	sourceIDs    []string
	foreignState *domain.State
}

func TestCompleteCompactionSnapshotServesFromAllReadEntryPoints(t *testing.T) {
	snapshot := buildCompactionSnapshot(t)
	path := writeSnapshot(t, t.TempDir(), snapshot.state)

	service := startService(t, path)
	server := httptest.NewServer(httpapi.New(service, httpapi.Options{
		Logger: observability.Discard{},
	}).Handler())
	defer server.Close()

	ctx := context.Background()
	var plan domain.CompactionPlan
	getJSON(t, ctx, server.URL+"/v1/compaction-plans/"+snapshot.planID, &plan)
	if plan.ID != snapshot.planID || plan.State != domain.PlanStateCompleted {
		t.Fatalf("plan entry returned %s:%s, want %s:completed", plan.ID, plan.State, snapshot.planID)
	}
	if plan.ResultSegmentID != snapshot.resultID {
		t.Fatalf("plan result = %q, want %q", plan.ResultSegmentID, snapshot.resultID)
	}

	var result domain.Segment
	getJSON(t, ctx, server.URL+"/v1/segments/"+snapshot.resultID, &result)
	if result.ID != snapshot.resultID || result.StreamID != snapshot.streamID {
		t.Fatalf("result entry returned %s for stream %s", result.ID, result.StreamID)
	}
	if len(result.SourceSegmentIDs) != len(snapshot.sourceIDs) {
		t.Fatalf("result has %d sources, want %d", len(result.SourceSegmentIDs), len(snapshot.sourceIDs))
	}
	for index, sourceID := range snapshot.sourceIDs {
		if result.SourceSegmentIDs[index] != sourceID {
			t.Fatalf("result source %d = %q, want %q", index, result.SourceSegmentIDs[index], sourceID)
		}
	}

	for _, sourceID := range snapshot.sourceIDs {
		var source domain.Segment
		getJSON(t, ctx, server.URL+"/v1/segments/"+sourceID, &source)
		if source.ID != sourceID || source.StreamID != snapshot.streamID {
			t.Fatalf("source entry returned %s for stream %s, want %s for stream %s",
				source.ID, source.StreamID, sourceID, snapshot.streamID)
		}
		if source.State != domain.SegmentStateRetired {
			t.Fatalf("source %s state = %s, want retired", source.ID, source.State)
		}
	}
}

func TestCorruptCompactionSnapshotsFailBeforeServingAndKeepFile(t *testing.T) {
	tests := []struct {
		name    string
		corrupt func(*compactionSnapshot)
	}{
		{
			name: "missing source",
			corrupt: func(snapshot *compactionSnapshot) {
				delete(snapshot.state.Segments, snapshot.sourceIDs[1])
			},
		},
		{
			name: "cross stream source",
			corrupt: func(snapshot *compactionSnapshot) {
				for id, stream := range snapshot.foreignState.Streams {
					snapshot.state.Streams[id] = stream
				}
				for id, segment := range snapshot.foreignState.Segments {
					snapshot.state.Segments[id] = segment
				}
				snapshot.state.Segments[snapshot.sourceIDs[0]].StreamID = firstStreamID(snapshot.foreignState)
			},
		},
		{
			name: "state inconsistent lineage takeover",
			corrupt: func(snapshot *compactionSnapshot) {
				firstResult := snapshot.state.Segments[snapshot.resultID]
				secondResultID := "seg_takeover_result"
				secondPlanID := "cmp_takeover"
				now := firstResult.CreatedAt.Add(time.Second)
				secondResult := &domain.Segment{
					ID:               secondResultID,
					StreamID:         snapshot.streamID,
					Generation:       firstResult.Generation + 1,
					Range:            firstResult.Range,
					State:            domain.SegmentStateSealed,
					RecordCount:      firstResult.RecordCount,
					SizeBytes:        firstResult.SizeBytes,
					Checksum:         "sha256:takeover",
					SourceSegmentIDs: append([]string(nil), snapshot.sourceIDs...),
					Durable:          true,
					ReplicaAcks:      map[string]domain.ReplicaAck{},
					CreatedAt:        now,
					UpdatedAt:        now,
					SealedAt:         &now,
				}
				secondPlan := &domain.CompactionPlan{
					ID:                     secondPlanID,
					StreamID:               snapshot.streamID,
					SourceSegmentIDs:       append([]string(nil), snapshot.sourceIDs...),
					SourceGenerations:      snapshot.state.Plans[snapshot.planID].SourceGenerations,
					Range:                  firstResult.Range,
					State:                  domain.PlanStateCompleted,
					ExpectedStreamRevision: snapshot.state.Plans[snapshot.planID].ExpectedStreamRevision,
					Revision:               3,
					ResultSegmentID:        secondResultID,
					ResultChecksum:         secondResult.Checksum,
					ResultSizeBytes:        secondResult.SizeBytes,
					CreatedAt:              now,
					UpdatedAt:              now,
					CompletedAt:            &now,
				}
				stream := snapshot.state.Streams[snapshot.streamID]
				stream.SealedSegmentIDs = append(stream.SealedSegmentIDs, secondResultID)
				snapshot.state.Segments[secondResultID] = secondResult
				snapshot.state.Plans[secondPlanID] = secondPlan
			},
		},
		{
			name: "result source disagrees with plan record",
			corrupt: func(snapshot *compactionSnapshot) {
				snapshot.state.Segments[snapshot.resultID].SourceSegmentIDs = append(
					[]string(nil),
					snapshot.sourceIDs[0],
					snapshot.sourceIDs[0],
				)
			},
		},
	}

	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			snapshot := buildCompactionSnapshot(t)
			test.corrupt(snapshot)
			path := writeSnapshot(t, t.TempDir(), snapshot.state)
			original, err := os.ReadFile(path)
			if err != nil {
				t.Fatalf("read corrupt snapshot: %v", err)
			}

			repo, err := storage.NewFileRepository(path)
			if err != nil {
				t.Fatalf("create repository: %v", err)
			}
			service, err := application.NewService(
				repo,
				idgenRandom(t),
				clock.NewFixed(time.Date(2026, 1, 2, 0, 0, 0, 0, time.UTC), time.Second),
				observability.Discard{},
				application.Limits{},
			)
			if err == nil {
				t.Fatal("service started from an unrecoverable compaction snapshot")
			}
			if service != nil {
				t.Fatal("failed recovery returned a non-nil service")
			}
			if !domain.IsCode(err, domain.CodeInconsistentState) {
				t.Fatalf("recovery error = %v, want code %s", err, domain.CodeInconsistentState)
			}
			preserved, err := os.ReadFile(path)
			if err != nil {
				t.Fatalf("read preserved snapshot: %v", err)
			}
			if !bytes.Equal(original, preserved) {
				t.Fatal("failed recovery modified the original snapshot")
			}
		})
	}
}

func buildCompactionSnapshot(t *testing.T) *compactionSnapshot {
	t.Helper()

	ctx := context.Background()
	now := time.Date(2026, 1, 1, 0, 0, 0, 0, time.UTC)
	ids := &idgen.Counter{}
	clockSource := clock.NewFixed(now, time.Second)
	repo := storage.NewInMemoryRepository(filepath.Join(t.TempDir(), "state.json"), nil)
	service, err := application.NewService(
		repo,
		ids,
		clockSource,
		observability.Discard{},
		application.Limits{MaxAppendRecords: 100, MaxAppendBytes: 1 << 20},
	)
	if err != nil {
		t.Fatalf("create initial service: %v", err)
	}

	stream, err := service.CreateStream(ctx, application.CreateStreamCommand{
		Namespace:      "field-lab",
		Name:           "lineage-recovery",
		ThresholdBytes: 32,
	})
	if err != nil {
		t.Fatalf("create stream: %v", err)
	}
	for index, payload := range []int64{20, 20, 20, 20} {
		if _, err := service.AppendRecords(ctx, application.AppendRecordsCommand{
			StreamID:         stream.Stream.ID,
			ExpectedRevision: uint64(index + 1),
			RecordCount:      2,
			PayloadBytes:     payload,
		}); err != nil {
			t.Fatalf("append batch %d: %v", index, err)
		}
	}
	segments, err := service.ListSegments(ctx, storage.SegmentFilter{
		StreamID: stream.Stream.ID,
		Limit:    100,
	})
	if err != nil {
		t.Fatalf("list segments: %v", err)
	}
	if len(segments) < 3 {
		t.Fatalf("got %d segments, want at least three", len(segments))
	}
	sourceIDs := []string{segments[0].ID, segments[1].ID}

	plan, err := service.CreateCompactionPlan(ctx, application.CreateCompactionPlanCommand{
		StreamID:               stream.Stream.ID,
		SourceSegmentIDs:       sourceIDs,
		ExpectedStreamRevision: stream.Stream.Revision + 4,
	})
	if err != nil {
		t.Fatalf("create plan: %v", err)
	}
	if _, err := service.StartCompactionPlan(ctx, application.StartCompactionPlanCommand{
		PlanID: plan.Plan.ID,
	}); err != nil {
		t.Fatalf("start plan: %v", err)
	}
	finished, err := service.FinishCompactionPlan(ctx, application.FinishCompactionPlanCommand{
		PlanID:          plan.Plan.ID,
		ResultChecksum:  "sha256:compacted",
		ResultSizeBytes: 160,
	})
	if err != nil {
		t.Fatalf("finish plan: %v", err)
	}

	state, err := repo.Load()
	if err != nil {
		t.Fatalf("load completed snapshot: %v", err)
	}
	foreign := buildForeignStreamState(t, now.Add(time.Hour))
	return &compactionSnapshot{
		state:        state,
		streamID:     stream.Stream.ID,
		planID:       plan.Plan.ID,
		resultID:     finished.ResultSegment.ID,
		sourceIDs:    sourceIDs,
		foreignState: foreign,
	}
}

func buildForeignStreamState(t *testing.T, now time.Time) *domain.State {
	t.Helper()

	streamID := "str_foreign"
	activeID := "seg_foreign_active"
	stream, active, err := domain.NewStream(
		streamID,
		activeID,
		"field-lab",
		"foreign-lineage",
		32,
		now,
	)
	if err != nil {
		t.Fatalf("create foreign stream: %v", err)
	}
	return &domain.State{
		SchemaVersion: domain.SnapshotSchemaVersion,
		Streams:       map[string]*domain.Stream{streamID: stream},
		Segments:      map[string]*domain.Segment{activeID: active},
		Plans:         map[string]*domain.CompactionPlan{},
		Audit:         []domain.AuditEvent{},
		UpdatedAt:     now,
	}
}

func firstStreamID(state *domain.State) string {
	for streamID := range state.Streams {
		return streamID
	}
	return ""
}

func writeSnapshot(t *testing.T, directory string, state *domain.State) string {
	t.Helper()
	data, err := storage.MarshalState(state)
	if err != nil {
		t.Fatalf("marshal snapshot: %v", err)
	}
	path := filepath.Join(directory, "state.json")
	if err := os.WriteFile(path, data, 0o600); err != nil {
		t.Fatalf("write snapshot: %v", err)
	}
	return path
}

func startService(t *testing.T, path string) *application.Service {
	t.Helper()
	repo, err := storage.NewFileRepository(path)
	if err != nil {
		t.Fatalf("create repository: %v", err)
	}
	service, err := application.NewService(
		repo,
		idgenRandom(t),
		clock.NewFixed(time.Date(2026, 1, 2, 0, 0, 0, 0, time.UTC), time.Second),
		observability.Discard{},
		application.Limits{},
	)
	if err != nil {
		t.Fatalf("start service from complete snapshot: %v", err)
	}
	return service
}

func idgenRandom(t *testing.T) *idgen.Random {
	t.Helper()
	return &idgen.Random{}
}

func getJSON(t *testing.T, ctx context.Context, url string, target any) {
	t.Helper()
	request, err := http.NewRequestWithContext(ctx, http.MethodGet, url, nil)
	if err != nil {
		t.Fatalf("build request: %v", err)
	}
	response, err := http.DefaultClient.Do(request)
	if err != nil {
		t.Fatalf("perform request: %v", err)
	}
	defer response.Body.Close()
	if response.StatusCode != http.StatusOK {
		t.Fatalf("GET %s returned %d", url, response.StatusCode)
	}
	if err := json.NewDecoder(response.Body).Decode(target); err != nil {
		t.Fatalf("decode response from %s: %v", url, err)
	}
}

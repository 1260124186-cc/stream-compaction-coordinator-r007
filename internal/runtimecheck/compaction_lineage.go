package runtimecheck

import (
	"bytes"
	"context"
	"net/http"
	"os"
	"path/filepath"
	"time"

	"github.com/1260124186-cc/solo-0017-stream-compaction/internal/application"
	"github.com/1260124186-cc/solo-0017-stream-compaction/internal/clock"
	"github.com/1260124186-cc/solo-0017-stream-compaction/internal/domain"
	"github.com/1260124186-cc/solo-0017-stream-compaction/internal/idgen"
	"github.com/1260124186-cc/solo-0017-stream-compaction/internal/observability"
	"github.com/1260124186-cc/solo-0017-stream-compaction/internal/storage"
)

// runCompactionLineage proves that only a snapshot with a complete compaction
// reference closure is allowed to serve. It builds a real completed compaction
// through the HTTP workflow, confirms the plan, result segment, and source
// segment read entry points after a restart, and then derives corrupted
// snapshots that must fail before any listener starts.
func runCompactionLineage(
	ctx context.Context,
	assertions *counterAssertions,
) error {
	h, err := newHarness()
	if err != nil {
		return err
	}
	defer h.close()

	streamID, sourceIDs, err := prepareTwoSealedSegments(ctx, h, "lineage-flow")
	if err != nil {
		return err
	}

	var planResult application.CompactionPlanResult
	if err := h.call(
		ctx,
		http.MethodPost,
		"/v1/streams/"+streamID+"/compaction-plans",
		map[string]any{
			"source_segment_ids":       sourceIDs,
			"expected_stream_revision": 5,
		},
		&planResult,
	); err != nil {
		return err
	}
	planID := planResult.Plan.ID

	if err := h.call(
		ctx,
		http.MethodPost,
		"/v1/compaction-plans/"+planID+"/start",
		map[string]any{},
		&planResult,
	); err != nil {
		return err
	}
	var finished application.CompactionPlanResult
	if err := h.call(
		ctx,
		http.MethodPost,
		"/v1/compaction-plans/"+planID+"/finish",
		map[string]any{
			"result_checksum":   "sha256:lineage",
			"result_size_bytes": 160,
		},
		&finished,
	); err != nil {
		return err
	}
	if err := assertions.require(
		finished.Plan.State == domain.PlanStateCompleted && finished.ResultSegment != nil,
		"lineage plan did not complete: %+v",
		finished,
	); err != nil {
		return err
	}
	resultID := finished.ResultSegment.ID

	// Seal one more segment that the completed plan never touched.
	for index, payload := range []int64{20, 20} {
		if err := h.call(
			ctx,
			http.MethodPost,
			"/v1/streams/"+streamID+"/records",
			map[string]any{
				"expected_revision": 6 + index,
				"record_count":      2,
				"payload_bytes":     payload,
			},
			&application.AppendRecordsResult{},
		); err != nil {
			return err
		}
	}
	var sealedList segmentList
	if err := h.call(
		ctx,
		http.MethodGet,
		"/v1/streams/"+streamID+"/segments?states=sealed&limit=100",
		nil,
		&sealedList,
	); err != nil {
		return err
	}
	if err := assertions.require(
		len(sealedList.Items) == 2 &&
			sealedList.Items[0].ID == resultID &&
			sealedList.Items[1].Range.BaseOffset == 80,
		"expected the result segment and one untouched sealed segment: %+v",
		sealedList.Items,
	); err != nil {
		return err
	}
	untouchedSealedID := sealedList.Items[1].ID

	// A second stream provides a foreign segment for the cross-stream case.
	var otherStream application.CreateStreamResult
	if err := h.call(
		ctx,
		http.MethodPost,
		"/v1/streams",
		map[string]any{
			"namespace":       "field-lab",
			"name":            "lineage-other",
			"threshold_bytes": 16,
		},
		&otherStream,
	); err != nil {
		return err
	}
	for index, payload := range []int64{8, 8} {
		if err := h.call(
			ctx,
			http.MethodPost,
			"/v1/streams/"+otherStream.Stream.ID+"/records",
			map[string]any{
				"expected_revision": index + 1,
				"record_count":      1,
				"payload_bytes":     payload,
			},
			&application.AppendRecordsResult{},
		); err != nil {
			return err
		}
	}
	otherSegments, err := listSegments(ctx, h, otherStream.Stream.ID)
	if err != nil {
		return err
	}
	if err := assertions.require(
		len(otherSegments) == 2 &&
			otherSegments[0].State == string(domain.SegmentStateSealed),
		"foreign stream did not produce a sealed segment: %+v",
		otherSegments,
	); err != nil {
		return err
	}
	foreignSegmentID := otherSegments[0].ID

	// The intact snapshot must recover and serve all three read entry points.
	if err := h.restart(); err != nil {
		return err
	}

	var planView domain.CompactionPlan
	if err := h.call(
		ctx,
		http.MethodGet,
		"/v1/compaction-plans/"+planID,
		nil,
		&planView,
	); err != nil {
		return err
	}
	if err := assertions.require(
		planView.State == domain.PlanStateCompleted &&
			planView.ResultSegmentID == resultID &&
			sameStrings(planView.SourceSegmentIDs, sourceIDs),
		"recovered plan does not match the completed compaction: %+v",
		planView,
	); err != nil {
		return err
	}

	var resultView domain.Segment
	if err := h.call(
		ctx,
		http.MethodGet,
		"/v1/segments/"+resultID,
		nil,
		&resultView,
	); err != nil {
		return err
	}
	if err := assertions.require(
		resultView.State == domain.SegmentStateSealed &&
			resultView.StreamID == streamID &&
			sameStrings(resultView.SourceSegmentIDs, sourceIDs),
		"recovered result segment lost its source closure: %+v",
		resultView,
	); err != nil {
		return err
	}

	for _, sourceID := range sourceIDs {
		var sourceView domain.Segment
		if err := h.call(
			ctx,
			http.MethodGet,
			"/v1/segments/"+sourceID,
			nil,
			&sourceView,
		); err != nil {
			return err
		}
		if err := assertions.require(
			sourceView.State == domain.SegmentStateRetired,
			"recovered source %s state is %s, want retired",
			sourceID,
			sourceView.State,
		); err != nil {
			return err
		}
	}

	baseline, err := h.repo.Load()
	if err != nil {
		return err
	}

	corruptions := []struct {
		name   string
		mutate func(*domain.State)
	}{
		{
			name: "missing-source",
			mutate: func(state *domain.State) {
				result := state.Segments[resultID]
				result.SourceSegmentIDs = append(result.SourceSegmentIDs, "seg_ghost")
			},
		},
		{
			name: "cross-stream-source",
			mutate: func(state *domain.State) {
				result := state.Segments[resultID]
				result.SourceSegmentIDs = append(result.SourceSegmentIDs, foreignSegmentID)
			},
		},
		{
			name: "inconsistent-state",
			mutate: func(state *domain.State) {
				state.Segments[resultID].SourceSegmentIDs = []string{
					sourceIDs[0],
					untouchedSealedID,
				}
			},
		},
		{
			name: "lineage-takeover",
			mutate: func(state *domain.State) {
				plantShadowCompaction(state, streamID, planID, resultID)
			},
		},
	}

	started := make([]string, 0)
	for _, item := range corruptions {
		corrupt := domain.CloneState(baseline)
		item.mutate(corrupt)
		data, err := storage.MarshalState(corrupt)
		if err != nil {
			return err
		}
		path := filepath.Join(h.root, item.name+".snapshot.json")
		if err := os.WriteFile(path, data, 0o600); err != nil {
			return err
		}
		repository, err := storage.NewFileRepository(path)
		if err != nil {
			return err
		}
		_, startErr := application.NewService(
			repository,
			&idgen.Counter{},
			clock.NewFixed(time.Date(2026, 1, 2, 0, 0, 0, 0, time.UTC), time.Second),
			observability.Discard{},
			application.Limits{
				MaxAppendRecords: 100,
				MaxAppendBytes:   1 << 20,
			},
		)
		if startErr == nil {
			started = append(started, item.name)
			continue
		}
		if err := assertions.require(
			domain.IsCode(startErr, domain.CodeInconsistentState),
			"snapshot %s failed with %v, want inconsistent_state",
			item.name,
			startErr,
		); err != nil {
			return err
		}
		after, err := os.ReadFile(path)
		if err != nil {
			return err
		}
		if err := assertions.require(
			bytes.Equal(after, data),
			"snapshot %s was modified by the failed startup",
			item.name,
		); err != nil {
			return err
		}
	}
	if err := assertions.require(
		len(started) == 0,
		"snapshots %v started serving a broken compaction closure",
		started,
	); err != nil {
		return err
	}
	return nil
}

// plantShadowCompaction adds a second completed plan whose result claims the
// same retired sources, so the lineage of the original result is ambiguous.
func plantShadowCompaction(
	state *domain.State,
	streamID string,
	planID string,
	resultID string,
) {
	plan := state.Plans[planID]
	result := state.Segments[resultID]
	timestamp := result.CreatedAt

	shadow := &domain.Segment{
		ID:               "seg_shadow",
		StreamID:         streamID,
		Generation:       result.Generation,
		Range:            result.Range,
		State:            domain.SegmentStateSealed,
		RecordCount:      result.RecordCount,
		SizeBytes:        result.SizeBytes,
		Checksum:         "sha256:shadow",
		SourceSegmentIDs: append([]string(nil), result.SourceSegmentIDs...),
		Durable:          true,
		ReplicaAcks:      map[string]domain.ReplicaAck{},
		CreatedAt:        timestamp,
		UpdatedAt:        timestamp,
		SealedAt:         &timestamp,
	}
	state.Segments[shadow.ID] = shadow

	generations := make(map[string]uint64, len(plan.SourceGenerations))
	for id, generation := range plan.SourceGenerations {
		generations[id] = generation
	}
	state.Plans["cmp_shadow"] = &domain.CompactionPlan{
		ID:                     "cmp_shadow",
		StreamID:               streamID,
		SourceSegmentIDs:       append([]string(nil), plan.SourceSegmentIDs...),
		SourceGenerations:      generations,
		Range:                  plan.Range,
		State:                  domain.PlanStateCompleted,
		ExpectedStreamRevision: plan.ExpectedStreamRevision,
		Revision:               plan.Revision,
		ResultSegmentID:        shadow.ID,
		ResultChecksum:         shadow.Checksum,
		ResultSizeBytes:        shadow.SizeBytes,
		CreatedAt:              timestamp,
		UpdatedAt:              timestamp,
		StartedAt:              &timestamp,
		CompletedAt:            &timestamp,
	}

	stream := state.Streams[streamID]
	stream.SealedSegmentIDs = append(stream.SealedSegmentIDs, shadow.ID)
}

func sameStrings(left, right []string) bool {
	if len(left) != len(right) {
		return false
	}
	members := make(map[string]struct{}, len(left))
	for _, value := range left {
		members[value] = struct{}{}
	}
	for _, value := range right {
		if _, exists := members[value]; !exists {
			return false
		}
	}
	return true
}

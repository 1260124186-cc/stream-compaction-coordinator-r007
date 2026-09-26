package domain

import (
	"fmt"
	"sort"
	"strings"
	"time"
)

func NewCompactionPlan(
	planID string,
	stream *Stream,
	segments map[string]*Segment,
	sourceSegmentIDs []string,
	expectedRevision uint64,
	now time.Time,
) (*CompactionPlan, error) {
	if stream == nil {
		return nil, NewError(CodeNotFound, "stream was not found")
	}
	if strings.TrimSpace(planID) == "" {
		return nil, NewError(CodeInvalidInput, "plan identifier is required")
	}
	if expectedRevision == 0 {
		return nil, NewError(CodeInvalidInput, "expected stream revision is required")
	}
	if expectedRevision != stream.Revision {
		return nil, NewError(CodeStaleRevision, "stream revision does not match").
			WithDetail("expected", expectedRevision).
			WithDetail("current", stream.Revision)
	}
	if len(sourceSegmentIDs) < 2 {
		return nil, NewError(CodeInvalidRange, "a compaction plan requires at least two source segments")
	}

	unique := make(map[string]struct{}, len(sourceSegmentIDs))
	ranges := make([]Range, 0, len(sourceSegmentIDs))
	generations := make(map[string]uint64, len(sourceSegmentIDs))
	for _, segmentID := range sourceSegmentIDs {
		if _, exists := unique[segmentID]; exists {
			return nil, NewError(CodeDuplicate, "source segment is repeated").
				WithDetail("segment_id", segmentID)
		}
		unique[segmentID] = struct{}{}

		segment, exists := segments[segmentID]
		if !exists || segment == nil {
			return nil, NewError(CodeNotFound, "source segment was not found").
				WithDetail("segment_id", segmentID)
		}
		if segment.StreamID != stream.ID {
			return nil, NewError(CodeInvalidRange, "source segment belongs to a different stream").
				WithDetail("segment_id", segmentID)
		}
		if segment.State != SegmentStateSealed {
			return nil, NewError(CodeInvalidState, "source segment must be sealed").
				WithDetail("segment_id", segmentID).
				WithDetail("state", segment.State)
		}
		if len(segment.SourceSegmentIDs) > 0 {
			return nil, NewError(CodeInvalidState, "compacted segments cannot be compacted again").
				WithDetail("segment_id", segmentID)
		}
		ranges = append(ranges, segment.Range)
		generations[segmentID] = segment.Generation
	}

	union, err := ValidateContiguousRanges(ranges)
	if err != nil {
		return nil, err
	}
	if union.EndOffset <= stream.CompactionFloor {
		return nil, NewError(CodeInvalidRange, "plan does not extend beyond the compaction floor")
	}

	timestamp := now.UTC()
	return &CompactionPlan{
		ID:                     planID,
		StreamID:               stream.ID,
		SourceSegmentIDs:       append([]string(nil), sourceSegmentIDs...),
		SourceGenerations:      generations,
		Range:                  union,
		State:                  PlanStateProposed,
		ExpectedStreamRevision: expectedRevision,
		Revision:               1,
		CreatedAt:              timestamp,
		UpdatedAt:              timestamp,
	}, nil
}

func StartCompactionPlan(
	plan *CompactionPlan,
	stream *Stream,
	segments map[string]*Segment,
	now time.Time,
) error {
	if plan == nil {
		return NewError(CodeNotFound, "compaction plan was not found")
	}
	if stream == nil {
		return NewError(CodeNotFound, "stream was not found")
	}
	if plan.State != PlanStateProposed {
		return NewError(CodeInvalidState, "only a proposed plan can be started").
			WithDetail("state", plan.State)
	}
	if stream.Revision != plan.ExpectedStreamRevision {
		return NewError(CodeStaleRevision, "stream changed after the plan was created").
			WithDetail("expected", plan.ExpectedStreamRevision).
			WithDetail("current", stream.Revision)
	}

	for _, segmentID := range plan.SourceSegmentIDs {
		segment, exists := segments[segmentID]
		if !exists || segment == nil {
			return NewError(CodeNotFound, "source segment was not found").
				WithDetail("segment_id", segmentID)
		}
		if segment.Generation != plan.SourceGenerations[segmentID] {
			return NewError(CodeConflict, "source segment generation changed").
				WithDetail("segment_id", segmentID)
		}
		if segment.State != SegmentStateSealed {
			return NewError(CodeInvalidState, "source segment is no longer sealed").
				WithDetail("segment_id", segmentID).
				WithDetail("state", segment.State)
		}
	}

	timestamp := now.UTC()
	for _, segmentID := range plan.SourceSegmentIDs {
		segment := segments[segmentID]
		segment.State = SegmentStateCompacting
		segment.UpdatedAt = timestamp
	}
	plan.State = PlanStateRunning
	plan.Revision++
	plan.StartedAt = &timestamp
	plan.UpdatedAt = timestamp
	return nil
}

type FinishCompactionInput struct {
	ResultChecksum  string
	ResultSizeBytes int64
}

func FinishCompactionPlan(
	plan *CompactionPlan,
	stream *Stream,
	segments map[string]*Segment,
	resultSegmentID string,
	input FinishCompactionInput,
	now time.Time,
) (*Segment, error) {
	if plan == nil {
		return nil, NewError(CodeNotFound, "compaction plan was not found")
	}
	if stream == nil {
		return nil, NewError(CodeNotFound, "stream was not found")
	}
	if plan.StreamID != stream.ID {
		return nil, NewError(CodeConflict, "plan and stream do not match")
	}
	if plan.State == PlanStateCompleted {
		if plan.ResultChecksum != input.ResultChecksum ||
			plan.ResultSizeBytes != input.ResultSizeBytes {
			return nil, NewError(CodeConflict, "completed plan has a different result")
		}
		segment, exists := segments[plan.ResultSegmentID]
		if !exists {
			return nil, NewError(CodeInconsistentState, "completed plan result segment is missing")
		}
		return segment, nil
	}
	if plan.State != PlanStateRunning {
		return nil, NewError(CodeInvalidState, "only a running plan can be finished").
			WithDetail("state", plan.State)
	}
	if strings.TrimSpace(resultSegmentID) == "" {
		return nil, NewError(CodeInvalidInput, "result segment identifier is required")
	}
	if _, exists := segments[resultSegmentID]; exists {
		return nil, NewError(CodeConflict, "result segment identifier is already in use")
	}
	if strings.TrimSpace(input.ResultChecksum) == "" {
		return nil, NewError(CodeInvalidInput, "result checksum is required")
	}
	if input.ResultSizeBytes <= 0 {
		return nil, NewError(CodeInvalidInput, "result byte count must be positive")
	}

	var highestGeneration uint64
	var sourceRecordCount uint64
	for _, segmentID := range plan.SourceSegmentIDs {
		segment, exists := segments[segmentID]
		if !exists || segment == nil {
			return nil, NewError(CodeInconsistentState, "source segment disappeared").
				WithDetail("segment_id", segmentID)
		}
		if segment.State != SegmentStateCompacting {
			return nil, NewError(CodeInvalidState, "source segment is not compacting").
				WithDetail("segment_id", segmentID).
				WithDetail("state", segment.State)
		}
		if segment.Generation != plan.SourceGenerations[segmentID] {
			return nil, NewError(CodeConflict, "source segment generation changed").
				WithDetail("segment_id", segmentID)
		}
		if segment.Generation > highestGeneration {
			highestGeneration = segment.Generation
		}
		recordCount, err := AddUint64Safely(sourceRecordCount, segment.RecordCount)
		if err != nil {
			return nil, err
		}
		sourceRecordCount = recordCount
	}

	timestamp := now.UTC()
	result := &Segment{
		ID:               resultSegmentID,
		StreamID:         stream.ID,
		Generation:       highestGeneration + 1,
		Range:            plan.Range,
		State:            SegmentStateSealed,
		RecordCount:      sourceRecordCount,
		SizeBytes:        input.ResultSizeBytes,
		Checksum:         input.ResultChecksum,
		SourceSegmentIDs: append([]string(nil), plan.SourceSegmentIDs...),
		Durable:          true,
		ReplicaAcks:      map[string]ReplicaAck{},
		CreatedAt:        timestamp,
		UpdatedAt:        timestamp,
		SealedAt:         &timestamp,
	}

	for _, segmentID := range plan.SourceSegmentIDs {
		segment := segments[segmentID]
		segment.State = SegmentStateRetired
		segment.UpdatedAt = timestamp
		segment.RetiredAt = &timestamp
	}

	stream.SealedSegmentIDs = removeIdentifiers(
		stream.SealedSegmentIDs,
		plan.SourceSegmentIDs,
	)
	stream.SealedSegmentIDs = append(stream.SealedSegmentIDs, result.ID)
	if plan.Range.EndOffset > stream.CompactionFloor {
		stream.CompactionFloor = plan.Range.EndOffset
	}
	stream.Revision++
	stream.UpdatedAt = timestamp

	live := make([]*Segment, 0, len(segments)+1)
	for _, segment := range segments {
		if segment.StreamID == stream.ID {
			live = append(live, segment)
		}
	}
	live = append(live, result)
	RecomputeStreamWatermark(stream, live)

	plan.State = PlanStateCompleted
	plan.Revision++
	plan.ResultSegmentID = result.ID
	plan.ResultChecksum = input.ResultChecksum
	plan.ResultSizeBytes = input.ResultSizeBytes
	plan.CompletedAt = &timestamp
	plan.UpdatedAt = timestamp
	segments[result.ID] = result
	return result, nil
}

func AbortRunningPlan(
	plan *CompactionPlan,
	segments map[string]*Segment,
	now time.Time,
) error {
	if plan == nil {
		return NewError(CodeNotFound, "compaction plan was not found")
	}
	if plan.State != PlanStateRunning {
		return NewError(CodeInvalidState, "only a running plan can be aborted")
	}
	timestamp := now.UTC()
	for _, segmentID := range plan.SourceSegmentIDs {
		segment, exists := segments[segmentID]
		if !exists || segment == nil {
			return NewError(CodeInconsistentState, "source segment disappeared during recovery").
				WithDetail("segment_id", segmentID)
		}
		if segment.State == SegmentStateCompacting {
			segment.State = SegmentStateSealed
			segment.UpdatedAt = timestamp
			segment.SealedAt = &timestamp
		}
	}
	plan.State = PlanStateAborted
	plan.Revision++
	plan.UpdatedAt = timestamp
	plan.CompletedAt = &timestamp
	return nil
}

func SortedSourceIDs(segments map[string]*Segment) []string {
	ids := make([]string, 0, len(segments))
	for id := range segments {
		ids = append(ids, id)
	}
	sort.Strings(ids)
	return ids
}

func removeIdentifiers(values []string, removed []string) []string {
	removeSet := make(map[string]struct{}, len(removed))
	for _, value := range removed {
		removeSet[value] = struct{}{}
	}
	result := make([]string, 0, len(values))
	for _, value := range values {
		if _, exists := removeSet[value]; exists {
			continue
		}
		result = append(result, value)
	}
	return result
}

func (p *CompactionPlan) String() string {
	if p == nil {
		return "<nil>"
	}
	return fmt.Sprintf("%s:%s", p.ID, p.State)
}

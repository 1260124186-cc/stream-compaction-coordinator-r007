package domain

import (
	"fmt"
	"sort"
)

// ValidateState checks all cross-entity references and semantic invariants. It
// is intentionally called before and after durable mutations so partial
// application logic cannot commit a mixed state.
func ValidateState(state *State) error {
	if state == nil {
		return NewError(CodeInconsistentState, "state is nil")
	}
	if state.SchemaVersion != SnapshotSchemaVersion {
		return NewError(CodeInconsistentState, "snapshot schema version is unsupported").
			WithDetail("schema_version", state.SchemaVersion)
	}
	if state.Streams == nil || state.Segments == nil || state.Plans == nil {
		return NewError(CodeInconsistentState, "state maps must not be nil")
	}

	for id, stream := range state.Streams {
		if stream == nil {
			return NewError(CodeInconsistentState, "stream value is nil").
				WithDetail("stream_id", id)
		}
		if stream.ID != id {
			return NewError(CodeInconsistentState, "stream map key does not match identifier").
				WithDetail("key", id).
				WithDetail("stream_id", stream.ID)
		}
		if err := validateStream(stream, state); err != nil {
			return err
		}
	}

	for id, segment := range state.Segments {
		if segment == nil {
			return NewError(CodeInconsistentState, "segment value is nil").
				WithDetail("segment_id", id)
		}
		if segment.ID != id {
			return NewError(CodeInconsistentState, "segment map key does not match identifier").
				WithDetail("key", id).
				WithDetail("segment_id", segment.ID)
		}
		if err := validateSegment(segment, state.Streams); err != nil {
			return err
		}
	}

	for id, plan := range state.Plans {
		if plan == nil {
			return NewError(CodeInconsistentState, "plan value is nil").
				WithDetail("plan_id", id)
		}
		if plan.ID != id {
			return NewError(CodeInconsistentState, "plan map key does not match identifier").
				WithDetail("key", id).
				WithDetail("plan_id", plan.ID)
		}
		if err := validatePlan(plan, state); err != nil {
			return err
		}
	}

	if err := validateCompactionLineage(state); err != nil {
		return err
	}

	if err := validateAudit(state.Audit, state.AuditSequence); err != nil {
		return err
	}
	return nil
}

func validateStream(stream *Stream, state *State) error {
	if stream.ID == "" {
		return NewError(CodeInconsistentState, "stream identifier is empty")
	}
	if stream.Namespace == "" || stream.Name == "" {
		return NewError(CodeInconsistentState, "stream namespace and name are required").
			WithDetail("stream_id", stream.ID)
	}
	if stream.SegmentThresholdBytes <= 0 {
		return NewError(CodeInconsistentState, "stream threshold must be positive").
			WithDetail("stream_id", stream.ID)
	}
	if stream.Revision == 0 {
		return NewError(CodeInconsistentState, "stream revision must be positive").
			WithDetail("stream_id", stream.ID)
	}
	active, exists := state.Segments[stream.ActiveSegmentID]
	if !exists || active == nil {
		return NewError(CodeInconsistentState, "active segment does not exist").
			WithDetail("stream_id", stream.ID).
			WithDetail("segment_id", stream.ActiveSegmentID)
	}
	if active.StreamID != stream.ID || active.State != SegmentStateOpen {
		return NewError(CodeInconsistentState, "active segment is not an open segment of the stream").
			WithDetail("stream_id", stream.ID).
			WithDetail("segment_id", active.ID)
	}

	seen := make(map[string]struct{}, len(stream.SealedSegmentIDs))
	for _, segmentID := range stream.SealedSegmentIDs {
		if _, exists := seen[segmentID]; exists {
			return NewError(CodeInconsistentState, "stream contains a duplicate sealed segment").
				WithDetail("stream_id", stream.ID).
				WithDetail("segment_id", segmentID)
		}
		seen[segmentID] = struct{}{}
		if segmentID == stream.ActiveSegmentID {
			return NewError(CodeInconsistentState, "active segment cannot appear in the sealed list").
				WithDetail("stream_id", stream.ID)
		}
		segment, exists := state.Segments[segmentID]
		if !exists || segment == nil {
			return NewError(CodeInconsistentState, "sealed segment does not exist").
				WithDetail("stream_id", stream.ID).
				WithDetail("segment_id", segmentID)
		}
		if segment.StreamID != stream.ID {
			return NewError(CodeInconsistentState, "sealed segment belongs to another stream").
				WithDetail("stream_id", stream.ID).
				WithDetail("segment_id", segmentID)
		}
		switch segment.State {
		case SegmentStateSealed, SegmentStateCompacting:
		default:
			return NewError(CodeInconsistentState, "sealed list contains a segment in the wrong state").
				WithDetail("segment_id", segmentID).
				WithDetail("state", segment.State)
		}
	}
	for segmentID, segment := range state.Segments {
		if segment.StreamID != stream.ID ||
			(segment.State != SegmentStateSealed && segment.State != SegmentStateCompacting) {
			continue
		}
		if _, exists := seen[segmentID]; !exists {
			return NewError(CodeInconsistentState, "live sealed segment is missing from the stream list").
				WithDetail("stream_id", stream.ID).
				WithDetail("segment_id", segmentID)
		}
	}

	if stream.CompactionFloor < 0 || stream.Watermark < 0 {
		return NewError(CodeInconsistentState, "stream floors must not be negative").
			WithDetail("stream_id", stream.ID)
	}
	live := make([]*Segment, 0)
	for _, segment := range state.Segments {
		if segment.StreamID == stream.ID {
			live = append(live, segment)
		}
	}
	expectedWatermark := reducedWatermark(stream.ID, live)
	if stream.Watermark != expectedWatermark {
		return NewError(CodeInconsistentState, "stream watermark does not match live segment boundaries").
			WithDetail("stream_id", stream.ID).
			WithDetail("watermark", stream.Watermark).
			WithDetail("expected", expectedWatermark)
	}
	return nil
}

func reducedWatermark(streamID string, segments []*Segment) int64 {
	watermark := int64(0)
	initialized := false
	for _, segment := range segments {
		if segment == nil || segment.StreamID != streamID {
			continue
		}
		if segment.State == SegmentStateOpen || segment.State == SegmentStateRetired {
			continue
		}
		boundary := segment.Range.BaseOffset
		if segment.Durable {
			boundary = segment.Range.EndOffset
		}
		if !initialized || boundary < watermark {
			watermark = boundary
			initialized = true
		}
	}
	return watermark
}

func validateSegment(segment *Segment, streams map[string]*Stream) error {
	if segment.ID == "" {
		return NewError(CodeInconsistentState, "segment identifier is empty")
	}
	stream, exists := streams[segment.StreamID]
	if !exists || stream == nil {
		return NewError(CodeInconsistentState, "segment references a missing stream").
			WithDetail("segment_id", segment.ID).
			WithDetail("stream_id", segment.StreamID)
	}
	if segment.Generation == 0 {
		return NewError(CodeInconsistentState, "segment generation must be positive").
			WithDetail("segment_id", segment.ID)
	}
	if err := segment.Range.Validate(); err != nil {
		return WrapError(CodeInconsistentState, "segment range is invalid", err).
			WithDetail("segment_id", segment.ID)
	}
	if segment.SizeBytes < 0 || segment.RecordCount < 0 {
		return NewError(CodeInconsistentState, "segment counters must not be negative").
			WithDetail("segment_id", segment.ID)
	}
	if segment.ReplicaAcks == nil {
		return NewError(CodeInconsistentState, "segment acknowledgement map must not be nil").
			WithDetail("segment_id", segment.ID)
	}
	for nodeID, ack := range segment.ReplicaAcks {
		if ack.NodeID != nodeID || ack.NodeID == "" {
			return NewError(CodeInconsistentState, "replica acknowledgement identity is inconsistent").
				WithDetail("segment_id", segment.ID)
		}
		if ack.Generation != segment.Generation {
			return NewError(CodeInconsistentState, "replica acknowledgement generation is inconsistent").
				WithDetail("segment_id", segment.ID).
				WithDetail("node_id", nodeID)
		}
		if !segment.Range.ContainsOffset(ack.Offset) {
			return NewError(CodeInconsistentState, "replica acknowledgement is outside segment range").
				WithDetail("segment_id", segment.ID).
				WithDetail("node_id", nodeID)
		}
	}

	switch segment.State {
	case SegmentStateOpen:
		if segment.SealedAt != nil || segment.RetiredAt != nil {
			return NewError(CodeInconsistentState, "open segment has a terminal timestamp").
				WithDetail("segment_id", segment.ID)
		}
		if stream.ActiveSegmentID != segment.ID {
			return NewError(CodeInconsistentState, "open segment is not the active segment").
				WithDetail("segment_id", segment.ID)
		}
	case SegmentStateSealed:
		if segment.SealedAt == nil {
			return NewError(CodeInconsistentState, "sealed segment lacks a seal timestamp").
				WithDetail("segment_id", segment.ID)
		}
		if segment.RetiredAt != nil {
			return NewError(CodeInconsistentState, "sealed segment has a retirement timestamp").
				WithDetail("segment_id", segment.ID)
		}
	case SegmentStateCompacting:
		if segment.SealedAt == nil {
			return NewError(CodeInconsistentState, "compacting segment lacks a seal timestamp").
				WithDetail("segment_id", segment.ID)
		}
	case SegmentStateRetired:
		if segment.RetiredAt == nil {
			return NewError(CodeInconsistentState, "retired segment lacks a retirement timestamp").
				WithDetail("segment_id", segment.ID)
		}
	default:
		return NewError(CodeInconsistentState, "segment state is not recognized").
			WithDetail("segment_id", segment.ID).
			WithDetail("state", segment.State)
	}

	for _, sourceID := range segment.SourceSegmentIDs {
		if sourceID == segment.ID {
			return NewError(CodeInconsistentState, "compacted segment references itself").
				WithDetail("segment_id", segment.ID)
		}
	}
	return nil
}

func validatePlan(plan *CompactionPlan, state *State) error {
	if plan.ID == "" || plan.StreamID == "" {
		return NewError(CodeInconsistentState, "plan identity is incomplete")
	}
	if len(plan.SourceSegmentIDs) < 2 {
		return NewError(CodeInconsistentState, "plan has too few source segments").
			WithDetail("plan_id", plan.ID)
	}
	if plan.Revision == 0 {
		return NewError(CodeInconsistentState, "plan revision must be positive").
			WithDetail("plan_id", plan.ID)
	}
	if err := plan.Range.Validate(); err != nil {
		return WrapError(CodeInconsistentState, "plan range is invalid", err).
			WithDetail("plan_id", plan.ID)
	}
	seenSources := make(map[string]struct{}, len(plan.SourceSegmentIDs))
	for _, segmentID := range plan.SourceSegmentIDs {
		if _, seen := seenSources[segmentID]; seen {
			return NewError(CodeInconsistentState, "plan contains a duplicate source segment").
				WithDetail("plan_id", plan.ID).
				WithDetail("segment_id", segmentID)
		}
		seenSources[segmentID] = struct{}{}

		segment, exists := state.Segments[segmentID]
		if !exists || segment == nil {
			return NewError(CodeInconsistentState, "plan source segment is missing").
				WithDetail("plan_id", plan.ID).
				WithDetail("segment_id", segmentID)
		}
		if segment.StreamID != plan.StreamID {
			return NewError(CodeInconsistentState, "plan source belongs to another stream").
				WithDetail("plan_id", plan.ID).
				WithDetail("segment_id", segmentID)
		}
		if segment.Generation != plan.SourceGenerations[segmentID] {
			return NewError(CodeInconsistentState, "plan source generation changed").
				WithDetail("plan_id", plan.ID).
				WithDetail("segment_id", segmentID)
		}
		if len(segment.SourceSegmentIDs) > 0 {
			return NewError(CodeInconsistentState, "plan source is already a compacted result").
				WithDetail("plan_id", plan.ID).
				WithDetail("segment_id", segmentID)
		}
	}

	switch plan.State {
	case PlanStateProposed:
		if plan.ResultSegmentID != "" {
			return NewError(CodeInconsistentState, "proposed plan already has a result").
				WithDetail("plan_id", plan.ID)
		}
	case PlanStateRunning:
		if plan.StartedAt == nil {
			return NewError(CodeInconsistentState, "running plan lacks a start timestamp").
				WithDetail("plan_id", plan.ID)
		}
		for _, segmentID := range plan.SourceSegmentIDs {
			if state.Segments[segmentID].State != SegmentStateCompacting {
				return NewError(CodeInconsistentState, "running plan source is not compacting").
					WithDetail("plan_id", plan.ID).
					WithDetail("segment_id", segmentID)
			}
		}
	case PlanStateCompleted:
		if plan.ResultChecksum == "" || plan.ResultSizeBytes <= 0 {
			return NewError(CodeInconsistentState, "completed plan lacks result metadata").
				WithDetail("plan_id", plan.ID)
		}
		result, exists := state.Segments[plan.ResultSegmentID]
		if !exists || result == nil {
			return NewError(CodeInconsistentState, "completed plan result is missing").
				WithDetail("plan_id", plan.ID)
		}
		if result.StreamID != plan.StreamID {
			return NewError(CodeInconsistentState, "completed plan result belongs to another stream").
				WithDetail("plan_id", plan.ID).
				WithDetail("segment_id", result.ID)
		}
		if result.State != SegmentStateSealed {
			return NewError(CodeInconsistentState, "completed plan result is not a live sealed segment").
				WithDetail("plan_id", plan.ID).
				WithDetail("segment_id", result.ID).
				WithDetail("state", result.State)
		}
		if !result.Durable {
			return NewError(CodeInconsistentState, "completed plan result is not durable").
				WithDetail("plan_id", plan.ID).
				WithDetail("segment_id", result.ID)
		}
		if result.Range != plan.Range || result.Checksum != plan.ResultChecksum ||
			result.SizeBytes != plan.ResultSizeBytes {
			return NewError(CodeInconsistentState, "completed plan result does not match the plan").
				WithDetail("plan_id", plan.ID)
		}
		if !sameStringSequence(result.SourceSegmentIDs, plan.SourceSegmentIDs) {
			return NewError(CodeInconsistentState, "completed plan result sources do not match plan sources").
				WithDetail("plan_id", plan.ID).
				WithDetail("result_segment_id", result.ID)
		}
		sourceRanges := make([]Range, 0, len(plan.SourceSegmentIDs))
		for _, segmentID := range plan.SourceSegmentIDs {
			source := state.Segments[segmentID]
			if source.StreamID != plan.StreamID || source.State != SegmentStateRetired {
				return NewError(CodeInconsistentState, "completed plan source is not a retired segment of the stream").
					WithDetail("plan_id", plan.ID).
					WithDetail("segment_id", segmentID)
			}
			sourceRanges = append(sourceRanges, source.Range)
		}
		if union, err := ValidateContiguousRanges(sourceRanges); err != nil || union != plan.Range {
			return NewError(CodeInconsistentState, "completed plan source ranges do not match the result range").
				WithDetail("plan_id", plan.ID)
		}
	case PlanStateAborted:
		for _, segmentID := range plan.SourceSegmentIDs {
			if state.Segments[segmentID].State != SegmentStateSealed {
				return NewError(CodeInconsistentState, "aborted plan source is not sealed").
					WithDetail("plan_id", plan.ID).
					WithDetail("segment_id", segmentID)
			}
		}
	default:
		return NewError(CodeInconsistentState, "plan state is not recognized").
			WithDetail("plan_id", plan.ID).
			WithDetail("state", plan.State)
	}
	return nil
}

// validateCompactionLineage verifies the bidirectional closure between a
// completed plan, its live result segment, and the exact raw sources it
// retired. Segments are intentionally kept after retirement so every compacted
// byte range remains traceable to the same stream and one owning plan.
func validateCompactionLineage(state *State) error {
	owners := make(map[string]string)
	completedByResult := make(map[string]string)
	for planID, plan := range state.Plans {
		if plan.State != PlanStateCompleted {
			continue
		}
		if owner, exists := completedByResult[plan.ResultSegmentID]; exists {
			return NewError(CodeInconsistentState, "compaction result belongs to multiple completed plans").
				WithDetail("segment_id", plan.ResultSegmentID).
				WithDetail("plan_id", planID).
				WithDetail("other_plan_id", owner)
		}
		completedByResult[plan.ResultSegmentID] = planID

		for _, sourceID := range plan.SourceSegmentIDs {
			if owner, exists := owners[sourceID]; exists {
				return NewError(CodeInconsistentState, "source segment was retired by another compaction plan").
					WithDetail("segment_id", sourceID).
					WithDetail("plan_id", planID).
					WithDetail("other_plan_id", owner)
			}
			owners[sourceID] = planID
		}
	}

	for segmentID, segment := range state.Segments {
		if segment.State == SegmentStateRetired {
			if owner, exists := owners[segmentID]; !exists {
				return NewError(CodeInconsistentState, "retired segment has no completed compaction plan").
					WithDetail("segment_id", segmentID)
			} else if state.Plans[owner].StreamID != segment.StreamID {
				return NewError(CodeInconsistentState, "retired segment was taken over by a plan from another stream").
					WithDetail("segment_id", segmentID).
					WithDetail("plan_id", owner)
			}
		}
		if len(segment.SourceSegmentIDs) == 0 {
			continue
		}

		planID, exists := completedByResult[segmentID]
		if !exists {
			return NewError(CodeInconsistentState, "compacted result is not attached to a completed plan").
				WithDetail("segment_id", segmentID)
		}
		plan := state.Plans[planID]
		if plan.StreamID != segment.StreamID {
			return NewError(CodeInconsistentState, "compacted result belongs to a plan in another stream").
				WithDetail("segment_id", segmentID).
				WithDetail("plan_id", planID)
		}
		if !sameStringSequence(segment.SourceSegmentIDs, plan.SourceSegmentIDs) {
			return NewError(CodeInconsistentState, "compacted result lineage does not match its plan").
				WithDetail("segment_id", segmentID).
				WithDetail("plan_id", planID)
		}
		for _, sourceID := range segment.SourceSegmentIDs {
			source := state.Segments[sourceID]
			if source == nil {
				return NewError(CodeInconsistentState, "compacted result source is missing").
					WithDetail("segment_id", segmentID).
					WithDetail("source_segment_id", sourceID)
			}
			if source.StreamID != segment.StreamID {
				return NewError(CodeInconsistentState, "compacted result source belongs to another stream").
					WithDetail("segment_id", segmentID).
					WithDetail("source_segment_id", sourceID)
			}
			if owner := owners[sourceID]; owner != planID {
				return NewError(CodeInconsistentState, "compacted result source was retired by another plan").
					WithDetail("segment_id", segmentID).
					WithDetail("source_segment_id", sourceID).
					WithDetail("plan_id", planID).
					WithDetail("other_plan_id", owner)
			}
		}
	}
	return nil
}

func sameStringSequence(left, right []string) bool {
	if len(left) != len(right) {
		return false
	}
	for index := range left {
		if left[index] != right[index] {
			return false
		}
	}
	return true
}

func validateAudit(events []AuditEvent, sequence uint64) error {
	var previous uint64
	for index, event := range events {
		if event.Sequence == 0 || event.Sequence <= previous {
			return NewError(CodeInconsistentState, "audit sequence is not increasing").
				WithDetail("index", index).
				WithDetail("sequence", event.Sequence)
		}
		if event.Action == "" {
			return NewError(CodeInconsistentState, "audit action is empty").
				WithDetail("sequence", event.Sequence)
		}
		previous = event.Sequence
	}
	if len(events) == 0 && sequence != 0 {
		return NewError(CodeInconsistentState, "empty audit log has a non-zero sequence")
	}
	if len(events) > 0 && events[len(events)-1].Sequence != sequence {
		return NewError(CodeInconsistentState, "audit sequence does not match the last event").
			WithDetail("sequence", sequence).
			WithDetail("last", events[len(events)-1].Sequence)
	}
	return nil
}

func FindRunningPlanIDs(state *State) []string {
	ids := make([]string, 0)
	for id, plan := range state.Plans {
		if plan != nil && plan.State == PlanStateRunning {
			ids = append(ids, id)
		}
	}
	sort.Strings(ids)
	return ids
}

func SegmentRangeSummary(segments []*Segment) string {
	ranges := make([]string, 0, len(segments))
	for _, segment := range segments {
		if segment == nil {
			continue
		}
		ranges = append(ranges, fmt.Sprintf(
			"%s[%d,%d):%s",
			segment.ID,
			segment.Range.BaseOffset,
			segment.Range.EndOffset,
			segment.State,
		))
	}
	sort.Strings(ranges)
	return fmt.Sprint(ranges)
}

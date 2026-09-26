package domain

// validateCompactionLineage verifies that every compacted segment has a
// complete and unambiguous reference closure. A compacted segment is only
// servable when every source resolves, belongs to the same stream, was
// retired by exactly one completed plan, and is not claimed by any other
// completed plan. Snapshots that cannot prove this closure are rejected so
// callers never consume a result whose original inputs cannot be traced and
// later plans cannot amplify the uncertainty.
func validateCompactionLineage(state *State) error {
	completedPlans := make([]*CompactionPlan, 0)
	for _, plan := range state.Plans {
		if plan != nil && plan.State == PlanStateCompleted {
			completedPlans = append(completedPlans, plan)
		}
	}
	claimsByResult := make(map[string][]*CompactionPlan)
	claimsBySource := make(map[string][]*CompactionPlan)
	for _, plan := range completedPlans {
		claimsByResult[plan.ResultSegmentID] = append(claimsByResult[plan.ResultSegmentID], plan)
		for _, sourceID := range plan.SourceSegmentIDs {
			claimsBySource[sourceID] = append(claimsBySource[sourceID], plan)
		}
	}

	for _, segment := range state.Segments {
		if segment == nil || len(segment.SourceSegmentIDs) == 0 {
			continue
		}
		if err := validateCompactedSegment(segment, state, claimsByResult, claimsBySource); err != nil {
			return err
		}
	}

	for _, plan := range completedPlans {
		result := state.Segments[plan.ResultSegmentID]
		if result == nil {
			// validatePlan already reports the missing result segment.
			continue
		}
		if !sameIdentifierSet(result.SourceSegmentIDs, plan.SourceSegmentIDs) {
			return NewError(CodeInconsistentState, "completed plan result does not record the plan sources").
				WithDetail("plan_id", plan.ID).
				WithDetail("segment_id", result.ID)
		}
	}
	return nil
}

func validateCompactedSegment(
	segment *Segment,
	state *State,
	claimsByResult map[string][]*CompactionPlan,
	claimsBySource map[string][]*CompactionPlan,
) error {
	if segment.State == SegmentStateOpen {
		return NewError(CodeInconsistentState, "open segment cannot carry compaction sources").
			WithDetail("segment_id", segment.ID)
	}

	seen := make(map[string]struct{}, len(segment.SourceSegmentIDs))
	for _, sourceID := range segment.SourceSegmentIDs {
		if _, exists := seen[sourceID]; exists {
			return NewError(CodeInconsistentState, "compacted segment repeats a source segment").
				WithDetail("segment_id", segment.ID).
				WithDetail("source_segment_id", sourceID)
		}
		seen[sourceID] = struct{}{}
		source := state.Segments[sourceID]
		if source == nil {
			return NewError(CodeInconsistentState, "compacted segment source is missing").
				WithDetail("segment_id", segment.ID).
				WithDetail("source_segment_id", sourceID)
		}
		if source.StreamID != segment.StreamID {
			return NewError(CodeInconsistentState, "compacted segment source belongs to another stream").
				WithDetail("segment_id", segment.ID).
				WithDetail("source_segment_id", sourceID).
				WithDetail("source_stream_id", source.StreamID)
		}
		if source.State != SegmentStateRetired {
			return NewError(CodeInconsistentState, "compacted segment source was not retired").
				WithDetail("segment_id", segment.ID).
				WithDetail("source_segment_id", sourceID).
				WithDetail("state", source.State)
		}
	}

	claims := claimsByResult[segment.ID]
	if len(claims) == 0 {
		return NewError(CodeInconsistentState, "compacted segment has no completed plan record").
			WithDetail("segment_id", segment.ID)
	}
	if len(claims) > 1 {
		return NewError(CodeInconsistentState, "compacted segment is claimed by multiple completed plans").
			WithDetail("segment_id", segment.ID)
	}
	plan := claims[0]
	if plan.StreamID != segment.StreamID {
		return NewError(CodeInconsistentState, "compacted segment and its plan belong to different streams").
			WithDetail("segment_id", segment.ID).
			WithDetail("plan_id", plan.ID)
	}
	if !sameIdentifierSet(plan.SourceSegmentIDs, segment.SourceSegmentIDs) {
		return NewError(CodeInconsistentState, "compacted segment sources do not match the plan record").
			WithDetail("segment_id", segment.ID).
			WithDetail("plan_id", plan.ID)
	}
	for _, sourceID := range segment.SourceSegmentIDs {
		for _, other := range claimsBySource[sourceID] {
			if other.ID == plan.ID {
				continue
			}
			return NewError(CodeInconsistentState, "compacted segment lineage is claimed by another completed plan").
				WithDetail("segment_id", segment.ID).
				WithDetail("source_segment_id", sourceID).
				WithDetail("plan_id", other.ID)
		}
	}
	return nil
}

func sameIdentifierSet(left, right []string) bool {
	members := make(map[string]struct{}, len(left))
	for _, value := range left {
		members[value] = struct{}{}
	}
	if len(members) != len(right) {
		return false
	}
	for _, value := range right {
		if _, exists := members[value]; !exists {
			return false
		}
	}
	return true
}

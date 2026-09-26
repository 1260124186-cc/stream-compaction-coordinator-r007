package runtimecheck

import (
	"context"
	"fmt"
)

type Result struct {
	Workflow   string `json:"workflow"`
	Assertions int    `json:"assertions"`
}

type checkFunc func(context.Context, *counterAssertions) error

var checks = map[string]checkFunc{
	"append-rollover":     runAppendRollover,
	"compaction-lineage":  runCompactionLineage,
	"compaction-recovery": runCompactionRecovery,
	"replica-watermark":   runReplicaWatermark,
}

func Run(ctx context.Context, workflow string) (Result, error) {
	check, exists := checks[workflow]
	if !exists {
		return Result{}, fmt.Errorf("unknown workflow check %q", workflow)
	}
	counter := &counterAssertions{}
	if err := check(ctx, counter); err != nil {
		return Result{}, fmt.Errorf("%s: %w", workflow, err)
	}
	return Result{
		Workflow:   workflow,
		Assertions: counter.value,
	}, nil
}

type counterAssertions struct {
	value int
}

func (c *counterAssertions) require(condition bool, message string, args ...any) error {
	if condition {
		c.value++
		return nil
	}
	return fmt.Errorf(message, args...)
}

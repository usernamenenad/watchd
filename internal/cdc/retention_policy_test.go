package cdc

import (
	"context"
	"errors"
	"testing"

	"github.com/usernamenenad/watchd/internal/telemetry"
	"github.com/usernamenenad/watchd/internal/telemetry/telemetrytest"
)

func TestNextRetentionState(t *testing.T) {
	const budget = 1000
	for _, test := range []struct {
		name     string
		current  retentionState
		retained int64
		want     retentionState
	}{
		{"ok stays ok", retentionOK, 100, retentionOK},
		{"rises to warn at the threshold", retentionOK, 500, retentionWarn},
		{"rises straight to degrade", retentionOK, 850, retentionDegrade},
		{"rises straight to terminal", retentionOK, 1000, retentionTerminal},
		{"degrade rises to terminal", retentionDegrade, 1200, retentionTerminal},
		{"warn holds within hysteresis", retentionWarn, 460, retentionWarn},
		{"warn falls below hysteresis", retentionWarn, 440, retentionOK},
		{"degrade holds within hysteresis", retentionDegrade, 760, retentionDegrade},
		{"degrade falls to warn", retentionDegrade, 600, retentionWarn},
		{"degrade falls through warn to ok", retentionDegrade, 100, retentionOK},
		{"terminal is final", retentionTerminal, 0, retentionTerminal},
		{"an unknown retained size changes nothing", retentionWarn, -1, retentionWarn},
	} {
		t.Run(test.name, func(t *testing.T) {
			if got := nextRetentionState(test.current, test.retained, budget, 0.5, 0.8); got != test.want {
				t.Fatalf("nextRetentionState(%s, %d) = %s, want %s", test.current, test.retained, got, test.want)
			}
		})
	}
}

func TestRetentionPolicyStagesAndStopsTheStream(t *testing.T) {
	metrics := telemetrytest.New(t)
	reader := newMeteredReader(t, metrics.Meter(), func(context.Context, Transaction) error { return nil })
	ctx := context.Background()
	var stopped error
	stop := func(cause error) { stopped = cause }

	for _, retained := range []int64{100, 600, 700, 900, 400, 1000} {
		reader.applyRetentionPolicy(ctx, retained, 1000, stop)
		if retained < 1000 && stopped != nil {
			t.Fatalf("the stream was stopped at %d of 1000", retained)
		}
	}
	if !errors.Is(stopped, ErrRetainedWALBudgetExceeded) {
		t.Fatalf("stop cause = %v, want ErrRetainedWALBudgetExceeded", stopped)
	}
	if got := reader.Stats().RetentionState; got != "terminal" {
		t.Fatalf("RetentionState = %q, want terminal", got)
	}
	// 100 ok; 600 warn; 700 warn; 900 degrade; 400 ok; 1000 terminal.
	for state, want := range map[string]float64{"ok": 1, "warn": 1, "degrade": 1, "terminal": 1} {
		got, _ := metrics.Value(t, "watchd.cdc.retention.transitions", telemetry.Attributes(telemetry.KeyState.String(state)))
		if got != want {
			t.Errorf("transitions{%s} = %v, want %v", state, got, want)
		}
	}
	metrics.AssertAllowedAttributes(t)
}

func TestNewReaderRejectsInvalidRetentionThresholds(t *testing.T) {
	for _, fractions := range [][2]float64{{0.8, 0.5}, {0.5, 1}, {-0.1, 0.5}, {0.5, 0.5}} {
		_, err := NewReader(ReaderConfig{
			DatabaseURL:              "postgres://example.invalid/watchd",
			SourceID:                 "test-postgres",
			SlotName:                 "watchd_source",
			PublicationName:          "watchd_publication",
			RetentionWarnFraction:    fractions[0],
			RetentionDegradeFraction: fractions[1],
		}, func(context.Context, Transaction) error { return nil })
		if !errors.Is(err, ErrInvalidReaderConfig) {
			t.Errorf("warn %v, degrade %v: error = %v, want ErrInvalidReaderConfig", fractions[0], fractions[1], err)
		}
	}
}

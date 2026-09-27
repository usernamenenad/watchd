package cdc

import (
	"errors"
	"testing"
)

func TestParseVisibility(t *testing.T) {
	visibility, err := parseVisibility("100:110:102,105")
	if err != nil {
		t.Fatalf("parseVisibility: %v", err)
	}
	if visibility.xmin != 100 || visibility.xmax != 110 || len(visibility.inProgress) != 2 {
		t.Fatalf("visibility = %+v, want xmin 100, xmax 110, two in progress", visibility)
	}

	empty, err := parseVisibility("7:7:")
	if err != nil {
		t.Fatalf("parseVisibility with no in-progress transactions: %v", err)
	}
	if len(empty.inProgress) != 0 {
		t.Fatalf("in progress = %v, want none", empty.inProgress)
	}
}

func TestParseVisibilityRejectsMalformedSnapshots(t *testing.T) {
	for _, text := range []string{
		"",
		"100:110",
		"x:110:",
		"100:x:",
		"110:100:",       // xmax before xmin
		"100:110:99",     // in-progress before xmin
		"100:110:110",    // in-progress at xmax
		"100:110:102,,5", // empty entry
	} {
		if _, err := parseVisibility(text); !errors.Is(err, ErrPostgresServer) {
			t.Errorf("parseVisibility(%q) error = %v, want %v", text, err, ErrPostgresServer)
		}
	}
}

func TestVisibilityIncludes(t *testing.T) {
	visibility, err := parseVisibility("100:110:102,105")
	if err != nil {
		t.Fatalf("parseVisibility: %v", err)
	}
	for _, test := range []struct {
		xid  uint32
		want bool
	}{
		{xid: 50, want: true},   // finished before xmin
		{xid: 99, want: true},   // finished just before xmin
		{xid: 100, want: true},  // xmin itself is not in progress here
		{xid: 102, want: false}, // in progress when the snapshot was fixed
		{xid: 104, want: true},  // finished between xmin and xmax
		{xid: 105, want: false}, // in progress when the snapshot was fixed
		{xid: 110, want: false}, // at xmax: not yet finished
		{xid: 500, want: false}, // after the snapshot
	} {
		if got := visibility.includes(test.xid); got != test.want {
			t.Errorf("includes(%d) = %t, want %t", test.xid, got, test.want)
		}
	}
}

// TestVisibilityIncludesAcrossXIDWraparound checks that the 32-bit xid
// pgoutput reports is widened into the snapshot's epoch correctly on both
// sides of a 32-bit wraparound.
func TestVisibilityIncludesAcrossXIDWraparound(t *testing.T) {
	const epoch = uint64(3) << 32
	// xmin is just before the wraparound, xmax just after it.
	visibility := &visibility{
		xmin:       epoch - 10,
		xmax:       epoch + 10,
		inProgress: map[uint64]struct{}{epoch - 5: {}, epoch + 5: {}},
	}
	for _, test := range []struct {
		xid  uint32
		want bool
	}{
		{xid: ^uint32(0) - 20, want: true}, // previous epoch, before xmin
		{xid: ^uint32(0) - 4, want: false}, // previous epoch, in progress (epoch-5)
		{xid: ^uint32(0), want: true},      // previous epoch, finished
		{xid: 2, want: true},               // new epoch, finished
		{xid: 5, want: false},              // new epoch, in progress
		{xid: 10, want: false},             // new epoch, at xmax
		{xid: 1000, want: false},           // new epoch, after the snapshot
	} {
		if got := visibility.includes(test.xid); got != test.want {
			t.Errorf("includes(%d) = %t, want %t", test.xid, got, test.want)
		}
	}
}

package cdc

import (
	"fmt"
	"strconv"
	"strings"
)

// visibility is a PostgreSQL MVCC snapshot as reported by
// pg_current_snapshot(): which transactions had committed when a snapshot
// read was fixed. It lets a consumer decide exactly whether a streamed
// transaction is already in the snapshot's rows, instead of inferring it
// from WAL positions, which a commit can race.
//
// All IDs are 64-bit xid8 values (epoch and 32-bit xid), so they never wrap.
type visibility struct {
	// xmin is the oldest transaction still running when the snapshot was
	// taken; every transaction before it had finished.
	xmin uint64
	// xmax is one past the newest finished transaction; every transaction at
	// or after it had not finished.
	xmax uint64
	// inProgress lists the transactions in [xmin, xmax) still running.
	inProgress map[uint64]struct{}
}

// parseVisibility parses pg_snapshot's text form, "xmin:xmax:xip,xip,...".
func parseVisibility(text string) (*visibility, error) {
	parts := strings.Split(text, ":")
	if len(parts) != 3 {
		return nil, fmt.Errorf("%w: invalid pg_snapshot %q", ErrPostgresServer, text)
	}
	xmin, err := strconv.ParseUint(parts[0], 10, 64)
	if err != nil {
		return nil, fmt.Errorf("%w: invalid pg_snapshot xmin %q", ErrPostgresServer, text)
	}
	xmax, err := strconv.ParseUint(parts[1], 10, 64)
	if err != nil || xmax < xmin {
		return nil, fmt.Errorf("%w: invalid pg_snapshot xmax %q", ErrPostgresServer, text)
	}

	inProgress := make(map[uint64]struct{})
	if parts[2] != "" {
		for _, field := range strings.Split(parts[2], ",") {
			xid, err := strconv.ParseUint(field, 10, 64)
			if err != nil || xid < xmin || xid >= xmax {
				return nil, fmt.Errorf("%w: invalid pg_snapshot in-progress list %q", ErrPostgresServer, text)
			}
			inProgress[xid] = struct{}{}
		}
	}

	return &visibility{xmin: xmin, xmax: xmax, inProgress: inProgress}, nil
}

// includes reports whether the committed transaction xid is visible in the
// snapshot. pgoutput reports the 32-bit xid without its epoch; it is widened
// against xmax, which is sound because any transaction the stream still
// delivers is within 2^31 transactions of the snapshot - PostgreSQL's own
// wraparound protection guarantees no live transaction is further away.
func (v *visibility) includes(xid uint32) bool {
	full := v.widen(xid)
	if full >= v.xmax {
		return false
	}
	if full < v.xmin {
		return true
	}
	_, running := v.inProgress[full]
	return !running
}

func (v *visibility) widen(xid uint32) uint64 {
	distance := int64(int32(xid - uint32(v.xmax)))
	full := int64(v.xmax) + distance
	if full < 0 {
		return 0
	}
	return uint64(full)
}

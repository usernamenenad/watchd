//go:build integration

package integration

import (
	"context"
	"testing"
	"time"

	"github.com/jackc/pglogrepl"
	"github.com/nenad/watchd/internal/cdc"
)

func TestCDCConnectsInReplicationMode(t *testing.T) {
	ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
	defer cancel()

	stream, err := cdc.Connect(ctx, envOrDefault("WATCHD_TEST_REPLICATION_URL", defaultReplicationURL))
	if err != nil {
		t.Fatalf("connect CDC: %v", err)
	}
	defer stream.Close(ctx)

	system, err := pglogrepl.IdentifySystem(ctx, stream.Conn())
	if err != nil {
		t.Fatalf("identify PostgreSQL system: %v", err)
	}
	if system.SystemID == "" {
		t.Fatal("PostgreSQL returned an empty system ID")
	}
}

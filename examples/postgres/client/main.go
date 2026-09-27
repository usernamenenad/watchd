// Command client keeps one tenant's permissions in sync with a local watchd
// and prints every state change, so freshness is visible as it happens.
//
//	go run ./examples/postgres/client -tenant 00000000-0000-0000-0000-000000000001
package main

import (
	"context"
	"flag"
	"fmt"
	"log"
	"os"
	"os/signal"
	"slices"
	"syscall"

	"google.golang.org/grpc"
	"google.golang.org/grpc/credentials/insecure"

	"github.com/usernamenenad/watchd/sdk/go/watchd"
)

func main() {
	address := flag.String("addr", "127.0.0.1:7070", "watchd address")
	tenant := flag.String("tenant", "00000000-0000-0000-0000-000000000001", "tenant to keep in sync")
	flag.Parse()

	client, err := watchd.Dial(*address, grpc.WithTransportCredentials(insecure.NewCredentials()))
	if err != nil {
		log.Fatal(err)
	}
	defer client.Close()

	ctx, stop := signal.NotifyContext(context.Background(), os.Interrupt, syscall.SIGTERM)
	defer stop()

	store := watchd.NewMemoryStore("tenant_id", "user_id")
	err = client.Sync(ctx, watchd.SyncConfig{
		Projection: "tenant_permissions",
		Scope:      *tenant,
		Store:      store,
		OnState: func(state watchd.State) {
			freshness := "STALE"
			if state.Fresh {
				freshness = "FRESH"
			}
			fmt.Printf("%s  cursor=%-24s rows=%d\n", freshness, state.Cursor, store.Len())
			if state.Fresh {
				printRows(store)
			}
		},
	})
	if err != nil {
		log.Fatal(err)
	}
}

func printRows(store *watchd.MemoryStore) {
	var lines []string
	for _, row := range store.Rows() {
		lines = append(lines, fmt.Sprintf("       user=%s permissions=%s version=%s",
			row["user_id"].Text, row["permissions"].Text, row["version"].Text))
	}
	slices.Sort(lines)
	for _, line := range lines {
		fmt.Println(line)
	}
}

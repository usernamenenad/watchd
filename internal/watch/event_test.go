package watch

import (
	"errors"
	"sync"
	"sync/atomic"
	"testing"
)

func TestMemoComputesOnceForConcurrentCallers(t *testing.T) {
	var memo Memo
	var computed atomic.Int32
	var wg sync.WaitGroup
	for range 16 {
		wg.Go(func() {
			value, err := memo.Load(func() (any, error) {
				computed.Add(1)
				return "encoded", nil
			})
			if value != "encoded" || err != nil {
				t.Errorf("Load = %v, %v", value, err)
			}
		})
	}
	wg.Wait()
	if got := computed.Load(); got != 1 {
		t.Fatalf("computed %d times, want once", got)
	}
}

func TestMemoSharesAnError(t *testing.T) {
	var memo Memo
	failure := errors.New("cannot encode")
	for range 2 {
		if _, err := memo.Load(func() (any, error) { return nil, failure }); !errors.Is(err, failure) {
			t.Fatalf("Load error = %v, want %v", err, failure)
		}
	}
}

func TestNilMemoComputesEveryTime(t *testing.T) {
	var memo *Memo
	computed := 0
	for range 2 {
		if _, err := memo.Load(func() (any, error) { computed++; return nil, nil }); err != nil {
			t.Fatal(err)
		}
	}
	if computed != 2 {
		t.Fatalf("computed %d times, want 2", computed)
	}
}

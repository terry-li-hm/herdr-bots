package main

import (
	"context"
	"os"
	"path/filepath"
	"reflect"
	"strings"
	"testing"
	"time"

	"github.com/terry-li-hm/herdr-bots/internal/store"
)

// showReceiptRun creates a real temporary store holding one job and one
// nonterminal run, lets the caller persist receipts against it, and returns the
// state path and run id for the CLI to read back through its own handle.
func showReceiptRun(t *testing.T, record func(ctx context.Context, state *store.Store, runID string) error) (string, string) {
	t.Helper()
	statePath := filepath.Join(t.TempDir(), "state.db")
	state, err := store.Open(statePath)
	if err != nil {
		t.Fatal(err)
	}
	ctx := context.Background()
	now := time.Now()
	definition := []byte(`{"id":"review"}`)
	if _, err := state.SyncJob(ctx, "review", "rev1", definition, true, now); err != nil {
		state.Close()
		t.Fatal(err)
	}
	created, err := state.CreateManualRun(ctx, "review", "rev1", definition, now)
	if err != nil {
		state.Close()
		t.Fatal(err)
	}
	if created.State != store.StateAccepted || !created.Unread {
		state.Close()
		t.Fatalf("fixture run must start nonterminal and unread: %+v", created)
	}
	// Receipts are only writable while the run is nonterminal, which is exactly
	// the state a freshly accepted run is in.
	if record != nil {
		if err := record(ctx, state, created.ID); err != nil {
			state.Close()
			t.Fatal(err)
		}
	}
	if err := state.Close(); err != nil {
		t.Fatal(err)
	}
	return statePath, created.ID
}

// captureShowStdout runs fn with os.Stdout replaced by a pipe and returns
// everything the command printed along with the command's own error.
func captureShowStdout(t *testing.T, fn func() error) (string, error) {
	t.Helper()
	read, writeEnd, err := os.Pipe()
	if err != nil {
		t.Fatal(err)
	}
	var captured []byte
	done := make(chan struct{})
	go func() {
		buf := make([]byte, 4096)
		for {
			n, readErr := read.Read(buf)
			captured = append(captured, buf[:n]...)
			if readErr != nil {
				close(done)
				return
			}
		}
	}()
	original := os.Stdout
	os.Stdout = writeEnd
	fnErr := fn()
	os.Stdout = original
	_ = writeEnd.Close()
	<-done
	_ = read.Close()
	return string(captured), fnErr
}

// assertReceiptLinesFollowVerdict proves the three receipt lines are emitted
// exactly, in order, on the three lines immediately after the verdict line.
func assertReceiptLinesFollowVerdict(t *testing.T, output string, want []string) {
	t.Helper()
	lines := strings.Split(output, "\n")
	for i, line := range lines {
		if !strings.HasPrefix(line, "verdict: ") {
			continue
		}
		if len(lines) < i+1+len(want) {
			t.Fatalf("show output ended before the receipt lines: %q", output)
		}
		if got := lines[i+1 : i+1+len(want)]; !reflect.DeepEqual(got, want) {
			t.Fatalf("receipt lines = %q, want %q", got, want)
		}
		return
	}
	t.Fatalf("show output has no verdict line: %q", output)
}

func TestShowReportsPersistedReceiptsVerbatimAndMarksRead(t *testing.T) {
	const (
		attestation   = `{"model":"claude-opus-5","provider":"anthropic","observed_at":"2026-08-25T09:00:00Z"}`
		inputReceipt  = `{"revision":"1a2b3c","paths":["internal/engine","internal/store"]}`
		changeReceipt = `{"branch":"review/bounded","paths":["internal/engine/bounded_review.go"]}`
	)
	statePath, runID := showReceiptRun(t, func(ctx context.Context, state *store.Store, id string) error {
		if err := state.SetModelAttestation(ctx, id, attestation); err != nil {
			return err
		}
		if err := state.SetInputReceipt(ctx, id, inputReceipt); err != nil {
			return err
		}
		return state.SetChangeReceipt(ctx, id, changeReceipt)
	})

	output, showErr := captureShowStdout(t, func() error {
		return showCmd([]string{runID, "--state", statePath})
	})
	if showErr != nil {
		t.Fatal(showErr)
	}
	assertReceiptLinesFollowVerdict(t, output, []string{
		"model-attestation: " + attestation,
		"input-receipt: " + inputReceipt,
		"change-receipt: " + changeReceipt,
	})

	// show remains the read action: reporting receipts must not have changed
	// which runs the inbox still owes attention to.
	state, err := store.Open(statePath)
	if err != nil {
		t.Fatal(err)
	}
	defer state.Close()
	stored, err := state.GetRun(context.Background(), runID)
	if err != nil {
		t.Fatal(err)
	}
	if stored.Unread {
		t.Fatalf("show did not mark the run read: %+v", stored)
	}
	if stored.ModelAttestation != attestation || stored.InputReceipt != inputReceipt || stored.ChangeReceipt != changeReceipt {
		t.Fatalf("show altered durable receipts: %+v", stored)
	}
}

func TestShowReportsOmittedReceiptsAsAbsent(t *testing.T) {
	statePath, runID := showReceiptRun(t, nil)

	output, showErr := captureShowStdout(t, func() error {
		return showCmd([]string{runID, "--state", statePath})
	})
	if showErr != nil {
		t.Fatal(showErr)
	}
	assertReceiptLinesFollowVerdict(t, output, []string{
		"model-attestation: -",
		"input-receipt: -",
		"change-receipt: -",
	})

	state, err := store.Open(statePath)
	if err != nil {
		t.Fatal(err)
	}
	defer state.Close()
	stored, err := state.GetRun(context.Background(), runID)
	if err != nil {
		t.Fatal(err)
	}
	if stored.Unread {
		t.Fatalf("show did not mark the run read: %+v", stored)
	}
}

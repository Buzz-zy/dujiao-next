package local

import (
	"errors"
	"fmt"
	"os"
	"os/exec"
	"path/filepath"
	"strings"
	"syscall"
	"testing"
	"time"

	"github.com/dujiao-next/internal/modules/procurement/contract"
)

func testEvent(id, kind string) contract.EvidenceEvent {
	return contract.EvidenceEvent{EventID: id, ProcurementOrderID: 42, TraceID: "trace-42", EventType: kind, SchemaVersion: 1, Supplier: "faka", TradeNo: "trade-42", Amount: "12.30", ResponseAt: time.Date(2026, 11, 17, 1, 2, 3, 0, time.UTC)}
}

func TestAppendEventsSurviveReopenAndAreIdempotent(t *testing.T) {
	dir := t.TempDir()
	store, err := New(dir)
	if err != nil {
		t.Fatal(err)
	}
	intent := testEvent("intent-1", "trade_intent")
	if err := store.AppendIntent(intent); err != nil {
		t.Fatal(err)
	}
	response := testEvent("response-1", "trade_response")
	response.RelatedEventID = intent.EventID
	if err := store.AppendResponse(response); err != nil {
		t.Fatal(err)
	}
	if err := store.AppendResponse(response); err != nil {
		t.Fatalf("same event should be idempotent: %v", err)
	}
	if err := store.Close(); err != nil {
		t.Fatal(err)
	}

	reopened, err := New(dir)
	if err != nil {
		t.Fatal(err)
	}
	defer reopened.Close()
	got, err := reopened.GetByEventID(response.EventID)
	if err != nil {
		t.Fatal(err)
	}
	if got == nil || got.TradeNo != response.TradeNo || got.Amount != response.Amount || got.RelatedEventID != intent.EventID {
		t.Fatalf("recovered event = %#v", got)
	}
	byTrace, err := reopened.GetByTraceID(response.TraceID)
	if err != nil || len(byTrace) != 2 {
		t.Fatalf("trace events=%d err=%v", len(byTrace), err)
	}
	pending, err := reopened.ListPending()
	if err != nil || len(pending) != 1 || pending[0].EventID != response.EventID {
		t.Fatalf("pending=%#v err=%v", pending, err)
	}
}

func TestSameEventIDDifferentPayloadConflicts(t *testing.T) {
	store, err := New(t.TempDir())
	if err != nil {
		t.Fatal(err)
	}
	defer store.Close()
	one := testEvent("same", "trade_intent")
	if err := store.AppendIntent(one); err != nil {
		t.Fatal(err)
	}
	two := one
	two.Amount = "99.00"
	if err := store.AppendIntent(two); !errors.Is(err, ErrEventConflict) {
		t.Fatalf("error=%v, want conflict", err)
	}
}

func TestCorruptLogIsQuarantined(t *testing.T) {
	dir := t.TempDir()
	store, err := New(dir)
	if err != nil {
		t.Fatal(err)
	}
	first := testEvent("first", "trade_intent")
	if err := store.AppendIntent(first); err != nil {
		t.Fatal(err)
	}
	if err := store.Close(); err != nil {
		t.Fatal(err)
	}
	path := filepath.Join(dir, logFileName)
	file, err := os.OpenFile(path, os.O_APPEND|os.O_WRONLY, 0)
	if err != nil {
		t.Fatal(err)
	}
	_, _ = file.Write([]byte{0, 0, 0, 20, 1, 2})
	_ = file.Close()
	corruptStore, openErr := New(dir)
	if !errors.Is(openErr, ErrCorruptLog) {
		t.Fatalf("error=%v, want corrupt log", openErr)
	}
	if corruptStore != nil {
		t.Fatal("corrupt store unexpectedly opened")
	}
	if !strings.Contains(openErr.Error(), "1 valid records restored") {
		t.Fatalf("corruption error must report restored evidence count: %v", openErr)
	}
	matches, err := filepath.Glob(filepath.Join(dir, logFileName+".corrupt.*"))
	if err != nil || len(matches) != 1 {
		t.Fatalf("quarantine files=%v err=%v", matches, err)
	}
	if _, err := os.Stat(matches[0]); err != nil {
		t.Fatalf("quarantined original log must be preserved: %v", err)
	}
	recovered, err := New(dir)
	if err != nil {
		t.Fatalf("reopen recovered valid prefix: %v", err)
	}
	defer recovered.Close()
	got, err := recovered.GetByEventID(first.EventID)
	if err != nil || got == nil || *got != first {
		t.Fatalf("previous valid evidence was not recovered: event=%#v err=%v", got, err)
	}
}

func TestIntentIsNotPendingResponse(t *testing.T) {
	store, err := New(t.TempDir())
	if err != nil {
		t.Fatal(err)
	}
	defer store.Close()
	if err := store.AppendIntent(testEvent("intent-only", "trade_intent")); err != nil {
		t.Fatal(err)
	}
	pending, err := store.ListPending()
	if err != nil {
		t.Fatal(err)
	}
	if len(pending) != 0 {
		t.Fatalf("intent-only event must never be replayed as response: %#v", pending)
	}
}

func TestDirectoryLockAndImportedMarkerSurviveRestart(t *testing.T) {
	dir := t.TempDir()
	store, err := New(dir)
	if err != nil {
		t.Fatal(err)
	}
	if _, err := New(dir); err == nil {
		t.Fatal("second process opened same evidence directory")
	}
	response := testEvent("response-imported", "trade_response")
	if err := store.AppendResponse(response); err != nil {
		t.Fatal(err)
	}
	if err := store.MarkImported(response.EventID); err != nil {
		t.Fatal(err)
	}
	if err := store.Close(); err != nil {
		t.Fatal(err)
	}
	reopened, err := New(dir)
	if err != nil {
		t.Fatal(err)
	}
	defer reopened.Close()
	pending, err := reopened.ListPending()
	if err != nil || len(pending) != 0 {
		t.Fatalf("pending=%#v err=%v", pending, err)
	}
	got, err := reopened.GetByEventID(response.EventID)
	if err != nil || got == nil || got.TradeNo != response.TradeNo {
		t.Fatalf("original evidence=%#v err=%v", got, err)
	}
}

func TestDirectoryLockBlocksIndependentProcess(t *testing.T) {
	dir := t.TempDir()
	store, err := New(dir)
	if err != nil {
		t.Fatal(err)
	}
	defer store.Close()

	command := exec.Command(os.Args[0], "-test.run=^TestEvidenceLockSubprocess$")
	command.Env = append(os.Environ(), "EVIDENCE_LOCK_TEST_DIR="+dir)
	output, err := command.CombinedOutput()
	if err != nil {
		t.Fatalf("second process lock check failed: %v\n%s", err, output)
	}
}

func TestEvidenceLockSubprocess(t *testing.T) {
	dir := os.Getenv("EVIDENCE_LOCK_TEST_DIR")
	if dir == "" {
		return
	}
	store, err := New(dir)
	if err == nil {
		_ = store.Close()
		t.Fatal("independent process unexpectedly acquired evidence lock")
	}
}

func TestAppendAndSyncFailuresKeepPriorEvidenceRecoverable(t *testing.T) {
	t.Run("partial append failure", func(t *testing.T) {
		dir := t.TempDir()
		store, err := New(dir)
		if err != nil {
			t.Fatal(err)
		}
		prior := testEvent("prior", "trade_intent")
		if err := store.AppendIntent(prior); err != nil {
			t.Fatal(err)
		}
		store.writeFrame = func(file *os.File, frame []byte) (int, error) {
			n, writeErr := file.Write(frame[:min(5, len(frame))])
			if writeErr != nil {
				return n, writeErr
			}
			return n, syscall.ENOSPC
		}
		if err := store.AppendResponse(testEvent("response", "trade_response")); !errors.Is(err, syscall.ENOSPC) {
			t.Fatalf("append error=%v, want disk full", err)
		}
		if err := store.AppendIntent(testEvent("later", "trade_intent")); err == nil {
			t.Fatal("store must remain fail-closed after a partial append")
		}
		_ = store.Close()
		if _, err := New(dir); !errors.Is(err, ErrCorruptLog) {
			t.Fatalf("reopen must report and quarantine partial frame, got %v", err)
		}
		recovered, err := New(dir)
		if err != nil {
			t.Fatal(err)
		}
		defer recovered.Close()
		got, err := recovered.GetByEventID(prior.EventID)
		if err != nil || got == nil {
			t.Fatalf("prior event lost after partial append: event=%#v err=%v", got, err)
		}
	})

	t.Run("sync failure", func(t *testing.T) {
		dir := t.TempDir()
		store, err := New(dir)
		if err != nil {
			t.Fatal(err)
		}
		prior := testEvent("prior", "trade_intent")
		if err := store.AppendIntent(prior); err != nil {
			t.Fatal(err)
		}
		store.syncFile = func(*os.File) error { return fmt.Errorf("injected fsync failure") }
		if err := store.AppendResponse(testEvent("response", "trade_response")); err == nil || !strings.Contains(err.Error(), "injected fsync failure") {
			t.Fatalf("sync error=%v, want injected fsync failure", err)
		}
		if err := store.AppendIntent(testEvent("later", "trade_intent")); err == nil {
			t.Fatal("store must remain fail-closed after fsync failure")
		}
		_ = store.Close()
		reopened, err := New(dir)
		if err != nil {
			t.Fatal(err)
		}
		defer reopened.Close()
		got, err := reopened.GetByEventID(prior.EventID)
		if err != nil || got == nil {
			t.Fatalf("prior event lost after fsync failure: event=%#v err=%v", got, err)
		}
	})
}

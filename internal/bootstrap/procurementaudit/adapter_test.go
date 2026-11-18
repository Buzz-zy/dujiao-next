package procurementaudit

import (
	"database/sql"
	"errors"
	"path/filepath"
	"strings"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	"github.com/dujiao-next/internal/constants"
	auditlogdomain "github.com/dujiao-next/internal/modules/auditlog/domain"
	procurementapp "github.com/dujiao-next/internal/modules/procurement/application"
	procurementcontract "github.com/dujiao-next/internal/modules/procurement/contract"
	procurementdomain "github.com/dujiao-next/internal/modules/procurement/domain"
	"github.com/glebarez/sqlite"
	"gorm.io/driver/postgres"
	"gorm.io/gorm"
	"gorm.io/gorm/clause"
)

func decisionTestDB(t *testing.T, withAudit bool, status string) (*gorm.DB, uint) {
	t.Helper()
	db, err := gorm.Open(sqlite.Open(filepath.Join(t.TempDir(), "decision.db")), &gorm.Config{DisableForeignKeyConstraintWhenMigrating: true})
	if err != nil {
		t.Fatal(err)
	}
	if err := db.AutoMigrate(&procurementdomain.Order{}); err != nil {
		t.Fatal(err)
	}
	if err := db.AutoMigrate(&procurementdomain.TradeResponseJournalEntry{}); err != nil {
		t.Fatal(err)
	}
	if withAudit {
		if err := db.AutoMigrate(&auditlogdomain.AuthzAuditLog{}); err != nil {
			t.Fatal(err)
		}
	}
	sqlDB, err := db.DB()
	if err != nil {
		t.Fatal(err)
	}
	sqlDB.SetMaxOpenConns(1)
	t.Cleanup(func() { _ = sqlDB.Close() })
	order := &procurementdomain.Order{ConnectionID: 1, LocalOrderID: 1, Status: status, TraceID: "stable-request-no", Currency: "CNY"}
	if err := db.Create(order).Error; err != nil {
		t.Fatal(err)
	}
	return db, order.ID
}

func TestTradeResponseJournalPersistsAndQueriesByTraceID(t *testing.T) {
	db, _ := decisionTestDB(t, true, constants.ProcurementStatusAccepted)
	adapter := New(db)
	respondedAt := time.Now().UTC()
	for _, tc := range []struct{ kind, no string }{{"late_response_same_trade_no", "MATCH"}, {"late_response_trade_no_mismatch", "WORKER-2"}} {
		if err := adapter.AppendTradeResponseJournal(procurementdomain.TradeResponseJournalEntry{ProcurementOrderID: 12, TraceID: "stable-trace", UpstreamTradeNo: tc.no, Amount: "7.00", ConfirmedTradeNo: "ADMIN-1", ConfirmedAmount: "5.00", EventType: tc.kind, ResponseTime: respondedAt}); err != nil {
			t.Fatal(err)
		}
	}
	entries, err := adapter.ListTradeResponseJournalByTraceID("stable-trace")
	if err != nil || len(entries) != 2 {
		t.Fatalf("late audit entries=%d error=%v", len(entries), err)
	}
	if entries[0].TraceID != "stable-trace" || entries[0].ConfirmedAmount != "5.00" || entries[0].Amount != "7.00" || entries[0].ResponseTime.IsZero() {
		t.Fatalf("late trade audit lacks required metadata: %#v", entries[0])
	}
}

func TestConflictCommitThenCallerErrorRetriesWithoutOverwritingResolution(t *testing.T) {
	db, _ := decisionTestDB(t, false, constants.ProcurementStatusAwaitingVerification)
	if err := db.AutoMigrate(&procurementdomain.TradeEvidenceConflict{}); err != nil {
		t.Fatal(err)
	}
	adapter := New(db)
	conflict := procurementdomain.TradeEvidenceConflict{EventID: "event-ambiguous", ProcurementOrderID: 12, TraceID: "stable-trace", ConflictType: "payload_mismatch", ExistingEventHash: "existing-hash", IncomingEventHash: "incoming-hash", Status: "open"}
	var injected atomic.Bool
	if err := db.Callback().Create().After("gorm:create").Register("test:commit_then_error", func(tx *gorm.DB) {
		if tx.Statement.Table == "procurement_trade_evidence_conflicts" && injected.CompareAndSwap(false, true) {
			tx.AddError(errors.New("injected post-commit response loss"))
		}
	}); err != nil {
		t.Fatal(err)
	}
	if err := adapter.RecordTradeEvidenceConflict(conflict); err == nil {
		t.Fatal("first call should report injected post-commit error")
	}
	rows, err := adapter.GetTradeEvidenceConflictsByEventID(conflict.EventID)
	if err != nil || len(rows) != 1 {
		t.Fatalf("committed row missing: len=%d err=%v", len(rows), err)
	}
	if err := adapter.ResolveTradeEvidenceConflict(rows[0].ID, 73, "reviewed original evidence"); err != nil {
		t.Fatal(err)
	}
	if err := adapter.RecordTradeEvidenceConflict(conflict); err != nil {
		t.Fatalf("idempotent retry: %v", err)
	}
	rows, err = adapter.GetTradeEvidenceConflictsByEventID(conflict.EventID)
	if err != nil || len(rows) != 1 {
		t.Fatalf("retry duplicated row: len=%d err=%v", len(rows), err)
	}
	got := rows[0]
	if got.EventID != conflict.EventID || got.ExistingEventHash != conflict.ExistingEventHash || got.IncomingEventHash != conflict.IncomingEventHash || got.Status != "resolved" || got.ResolutionNote != "reviewed original evidence" || got.ResolvedByAdminID == nil || *got.ResolvedByAdminID != 73 || got.ResolvedAt == nil {
		t.Fatalf("retry overwrote persisted content or resolution: %#v", got)
	}
}

func TestConcurrentWorkersDeduplicateIdenticalConflictAndPreserveResolution(t *testing.T) {
	db, _ := decisionTestDB(t, false, constants.ProcurementStatusAwaitingVerification)
	if err := db.AutoMigrate(&procurementdomain.TradeEvidenceConflict{}); err != nil {
		t.Fatal(err)
	}
	first, second := New(db), New(db)
	conflict := procurementdomain.TradeEvidenceConflict{EventID: "event-concurrent-conflict", ProcurementOrderID: 12, TraceID: "stable-trace", ConflictType: "payload_mismatch", ExistingEventHash: "hash-existing", IncomingEventHash: "hash-incoming", Status: "open"}
	if err := first.RecordTradeEvidenceConflict(conflict); err != nil {
		t.Fatal(err)
	}
	rows, err := first.GetTradeEvidenceConflictsByEventID(conflict.EventID)
	if err != nil || len(rows) != 1 {
		t.Fatalf("initial record rows=%d err=%v", len(rows), err)
	}
	if err := first.ResolveTradeEvidenceConflict(rows[0].ID, 91, "manual review completed"); err != nil {
		t.Fatal(err)
	}
	start := make(chan struct{})
	errs := make(chan error, 2)
	var wg sync.WaitGroup
	for _, worker := range []*Adapter{first, second} {
		wg.Add(1)
		go func(adapter *Adapter) {
			defer wg.Done()
			<-start
			errs <- adapter.RecordTradeEvidenceConflict(conflict)
		}(worker)
	}
	close(start)
	wg.Wait()
	close(errs)
	for err := range errs {
		if err != nil {
			t.Fatalf("concurrent conflict record: %v", err)
		}
	}
	rows, err = second.GetTradeEvidenceConflictsByEventID(conflict.EventID)
	if err != nil || len(rows) != 1 {
		t.Fatalf("concurrent insert rows=%d err=%v", len(rows), err)
	}
	got := rows[0]
	if got.EventID != conflict.EventID || got.ExistingEventHash != conflict.ExistingEventHash || got.IncomingEventHash != conflict.IncomingEventHash || got.ConflictType != conflict.ConflictType {
		t.Fatalf("conflict payload changed: %#v", got)
	}
	if got.Status != "resolved" || got.ResolutionNote != "manual review completed" || got.ResolvedByAdminID == nil || *got.ResolvedByAdminID != 91 || got.ResolvedAt == nil {
		t.Fatalf("concurrent rescan overwrote resolution: %#v", got)
	}
}

func TestConflictResolutionSurvivesDatabaseRestartAndIdempotentReplay(t *testing.T) {
	dbPath := filepath.Join(t.TempDir(), "conflict-restart.db")
	open := func() (*gorm.DB, *sql.DB) {
		db, err := gorm.Open(sqlite.Open(dbPath), &gorm.Config{DisableForeignKeyConstraintWhenMigrating: true})
		if err != nil {
			t.Fatal(err)
		}
		if err := db.AutoMigrate(&procurementdomain.TradeEvidenceConflict{}); err != nil {
			t.Fatal(err)
		}
		sqlDB, err := db.DB()
		if err != nil {
			t.Fatal(err)
		}
		sqlDB.SetMaxOpenConns(1)
		return db, sqlDB
	}
	db, sqlDB := open()
	first := New(db)
	conflict := procurementdomain.TradeEvidenceConflict{EventID: "event-restart", ProcurementOrderID: 12, TraceID: "trace-restart", ConflictType: "payload_mismatch", ExistingEventHash: "sha-existing", IncomingEventHash: "sha-incoming", Status: "open"}
	if err := first.RecordTradeEvidenceConflict(conflict); err != nil {
		t.Fatal(err)
	}
	rows, err := first.GetTradeEvidenceConflictsByEventID(conflict.EventID)
	if err != nil || len(rows) != 1 {
		t.Fatalf("initial query rows=%d err=%v", len(rows), err)
	}
	if err := first.ResolveTradeEvidenceConflict(rows[0].ID, 57, "confirmed by operator after review"); err != nil {
		t.Fatal(err)
	}
	if err := sqlDB.Close(); err != nil {
		t.Fatalf("close first database: %v", err)
	}

	db2, sqlDB2 := open()
	t.Cleanup(func() { _ = sqlDB2.Close() })
	second := New(db2)
	rows, err = second.GetTradeEvidenceConflictsByEventID(conflict.EventID)
	if err != nil || len(rows) != 1 {
		t.Fatalf("query after restart rows=%d err=%v", len(rows), err)
	}
	before := rows[0]
	if before.EventID != conflict.EventID || before.ConflictType != conflict.ConflictType || before.ExistingEventHash != conflict.ExistingEventHash || before.IncomingEventHash != conflict.IncomingEventHash || before.Status != "resolved" || before.ResolvedByAdminID == nil || *before.ResolvedByAdminID != 57 || before.ResolvedAt == nil || before.ResolutionNote != "confirmed by operator after review" {
		t.Fatalf("conflict resolution was not persisted across restart: %#v", before)
	}
	if err := second.RecordTradeEvidenceConflict(conflict); err != nil {
		t.Fatalf("replay after restart: %v", err)
	}
	rows, err = second.GetTradeEvidenceConflictsByEventID(conflict.EventID)
	if err != nil || len(rows) != 1 {
		t.Fatalf("replay duplicated conflict: rows=%d err=%v", len(rows), err)
	}
	after := rows[0]
	if after.Status != before.Status || after.ResolutionNote != before.ResolutionNote || after.ResolvedByAdminID == nil || *after.ResolvedByAdminID != *before.ResolvedByAdminID || after.ResolvedAt == nil || !after.ResolvedAt.Equal(*before.ResolvedAt) || after.EventID != before.EventID || after.ExistingEventHash != before.ExistingEventHash || after.IncomingEventHash != before.IncomingEventHash {
		t.Fatalf("idempotent replay overwrote conflict/resolution: before=%#v after=%#v", before, after)
	}
	var count int64
	if err := db2.Model(&procurementdomain.TradeEvidenceConflict{}).Where("conflict_key = ?", before.ConflictKey).Count(&count).Error; err != nil || count != 1 {
		t.Fatalf("persisted conflict count=%d err=%v", count, err)
	}
}

func evidenceResponse(eventID, tradeNo, amount string) procurementcontract.EvidenceEvent {
	return procurementcontract.EvidenceEvent{EventID: eventID, ProcurementOrderID: 12, TraceID: "stable-trace", EventType: "trade_response", TradeNo: tradeNo, Amount: amount, ResponseAt: time.Date(2026, 11, 17, 12, 0, 0, 123, time.UTC), SchemaVersion: 1, Supplier: "faka", RelatedEventID: "intent-12"}
}

func TestImportTradeResponseJournalIsIdempotentAndDetectsConflict(t *testing.T) {
	db, _ := decisionTestDB(t, false, constants.ProcurementStatusAwaitingVerification)
	adapter := New(db)
	event := evidenceResponse("event-1", "WORKER-TRADE", "8.25")
	if err := adapter.ImportTradeResponseEvidence(event); err != nil {
		t.Fatal(err)
	}
	if err := adapter.ImportTradeResponseEvidence(event); err != nil {
		t.Fatalf("identical re-import: %v", err)
	}
	changed := event
	changed.Amount = "88.25"
	if err := adapter.ImportTradeResponseEvidence(changed); !errors.Is(err, procurementcontract.ErrEvidenceJournalConflict) {
		t.Fatalf("conflict=%v", err)
	}
	entries, err := adapter.ListTradeResponseJournalByTraceID(event.TraceID)
	if err != nil || len(entries) != 1 {
		t.Fatalf("entries=%d err=%v", len(entries), err)
	}
	entry := entries[0]
	if entry.SourceEventID == nil || *entry.SourceEventID != event.EventID || entry.SourceEventHash == nil || entry.UpstreamTradeNo != event.TradeNo || entry.Amount != event.Amount || !entry.ResponseTime.Equal(event.ResponseAt) {
		t.Fatalf("imported entry=%#v", entry)
	}
	changedSupplier := event
	changedSupplier.Supplier = "different-supplier"
	if err := adapter.ImportTradeResponseEvidence(changedSupplier); !errors.Is(err, procurementcontract.ErrEvidenceJournalConflict) {
		t.Fatalf("non-column source content conflict=%v", err)
	}
}

func TestScanPendingPersistsConflictAndRepeatedScansPreserveResolution(t *testing.T) {
	db, _ := decisionTestDB(t, false, constants.ProcurementStatusAwaitingVerification)
	if err := db.AutoMigrate(&procurementdomain.TradeEvidenceConflict{}); err != nil {
		t.Fatal(err)
	}
	adapter := New(db)
	original := evidenceResponse("scan-conflict-event", "ORIGINAL-TRADE", "5.00")
	if err := adapter.ImportTradeResponseEvidence(original); err != nil {
		t.Fatal(err)
	}
	incoming := original
	incoming.Amount = "9.00"
	source := &confirmedEventSource{event: incoming}
	recovery, err := procurementapp.NewTradeResponseJournalRecovery(source, adapter)
	if err != nil {
		t.Fatal(err)
	}
	var originalRows []procurementdomain.TradeResponseJournalEntry
	if err := db.Where("source_event_id = ?", original.EventID).Find(&originalRows).Error; err != nil || len(originalRows) != 1 {
		t.Fatalf("original journal rows=%d err=%v", len(originalRows), err)
	}
	originalHash := *originalRows[0].SourceEventHash
	changedEntry, err := procurementcontract.TradeResponseJournalEntryFromEvidence(incoming)
	if err != nil {
		t.Fatal(err)
	}
	for i := 0; i < 10; i++ {
		report, scanErr := recovery.ScanPending()
		if !errors.Is(scanErr, procurementcontract.ErrEvidenceJournalConflict) || len(report.Failures) != 1 || report.Failures[0].EventID != original.EventID {
			t.Fatalf("scan %d did not report conflict: report=%+v err=%v", i+1, report, scanErr)
		}
	}
	conflicts, err := adapter.GetTradeEvidenceConflictsByEventID(original.EventID)
	if err != nil || len(conflicts) != 1 {
		t.Fatalf("conflicts=%d err=%v", len(conflicts), err)
	}
	conflict := conflicts[0]
	if conflict.EventID != original.EventID || conflict.ConflictType != "source_event_payload_mismatch" || conflict.ExistingEventHash != originalHash || conflict.IncomingEventHash != *changedEntry.SourceEventHash {
		t.Fatalf("incorrect conflict evidence: %#v", conflict)
	}
	if err := adapter.ResolveTradeEvidenceConflict(conflict.ID, 64, "reviewed evidence conflict"); err != nil {
		t.Fatal(err)
	}
	before, err := adapter.GetTradeEvidenceConflictsByEventID(original.EventID)
	if err != nil || len(before) != 1 {
		t.Fatalf("before resolution snapshot: rows=%d err=%v", len(before), err)
	}
	for i := 0; i < 10; i++ {
		report, scanErr := recovery.ScanPending()
		if !errors.Is(scanErr, procurementcontract.ErrEvidenceJournalConflict) || len(report.Failures) != 1 || report.Failures[0].EventID != original.EventID {
			t.Fatalf("post-resolution scan %d did not report conflict: report=%+v err=%v", i+1, report, scanErr)
		}
	}
	after, err := adapter.GetTradeEvidenceConflictsByEventID(original.EventID)
	if err != nil || len(after) != 1 {
		t.Fatalf("after scans conflicts=%d err=%v", len(after), err)
	}
	if after[0].Status != "resolved" || after[0].ResolutionNote != before[0].ResolutionNote || after[0].ResolvedByAdminID == nil || *after[0].ResolvedByAdminID != 64 || after[0].ResolvedAt == nil || before[0].ResolvedAt == nil || !after[0].ResolvedAt.Equal(*before[0].ResolvedAt) || !after[0].LastSeenAt.After(before[0].LastSeenAt) {
		t.Fatalf("rescan changed resolution or did not update LastSeenAt: before=%#v after=%#v", before[0], after[0])
	}
	var persisted procurementdomain.TradeResponseJournalEntry
	if err := db.Where("source_event_id = ?", original.EventID).First(&persisted).Error; err != nil {
		t.Fatal(err)
	}
	if persisted.Amount != original.Amount || persisted.UpstreamTradeNo != original.TradeNo || persisted.SourceEventHash == nil || *persisted.SourceEventHash != originalHash {
		t.Fatalf("original Journal was overwritten: %#v", persisted)
	}
}

func TestScanPendingConflictSurvivesDatabaseRestart(t *testing.T) {
	dbPath := filepath.Join(t.TempDir(), "scan-conflict-restart.db")
	open := func() *gorm.DB {
		db, err := gorm.Open(sqlite.Open(dbPath), &gorm.Config{DisableForeignKeyConstraintWhenMigrating: true})
		if err != nil {
			t.Fatal(err)
		}
		if err := db.AutoMigrate(&procurementdomain.TradeResponseJournalEntry{}, &procurementdomain.TradeEvidenceConflict{}); err != nil {
			t.Fatal(err)
		}
		return db
	}
	original := evidenceResponse("scan-restart-event", "TRADE-ORIG", "2.00")
	incoming := original
	incoming.Amount = "3.00"
	source := &confirmedEventSource{event: incoming}
	db := open()
	first := New(db)
	if err := first.ImportTradeResponseEvidence(original); err != nil {
		t.Fatal(err)
	}
	recovery, err := procurementapp.NewTradeResponseJournalRecovery(source, first)
	if err != nil {
		t.Fatal(err)
	}
	if report, scanErr := recovery.ScanPending(); !errors.Is(scanErr, procurementcontract.ErrEvidenceJournalConflict) || len(report.Failures) != 1 {
		t.Fatalf("initial conflict scan report=%+v err=%v", report, scanErr)
	}
	rows, err := first.GetTradeEvidenceConflictsByEventID(original.EventID)
	if err != nil || len(rows) != 1 {
		t.Fatalf("initial conflicts=%d err=%v", len(rows), err)
	}
	if err := first.ResolveTradeEvidenceConflict(rows[0].ID, 65, "resolved before restart"); err != nil {
		t.Fatal(err)
	}
	sqlDB, err := db.DB()
	if err != nil {
		t.Fatal(err)
	}
	if err := sqlDB.Close(); err != nil {
		t.Fatal(err)
	}

	db2 := open()
	sqlDB2, err := db2.DB()
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { _ = sqlDB2.Close() })
	second := New(db2)
	recovery2, err := procurementapp.NewTradeResponseJournalRecovery(source, second)
	if err != nil {
		t.Fatal(err)
	}
	if report, scanErr := recovery2.ScanPending(); !errors.Is(scanErr, procurementcontract.ErrEvidenceJournalConflict) || len(report.Failures) != 1 || report.Failures[0].EventID != original.EventID {
		t.Fatalf("restart conflict scan report=%+v err=%v", report, scanErr)
	}
	rows, err = second.GetTradeEvidenceConflictsByEventID(original.EventID)
	if err != nil || len(rows) != 1 {
		t.Fatalf("restart conflicts=%d err=%v", len(rows), err)
	}
	if rows[0].Status != "resolved" || rows[0].ResolutionNote != "resolved before restart" || rows[0].ResolvedByAdminID == nil || *rows[0].ResolvedByAdminID != 65 || rows[0].ResolvedAt == nil {
		t.Fatalf("restart scan overwrote resolution: %#v", rows[0])
	}
	var count int64
	if err := db2.Model(&procurementdomain.TradeEvidenceConflict{}).Where("event_id = ?", original.EventID).Count(&count).Error; err != nil || count != 1 {
		t.Fatalf("restart conflict count=%d err=%v", count, err)
	}
}

func TestScanPendingReturnsConflictIsolationWriteFailure(t *testing.T) {
	db, _ := decisionTestDB(t, false, constants.ProcurementStatusAwaitingVerification)
	if err := db.AutoMigrate(&procurementdomain.TradeEvidenceConflict{}); err != nil {
		t.Fatal(err)
	}
	adapter := New(db)
	original := evidenceResponse("scan-isolation-failure", "TRADE-ORIG", "1.00")
	if err := adapter.ImportTradeResponseEvidence(original); err != nil {
		t.Fatal(err)
	}
	incoming := original
	incoming.Amount = "7.00"
	source := &confirmedEventSource{event: incoming}
	recovery, err := procurementapp.NewTradeResponseJournalRecovery(source, adapter)
	if err != nil {
		t.Fatal(err)
	}
	if err := db.Callback().Create().Before("gorm:create").Register("test:fail_conflict_isolation", func(tx *gorm.DB) {
		if tx.Statement.Table == "procurement_trade_evidence_conflicts" {
			tx.AddError(errors.New("injected conflict isolation write failure"))
		}
	}); err != nil {
		t.Fatal(err)
	}
	report, scanErr := recovery.ScanPending()
	if scanErr == nil || !strings.Contains(scanErr.Error(), "persist isolation record") || len(report.Failures) != 1 || report.Failures[0].EventID != original.EventID {
		t.Fatalf("isolation failure was not surfaced: report=%+v err=%v", report, scanErr)
	}
	var count int64
	if err := db.Model(&procurementdomain.TradeEvidenceConflict{}).Where("event_id = ?", original.EventID).Count(&count).Error; err != nil || count != 0 {
		t.Fatalf("failed isolation falsely persisted conflict count=%d err=%v", count, err)
	}
	var persisted procurementdomain.TradeResponseJournalEntry
	if err := db.Where("source_event_id = ?", original.EventID).First(&persisted).Error; err != nil {
		t.Fatal(err)
	}
	if persisted.Amount != original.Amount || persisted.UpstreamTradeNo != original.TradeNo {
		t.Fatalf("failed isolation changed Journal: %#v", persisted)
	}
}

type confirmedEventSource struct {
	event procurementcontract.EvidenceEvent
}

func (*confirmedEventSource) AppendIntent(procurementcontract.EvidenceEvent) error {
	return errors.New("read-only source")
}
func (*confirmedEventSource) AppendResponse(procurementcontract.EvidenceEvent) error {
	return errors.New("read-only source")
}
func (s *confirmedEventSource) GetByEventID(id string) (*procurementcontract.EvidenceEvent, error) {
	if id != s.event.EventID {
		return nil, nil
	}
	event := s.event
	return &event, nil
}
func (s *confirmedEventSource) GetByTraceID(trace string) ([]procurementcontract.EvidenceEvent, error) {
	if trace != s.event.TraceID {
		return nil, nil
	}
	return []procurementcontract.EvidenceEvent{s.event}, nil
}
func (s *confirmedEventSource) ListPending() ([]procurementcontract.EvidenceEvent, error) {
	return []procurementcontract.EvidenceEvent{s.event}, nil
}
func (*confirmedEventSource) IndependentlyConfirmed() bool { return true }

func TestResponseEvidenceRecoveryAfterDatabaseRestartDoesNotDuplicateJournal(t *testing.T) {
	dbPath := filepath.Join(t.TempDir(), "restart-journal.db")
	open := func() *gorm.DB {
		db, err := gorm.Open(sqlite.Open(dbPath), &gorm.Config{DisableForeignKeyConstraintWhenMigrating: true})
		if err != nil {
			t.Fatal(err)
		}
		if err := db.AutoMigrate(&procurementdomain.TradeResponseJournalEntry{}); err != nil {
			t.Fatal(err)
		}
		return db
	}
	event := evidenceResponse("restart-event", "TRADE-RESTART", "6.00")
	source := &confirmedEventSource{event: event}
	db := open()
	recovery, err := procurementapp.NewTradeResponseJournalRecovery(source, New(db))
	if err != nil {
		t.Fatal(err)
	}
	report, err := recovery.ScanPending()
	if err != nil || report.Imported != 1 {
		t.Fatalf("first scan report=%+v err=%v", report, err)
	}
	sqlDB, err := db.DB()
	if err != nil {
		t.Fatal(err)
	}
	if err := sqlDB.Close(); err != nil {
		t.Fatal(err)
	}

	// Reopening the SQLite file simulates a new process after the import committed but before any acknowledgement.
	db = open()
	sqlDB, err = db.DB()
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { _ = sqlDB.Close() })
	recovery, err = procurementapp.NewTradeResponseJournalRecovery(source, New(db))
	if err != nil {
		t.Fatal(err)
	}
	report, err = recovery.ScanPending()
	if err != nil || report.Imported != 1 {
		t.Fatalf("restart scan report=%+v err=%v", report, err)
	}
	var count int64
	if err := db.Model(&procurementdomain.TradeResponseJournalEntry{}).Where("source_event_id = ?", event.EventID).Count(&count).Error; err != nil || count != 1 {
		t.Fatalf("Journal rows=%d err=%v; want exactly one after restart", count, err)
	}
}

func TestConcurrentJournalImportsOfSameEventAreIdempotent(t *testing.T) {
	db, _ := decisionTestDB(t, false, constants.ProcurementStatusAwaitingVerification)
	adapter := New(db)
	event := evidenceResponse("concurrent-event", "TRADE-C", "3.00")
	start := make(chan struct{})
	errs := make(chan error, 2)
	var wg sync.WaitGroup
	for i := 0; i < 2; i++ {
		wg.Add(1)
		go func() { defer wg.Done(); <-start; errs <- adapter.ImportTradeResponseEvidence(event) }()
	}
	close(start)
	wg.Wait()
	close(errs)
	for err := range errs {
		if err != nil {
			t.Fatalf("concurrent import: %v", err)
		}
	}
	var count int64
	if err := db.Model(&procurementdomain.TradeResponseJournalEntry{}).Where("source_event_id = ?", event.EventID).Count(&count).Error; err != nil || count != 1 {
		t.Fatalf("rows=%d err=%v", count, err)
	}
}

func TestImportDoesNotOverwriteAdministratorConfirmedJournal(t *testing.T) {
	db, id := decisionTestDB(t, false, constants.ProcurementStatusAccepted)
	adapter := New(db)
	adminTrade, adminAmount := "ADMIN-TRADE", "5.00"
	if err := adapter.AppendTradeResponseJournal(procurementdomain.TradeResponseJournalEntry{ProcurementOrderID: id, TraceID: "stable-trace", UpstreamTradeNo: adminTrade, Amount: adminAmount, ConfirmedTradeNo: adminTrade, ConfirmedAmount: adminAmount, OperatorAdminID: ptrUint(9), ReconciliationNote: "admin verified", EventType: "manual_reconciliation_recorded", ResponseTime: time.Now().UTC()}); err != nil {
		t.Fatal(err)
	}
	if err := adapter.ImportTradeResponseEvidence(evidenceResponse("late-event", "WORKER-TRADE", "8.25")); err != nil {
		t.Fatal(err)
	}
	entries, err := adapter.ListTradeResponseJournalByTraceID("stable-trace")
	if err != nil || len(entries) != 2 {
		t.Fatalf("entries=%d err=%v", len(entries), err)
	}
	if entries[0].UpstreamTradeNo != adminTrade || entries[0].Amount != adminAmount || entries[0].OperatorAdminID == nil || *entries[0].OperatorAdminID != 9 {
		t.Fatalf("admin journal was changed: %#v", entries[0])
	}
}

func TestImportRejectsIntentAndUnavailableJournal(t *testing.T) {
	db, _ := decisionTestDB(t, false, constants.ProcurementStatusAwaitingVerification)
	adapter := New(db)
	intent := evidenceResponse("intent-event", "TRADE-I", "1.00")
	intent.EventType = "trade_intent"
	if err := adapter.ImportTradeResponseEvidence(intent); !errors.Is(err, procurementcontract.ErrInvalidTradeResponseEvidence) {
		t.Fatalf("intent import=%v", err)
	}
	if err := adapter.ImportTradeResponseEvidence(evidenceResponse("response-event", "TRADE-R", "2.00")); err != nil {
		t.Fatal(err)
	}
	sqlDB, err := db.DB()
	if err != nil {
		t.Fatal(err)
	}
	if err := sqlDB.Close(); err != nil {
		t.Fatal(err)
	}
	if err := adapter.ImportTradeResponseEvidence(evidenceResponse("db-down-event", "TRADE-D", "3.00")); err == nil {
		t.Fatal("closed Journal database must fail import")
	}
}

func TestJournalMigrationKeepsLegacyNullSourceIDsAndUniqueIndex(t *testing.T) {
	db, err := gorm.Open(sqlite.Open(filepath.Join(t.TempDir(), "legacy-journal.db")), &gorm.Config{DisableForeignKeyConstraintWhenMigrating: true})
	if err != nil {
		t.Fatal(err)
	}
	if err := db.Exec(`CREATE TABLE procurement_trade_response_journal (
		id INTEGER PRIMARY KEY AUTOINCREMENT,
		procurement_order_id INTEGER NOT NULL,
		trace_id VARCHAR(64) NOT NULL,
		upstream_trade_no VARCHAR(128) NOT NULL DEFAULT '',
		amount VARCHAR(64) NOT NULL DEFAULT '',
		response_time DATETIME NOT NULL,
		event_type VARCHAR(64) NOT NULL,
		created_at DATETIME NOT NULL,
		operator_admin_id INTEGER NULL,
		confirmed_trade_no VARCHAR(128) NOT NULL DEFAULT '',
		confirmed_amount VARCHAR(64) NOT NULL DEFAULT '',
		reconciliation_note TEXT NULL
	)`).Error; err != nil {
		t.Fatal(err)
	}
	legacyTime := time.Date(2026, 11, 17, 12, 0, 0, 0, time.UTC)
	for _, trace := range []string{"legacy-a", "legacy-b"} {
		if err := db.Exec(`INSERT INTO procurement_trade_response_journal (procurement_order_id, trace_id, response_time, event_type, created_at) VALUES (?, ?, ?, ?, ?)`, 1, trace, legacyTime, "manual_reconciliation_recorded", legacyTime).Error; err != nil {
			t.Fatal(err)
		}
	}
	if err := db.AutoMigrate(&procurementdomain.TradeResponseJournalEntry{}); err != nil {
		t.Fatalf("upgrade legacy Journal schema: %v", err)
	}
	sqlDB, err := db.DB()
	if err != nil {
		t.Fatal(err)
	}
	sqlDB.SetMaxOpenConns(1)
	t.Cleanup(func() { _ = sqlDB.Close() })
	if !db.Migrator().HasColumn(&procurementdomain.TradeResponseJournalEntry{}, "SourceEventID") || !db.Migrator().HasColumn(&procurementdomain.TradeResponseJournalEntry{}, "SourceEventHash") {
		t.Fatal("nullable provenance columns were not migrated")
	}
	if !db.Migrator().HasIndex(&procurementdomain.TradeResponseJournalEntry{}, "ux_procurement_trade_journal_source_event_id") {
		t.Fatal("source EventID unique index is missing")
	}
	var legacy []procurementdomain.TradeResponseJournalEntry
	if err := db.Order("id ASC").Find(&legacy).Error; err != nil || len(legacy) != 2 || legacy[0].SourceEventID != nil || legacy[1].SourceEventID != nil {
		t.Fatalf("legacy Journal rows were not preserved with NULL source IDs: rows=%#v err=%v", legacy, err)
	}
	for _, eventType := range []string{"manual_reconciliation_recorded", "late_response_same_trade_no"} {
		if err := New(db).AppendTradeResponseJournal(procurementdomain.TradeResponseJournalEntry{ProcurementOrderID: 1, TraceID: "legacy", EventType: eventType, ResponseTime: time.Now().UTC()}); err != nil {
			t.Fatalf("legacy NULL EventID insert: %v", err)
		}
	}
	if err := db.AutoMigrate(&procurementdomain.TradeResponseJournalEntry{}); err != nil {
		t.Fatalf("repeat Journal migration: %v", err)
	}
}

func TestPostgresImportUsesSourceEventConflictTarget(t *testing.T) {
	db, err := gorm.Open(postgres.New(postgres.Config{DSN: "host=127.0.0.1 user=test dbname=test sslmode=disable"}), &gorm.Config{DryRun: true, DisableAutomaticPing: true, SkipDefaultTransaction: true})
	if err != nil {
		t.Fatal(err)
	}
	event := evidenceResponse("pg-event", "TRADE-PG", "4.00")
	entry, err := procurementcontract.TradeResponseJournalEntryFromEvidence(event)
	if err != nil {
		t.Fatal(err)
	}
	stmt := db.Clauses(clause.OnConflict{Columns: []clause.Column{{Name: "source_event_id"}}, DoNothing: true}).Create(&entry).Statement
	if !strings.Contains(stmt.SQL.String(), `ON CONFLICT ("source_event_id") DO NOTHING`) {
		t.Fatalf("PostgreSQL insert SQL=%s", stmt.SQL.String())
	}
}

func ptrUint(value uint) *uint { return &value }

func TestTradeResponseReconciliationPersistsOperator(t *testing.T) {
	db, _ := decisionTestDB(t, false, constants.ProcurementStatusAwaitingVerification)
	adapter := New(db)
	if err := adapter.RecordTradeResponseReconciliation(12, "stable-trace", "CONFIRMED-1", "6.25", "checked supplier record", 42, time.Now()); err != nil {
		t.Fatal(err)
	}
	entries, err := adapter.ListTradeResponseJournalByTraceID("stable-trace")
	if err != nil || len(entries) != 1 {
		t.Fatalf("journal entries=%d error=%v", len(entries), err)
	}
	if entries[0].OperatorAdminID == nil || *entries[0].OperatorAdminID != 42 || entries[0].EventType != "manual_reconciliation_recorded" {
		t.Fatalf("reconciliation journal does not identify operator: %#v", entries[0])
	}
}

func TestManualDecisionCASOnlyAllowsAwaitingVerification(t *testing.T) {
	for _, status := range []string{constants.ProcurementStatusSubmitting, constants.ProcurementStatusPending, constants.ProcurementStatusAccepted, constants.ProcurementStatusFulfilled, constants.ProcurementStatusCanceled, constants.ProcurementStatusAwaitingVerification} {
		t.Run(status, func(t *testing.T) {
			db, id := decisionTestDB(t, true, status)
			updated, err := New(db).ResolveProcurementDecision(id, "stable-request-no", "retry_authorized_after_no_charge", "confirmed no charge and no order", "", 7, constants.ProcurementStatusPending, map[string]interface{}{"error_message": "manual retry authorized"})
			if err != nil {
				t.Fatal(err)
			}
			want := status == constants.ProcurementStatusAwaitingVerification
			if updated != want {
				t.Fatalf("updated=%v, want %v", updated, want)
			}
			var order procurementdomain.Order
			if err := db.First(&order, id).Error; err != nil {
				t.Fatal(err)
			}
			if order.TraceID != "stable-request-no" || (want && order.Status != constants.ProcurementStatusPending) || (!want && order.Status != status) {
				t.Fatalf("unexpected order status/trace: %q/%q", order.Status, order.TraceID)
			}
			var count int64
			if err := db.Model(&auditlogdomain.AuthzAuditLog{}).Count(&count).Error; err != nil || count != map[bool]int64{true: 1, false: 0}[want] {
				t.Fatalf("audit count=%d error=%v", count, err)
			}
			if want {
				var audit auditlogdomain.AuthzAuditLog
				if err := db.First(&audit).Error; err != nil {
					t.Fatal(err)
				}
				if audit.RequestID != order.TraceID || audit.OperatorAdminID != 7 || audit.DetailJSON["confirmed_no_charge"] != true || audit.DetailJSON["confirmed_no_order"] != true {
					t.Fatalf("audit lacks confirmed decision or stable trace: %#v", audit)
				}
			}
		})
	}
}

func TestManualDecisionConcurrentAdministratorsOnlyOneWins(t *testing.T) {
	db, id := decisionTestDB(t, true, constants.ProcurementStatusAwaitingVerification)
	adapter := New(db)
	var wg sync.WaitGroup
	results := make(chan bool, 2)
	errors := make(chan error, 2)
	start := make(chan struct{})
	for operator := uint(1); operator <= 2; operator++ {
		wg.Add(1)
		go func(operator uint) {
			defer wg.Done()
			<-start
			updated, err := adapter.ResolveProcurementDecision(id, "stable-request-no", "retry_authorized_after_no_charge", "confirmed no charge and no order", "", operator, constants.ProcurementStatusPending, nil)
			results <- updated
			errors <- err
		}(operator)
	}
	close(start)
	wg.Wait()
	close(results)
	close(errors)
	wins := 0
	for err := range errors {
		if err != nil {
			t.Fatal(err)
		}
	}
	for updated := range results {
		if updated {
			wins++
		}
	}
	var audits int64
	if err := db.Model(&auditlogdomain.AuthzAuditLog{}).Count(&audits).Error; err != nil {
		t.Fatal(err)
	}
	if wins != 1 || audits != 1 {
		t.Fatalf("winners=%d audits=%d, want one of each", wins, audits)
	}
}

func TestManualDecisionAuditFailureRollsBackAuthorization(t *testing.T) {
	db, id := decisionTestDB(t, false, constants.ProcurementStatusAwaitingVerification)
	updated, err := New(db).ResolveProcurementDecision(id, "stable-request-no", "retry_authorized_after_no_charge", "confirmed no charge and no order", "", 7, constants.ProcurementStatusPending, nil)
	if err == nil || updated {
		t.Fatalf("audit failure returned updated=%v err=%v", updated, err)
	}
	var order procurementdomain.Order
	if err := db.First(&order, id).Error; err != nil {
		t.Fatal(err)
	}
	if order.Status != constants.ProcurementStatusAwaitingVerification || order.TraceID != "stable-request-no" {
		t.Fatalf("failed audit changed order to %q/%q", order.Status, order.TraceID)
	}
}

func TestManualDecisionStateWriteFailureDoesNotCreateAudit(t *testing.T) {
	db, id := decisionTestDB(t, true, constants.ProcurementStatusAwaitingVerification)
	if err := db.Exec(`CREATE TRIGGER fail_retry_authorization BEFORE UPDATE ON procurement_orders WHEN NEW.status = 'pending' BEGIN SELECT RAISE(FAIL, 'mock state write failure'); END`).Error; err != nil {
		t.Fatal(err)
	}
	updated, err := New(db).ResolveProcurementDecision(id, "stable-request-no", "retry_authorized_after_no_charge", "confirmed no charge and no order", "", 7, constants.ProcurementStatusPending, nil)
	if err == nil || updated {
		t.Fatalf("state write failure returned updated=%v err=%v", updated, err)
	}
	var order procurementdomain.Order
	if err := db.First(&order, id).Error; err != nil {
		t.Fatal(err)
	}
	var audits int64
	if err := db.Model(&auditlogdomain.AuthzAuditLog{}).Count(&audits).Error; err != nil {
		t.Fatal(err)
	}
	if order.Status != constants.ProcurementStatusAwaitingVerification || audits != 0 {
		t.Fatalf("failed state write left status=%q audits=%d", order.Status, audits)
	}
}

func TestRecordProcurementDecisionPersistsDecisionDetails(t *testing.T) {
	db, err := gorm.Open(sqlite.Open("file:procurement_audit_test?mode=memory&cache=shared"), &gorm.Config{})
	if err != nil {
		t.Fatal(err)
	}
	if err := db.AutoMigrate(&auditlogdomain.AuthzAuditLog{}); err != nil {
		t.Fatal(err)
	}
	adapter := New(db)
	if err := adapter.RecordProcurementDecision(9, 12, "stable-trace", "retry_authorized_after_no_charge", "supplier confirmed no debit", ""); err != nil {
		t.Fatal(err)
	}
	var entry auditlogdomain.AuthzAuditLog
	if err := db.First(&entry).Error; err != nil {
		t.Fatal(err)
	}
	if entry.OperatorAdminID != 9 || entry.Object != "procurement_orders/12" || entry.RequestID != "stable-trace" || entry.Action != "retry_authorized_after_no_charge" {
		t.Fatalf("persisted audit metadata = %#v", entry)
	}
	if entry.DetailJSON["reason"] != "supplier confirmed no debit" || entry.DetailJSON["decision"] != "retry_authorized_after_no_charge" {
		t.Fatalf("persisted audit details = %#v", entry.DetailJSON)
	}
}

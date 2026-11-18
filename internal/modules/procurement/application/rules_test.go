package application

import (
	"errors"
	"strings"
	"testing"
	"time"

	"github.com/dujiao-next/internal/constants"
	procurementcontract "github.com/dujiao-next/internal/modules/procurement/contract"
	procurementdomain "github.com/dujiao-next/internal/modules/procurement/domain"
	"github.com/dujiao-next/internal/shared/jsonmap"
	"github.com/dujiao-next/internal/shared/money"
	"github.com/shopspring/decimal"
)

type recoveryRepository struct {
	procurementcontract.Repository
	order *procurementdomain.Order
	err   error
	reads *int
}

func (r recoveryRepository) GetByID(uint) (*procurementdomain.Order, error) {
	if r.reads != nil {
		*r.reads++
	}
	return r.order, r.err
}

type recoveryAudit struct {
	procurementcontract.ManualDecisionAudit
	events []procurementdomain.TradeResponseJournalEntry
	err    error
	reads  *int
}

type recoveryEvidenceStore struct {
	events  map[string]procurementcontract.EvidenceEvent
	listErr error
}

func (s *recoveryEvidenceStore) AppendIntent(e procurementcontract.EvidenceEvent) error {
	s.events[e.EventID] = e
	return nil
}
func (s *recoveryEvidenceStore) AppendResponse(e procurementcontract.EvidenceEvent) error {
	s.events[e.EventID] = e
	return nil
}
func (s *recoveryEvidenceStore) GetByEventID(id string) (*procurementcontract.EvidenceEvent, error) {
	e, ok := s.events[id]
	if !ok {
		return nil, nil
	}
	return &e, nil
}
func (s *recoveryEvidenceStore) GetByTraceID(trace string) ([]procurementcontract.EvidenceEvent, error) {
	var out []procurementcontract.EvidenceEvent
	for _, e := range s.events {
		if e.TraceID == trace {
			out = append(out, e)
		}
	}
	return out, nil
}
func (s *recoveryEvidenceStore) ListPending() ([]procurementcontract.EvidenceEvent, error) {
	if s.listErr != nil {
		return nil, s.listErr
	}
	var out []procurementcontract.EvidenceEvent
	for _, e := range s.events {
		out = append(out, e)
	}
	return out, nil
}
func (*recoveryEvidenceStore) IndependentlyConfirmed() bool { return true }

type recoveryJournalImporter struct {
	entries map[string]procurementdomain.TradeResponseJournalEntry
	fail    error
	calls   int
}

func (i *recoveryJournalImporter) ImportTradeResponseEvidence(e procurementcontract.EvidenceEvent) error {
	i.calls++
	if i.fail != nil {
		return i.fail
	}
	entry, err := procurementcontract.TradeResponseJournalEntryFromEvidence(e)
	if err != nil {
		return err
	}
	if old, ok := i.entries[e.EventID]; ok {
		if old.SourceEventHash == nil || entry.SourceEventHash == nil || *old.SourceEventHash != *entry.SourceEventHash {
			return procurementcontract.ErrEvidenceJournalConflict
		}
		return nil
	}
	i.entries[e.EventID] = entry
	return nil
}

func recoveryResponse(id, kind string) procurementcontract.EvidenceEvent {
	return procurementcontract.EvidenceEvent{EventID: id, ProcurementOrderID: 41, TraceID: "stable-trace", EventType: kind, TradeNo: "TRADE-41", Amount: "8.25", ResponseAt: time.Date(2026, 11, 17, 12, 0, 0, 0, time.UTC), SchemaVersion: 1, Supplier: "faka", RelatedEventID: "intent-41"}
}

func TestRecoveryScansOnlyTradeResponsesAndIsRestartIdempotent(t *testing.T) {
	source := &recoveryEvidenceStore{events: map[string]procurementcontract.EvidenceEvent{"intent": recoveryResponse("intent", "trade_intent"), "response": recoveryResponse("response", "trade_response")}}
	importer := &recoveryJournalImporter{entries: map[string]procurementdomain.TradeResponseJournalEntry{}}
	first, err := NewTradeResponseJournalRecovery(source, importer)
	if err != nil {
		t.Fatal(err)
	}
	report, err := first.ScanPending()
	if err != nil || report.Imported != 1 || report.Skipped != 1 {
		t.Fatalf("report=%+v err=%v", report, err)
	}
	// Restart leaves the same immutable S3 evidence available for idempotent replay.
	second, err := NewTradeResponseJournalRecovery(source, importer)
	if err != nil {
		t.Fatal(err)
	}
	report, err = second.ScanPending()
	if err != nil || report.Imported != 1 || len(importer.entries) != 1 {
		t.Fatalf("restart report=%+v entries=%d err=%v", report, len(importer.entries), err)
	}
	if importer.calls != 2 {
		t.Fatalf("import calls=%d, want one attempt per scan", importer.calls)
	}
}

func TestRecoveryByEventIDRejectsIntentAndUnknown(t *testing.T) {
	source := &recoveryEvidenceStore{events: map[string]procurementcontract.EvidenceEvent{"intent": recoveryResponse("intent", "trade_intent")}}
	importer := &recoveryJournalImporter{entries: map[string]procurementdomain.TradeResponseJournalEntry{}}
	recovery, err := NewTradeResponseJournalRecovery(source, importer)
	if err != nil {
		t.Fatal(err)
	}
	if err := recovery.ImportByEventID("intent"); err == nil {
		t.Fatal("intent must not enter response journal")
	}
	if err := recovery.ImportByEventID("missing"); err == nil {
		t.Fatal("missing evidence must fail")
	}
	if importer.calls != 0 {
		t.Fatalf("import calls=%d; invalid source must not reach Journal", importer.calls)
	}
}

func TestRecoveryReportsDatabaseFailureAndRetriesLater(t *testing.T) {
	dbErr := errors.New("journal database unavailable")
	source := &recoveryEvidenceStore{events: map[string]procurementcontract.EvidenceEvent{"response": recoveryResponse("response", "trade_response")}}
	importer := &recoveryJournalImporter{entries: map[string]procurementdomain.TradeResponseJournalEntry{}, fail: dbErr}
	recovery, err := NewTradeResponseJournalRecovery(source, importer)
	if err != nil {
		t.Fatal(err)
	}
	report, err := recovery.ScanPending()
	if !errors.Is(err, dbErr) || len(report.Failures) != 1 {
		t.Fatalf("report=%+v err=%v", report, err)
	}
	importer.fail = nil
	report, err = recovery.ScanPending()
	if err != nil || report.Imported != 1 {
		t.Fatalf("retry report=%+v err=%v", report, err)
	}
}

func TestRecoveryRejectsSameEventIDDifferentContent(t *testing.T) {
	store := &recoveryEvidenceStore{events: map[string]procurementcontract.EvidenceEvent{"response": recoveryResponse("response", "trade_response")}}
	importer := &recoveryJournalImporter{entries: map[string]procurementdomain.TradeResponseJournalEntry{}}
	recovery, err := NewTradeResponseJournalRecovery(store, importer)
	if err != nil {
		t.Fatal(err)
	}
	if err := recovery.ImportByEventID("response"); err != nil {
		t.Fatal(err)
	}
	changed := recoveryResponse("response", "trade_response")
	changed.Supplier = "other"
	store.events["response"] = changed
	if err := recovery.ImportByEventID("response"); !errors.Is(err, procurementcontract.ErrEvidenceJournalConflict) {
		t.Fatalf("conflict=%v", err)
	}
	if importer.entries["response"].UpstreamTradeNo != "TRADE-41" {
		t.Fatal("conflict changed imported Journal evidence")
	}
}

func (a recoveryAudit) ListTradeResponseJournalByTraceID(string) ([]procurementdomain.TradeResponseJournalEntry, error) {
	if a.reads != nil {
		*a.reads++
	}
	return a.events, a.err
}

func TestReportFakaTradeCASRecoveryKeepsAmbiguousOrConflictingStateUntouched(t *testing.T) {
	order := &procurementdomain.Order{ID: 9, TraceID: "trace-9", Status: constants.ProcurementStatusAwaitingVerification}
	response := &procurementcontract.CreateOrderResult{OrderNo: "TRADE-9", Amount: "8.25"}
	event := procurementdomain.TradeResponseJournalEntry{ProcurementOrderID: 9, TraceID: "trace-9", UpstreamTradeNo: "TRADE-9", Amount: "8.25", EventType: "trade_success_response"}
	casErr := errors.New("ambiguous database write")
	tests := []struct {
		name       string
		order      *procurementdomain.Order
		response   *procurementcontract.CreateOrderResult
		events     []procurementdomain.TradeResponseJournalEntry
		journalErr error
		freezeOK   bool
		freezeErr  error
		want       string
	}{
		{name: "evidence ready while frozen", order: order, events: []procurementdomain.TradeResponseJournalEntry{event}, freezeOK: true, want: "evidence ready for administrator review"},
		{name: "freeze failed remains submitting", order: &procurementdomain.Order{ID: 9, TraceID: "trace-9", Status: constants.ProcurementStatusSubmitting}, events: []procurementdomain.TradeResponseJournalEntry{event}, freezeErr: errors.New("freeze write failed"), want: "freeze_succeeded=false"},
		{name: "journal read failed", order: order, journalErr: errors.New("journal unavailable"), freezeOK: true, want: "read journal"},
		{name: "journal amount mismatch", order: order, events: []procurementdomain.TradeResponseJournalEntry{{ProcurementOrderID: 9, TraceID: "trace-9", UpstreamTradeNo: "TRADE-9", Amount: "99.00", EventType: "trade_success_response"}}, freezeOK: true, want: "recovery conflict"},
		{name: "journal trade number mismatch", order: order, events: []procurementdomain.TradeResponseJournalEntry{{ProcurementOrderID: 9, TraceID: "trace-9", UpstreamTradeNo: "OTHER-TRADE", Amount: "8.25", EventType: "trade_success_response"}}, freezeOK: true, want: "recovery conflict"},
		{name: "multiple success events are ambiguous", order: order, events: []procurementdomain.TradeResponseJournalEntry{event, event}, freezeOK: true, want: "found 2 matching durable success events"},
		{name: "trace linked to another procurement order", order: order, events: []procurementdomain.TradeResponseJournalEntry{{ProcurementOrderID: 10, TraceID: "trace-9", EventType: "trade_success_response"}}, freezeOK: true, want: "also associated with procurement order 10"},
		{name: "CAS error but accepted state already committed", order: &procurementdomain.Order{ID: 9, TraceID: "trace-9", Status: constants.ProcurementStatusAccepted, UpstreamOrderNo: "TRADE-9", UpstreamAmount: money.FromDecimal(decimal.RequireFromString("8.25"))}, events: []procurementdomain.TradeResponseJournalEntry{event}, freezeOK: false, want: "already contains matching accepted state"},
		{name: "stored money precision is normalized", order: &procurementdomain.Order{ID: 9, TraceID: "trace-9", Status: constants.ProcurementStatusAccepted, UpstreamOrderNo: "TRADE-9", UpstreamAmount: money.FromDecimal(decimal.RequireFromString("8.25"))}, response: &procurementcontract.CreateOrderResult{OrderNo: "TRADE-9", Amount: "8.250"}, events: []procurementdomain.TradeResponseJournalEntry{{ProcurementOrderID: 9, TraceID: "trace-9", UpstreamTradeNo: "TRADE-9", Amount: "8.250", EventType: "trade_success_response"}}, freezeOK: false, want: "already contains matching accepted state"},
		{name: "admin confirmed different trade is preserved", order: &procurementdomain.Order{ID: 9, TraceID: "trace-9", Status: constants.ProcurementStatusAccepted, UpstreamOrderNo: "ADMIN-TRADE", UpstreamAmount: order.UpstreamAmount}, events: []procurementdomain.TradeResponseJournalEntry{event}, freezeOK: false, want: "administrator/current data preserved"},
		{name: "procurement TraceID changed", order: &procurementdomain.Order{ID: 9, TraceID: "other-trace", Status: constants.ProcurementStatusAwaitingVerification}, events: []procurementdomain.TradeResponseJournalEntry{event}, freezeOK: true, want: "TraceID changed"},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			beforeStatus, beforeTrade, beforeAmount := tt.order.Status, tt.order.UpstreamOrderNo, tt.order.UpstreamAmount.String()
			if tt.response == nil {
				tt.response = response
			}
			svc := &Service{procRepo: recoveryRepository{order: tt.order}, decisionAudit: recoveryAudit{events: tt.events, err: tt.journalErr}}
			err := svc.reportFakaTradeCASRecovery(&procurementdomain.Order{ID: 9, TraceID: "trace-9"}, tt.response, casErr, tt.freezeOK, tt.freezeErr)
			if err == nil || !strings.Contains(err.Error(), tt.want) || !errors.Is(err, casErr) {
				t.Fatalf("recovery error=%v; want %q and original CAS error", err, tt.want)
			}
			if tt.order.Status != beforeStatus || tt.order.UpstreamOrderNo != beforeTrade || tt.order.UpstreamAmount.String() != beforeAmount {
				t.Fatalf("administrator state was modified: %#v", tt.order)
			}
		})
	}
}

func TestReportFakaTradeCASRecoveryCanRepeatWithoutMutation(t *testing.T) {
	order := &procurementdomain.Order{ID: 9, TraceID: "trace-9", Status: constants.ProcurementStatusAwaitingVerification}
	event := procurementdomain.TradeResponseJournalEntry{ID: 77, ProcurementOrderID: 9, TraceID: "trace-9", UpstreamTradeNo: "TRADE-9", Amount: "8.25", EventType: "trade_success_response"}
	procurementReads, journalReads := 0, 0
	svc := &Service{procRepo: recoveryRepository{order: order, reads: &procurementReads}, decisionAudit: recoveryAudit{events: []procurementdomain.TradeResponseJournalEntry{event}, reads: &journalReads}}
	originalEvents := append([]procurementdomain.TradeResponseJournalEntry(nil), []procurementdomain.TradeResponseJournalEntry{event}...)
	for i := 0; i < 2; i++ {
		err := svc.reportFakaTradeCASRecovery(&procurementdomain.Order{ID: 9, TraceID: "trace-9"}, &procurementcontract.CreateOrderResult{OrderNo: "TRADE-9", Amount: "8.25"}, errors.New("injected CAS error"), true, nil)
		if err == nil || !strings.Contains(err.Error(), "evidence ready for administrator review") {
			t.Fatalf("run %d: unexpected recovery result: %v", i+1, err)
		}
	}
	if procurementReads != 2 || journalReads != 2 {
		t.Fatalf("reads: procurement=%d journal=%d", procurementReads, journalReads)
	}
	if order.Status != constants.ProcurementStatusAwaitingVerification || order.UpstreamOrderNo != "" {
		t.Fatalf("recovery mutated procurement: %#v", order)
	}
	if len(originalEvents) != 1 || originalEvents[0] != event {
		t.Fatalf("recovery mutated original journal snapshot: %#v", originalEvents)
	}
}

// ── Unit tests for pure functions ──

func TestParseRetryIntervals(t *testing.T) {
	tests := []struct {
		name     string
		input    string
		expected []time.Duration
	}{
		{
			name:     "empty string returns defaults",
			input:    "",
			expected: []time.Duration{30 * time.Second, 60 * time.Second, 300 * time.Second},
		},
		{
			name:     "valid array",
			input:    "[10,20,30]",
			expected: []time.Duration{10 * time.Second, 20 * time.Second, 30 * time.Second},
		},
		{
			name:     "with spaces",
			input:    "[ 10 , 20 , 30 ]",
			expected: []time.Duration{10 * time.Second, 20 * time.Second, 30 * time.Second},
		},
		{
			name:     "invalid entries skipped",
			input:    "[10,abc,30]",
			expected: []time.Duration{10 * time.Second, 30 * time.Second},
		},
		{
			name:     "all invalid returns defaults",
			input:    "[abc,def]",
			expected: []time.Duration{30 * time.Second, 60 * time.Second, 300 * time.Second},
		},
		{
			name:     "negative values skipped",
			input:    "[10,-5,30]",
			expected: []time.Duration{10 * time.Second, 30 * time.Second},
		},
		{
			name:     "zero values skipped",
			input:    "[0,10]",
			expected: []time.Duration{10 * time.Second},
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			result := parseRetryIntervals(tt.input)
			if len(result) != len(tt.expected) {
				t.Fatalf("expected %d intervals, got %d: %v", len(tt.expected), len(result), result)
			}
			for i, d := range result {
				if d != tt.expected[i] {
					t.Errorf("interval[%d]: expected %v, got %v", i, tt.expected[i], d)
				}
			}
		})
	}
}

func TestBuildUpstreamRefundRecords_SortsByCreatedAtAscAndRenumbersID(t *testing.T) {
	records := []jsonmap.JSON{
		{
			"id":         99,
			"type":       "wallet",
			"amount":     "20.00",
			"created_at": "2026-04-12T10:00:00Z",
		},
		{
			"id":         100,
			"type":       "wallet",
			"amount":     "10.00",
			"created_at": "2026-04-12T09:00:00Z",
		},
	}

	got := buildUpstreamRefundRecords(records)
	if len(got) != 2 {
		t.Fatalf("expected 2 records, got %d", len(got))
	}
	if amount, _ := got[0]["amount"].(string); amount != "10.00" {
		t.Fatalf("expected first amount 10.00, got %#v", got[0]["amount"])
	}
	if amount, _ := got[1]["amount"].(string); amount != "20.00" {
		t.Fatalf("expected second amount 20.00, got %#v", got[1]["amount"])
	}
	if id, ok := got[0]["id"].(int); !ok || id != 1 {
		t.Fatalf("expected first id 1, got %#v", got[0]["id"])
	}
	if id, ok := got[1]["id"].(int); !ok || id != 2 {
		t.Fatalf("expected second id 2, got %#v", got[1]["id"])
	}
}

func TestIsRetryableErrorCode(t *testing.T) {
	nonRetryable := []string{
		"insufficient_balance",
		"payment_failed",
		"product_unavailable",
		"sku_unavailable",
		"invalid_request",
		"unauthorized",
		"forbidden",
		"duplicate_order",
		"product_out_of_stock",
	}
	for _, code := range nonRetryable {
		if isRetryableErrorCode(code) {
			t.Errorf("expected %q to be non-retryable", code)
		}
	}

	retryable := []string{
		"server_error",
		"timeout",
		"network_error",
		"unknown_error",
		"",
	}
	for _, code := range retryable {
		if !isRetryableErrorCode(code) {
			t.Errorf("expected %q to be retryable", code)
		}
	}

	// 测试带空格的情况
	if isRetryableErrorCode("  unauthorized  ") {
		t.Error("expected trimmed 'unauthorized' to be non-retryable")
	}
}

func TestIsUpstreamTransitionAllowed(t *testing.T) {
	for _, tc := range []struct {
		current, upstream string
		want              bool
	}{
		{constants.ProcurementStatusAccepted, "delivered", true},
		{constants.ProcurementStatusPartiallyRefunded, "delivered", true},
		{constants.ProcurementStatusFulfilled, "delivered", false},
		{constants.ProcurementStatusRefunded, "delivered", false},
		{constants.ProcurementStatusCanceled, "delivered", false},
		{constants.ProcurementStatusFulfilled, "canceled", false},
		{constants.ProcurementStatusAccepted, "canceled", true},
		{constants.ProcurementStatusAccepted, "partially_refunded", true},
		{constants.ProcurementStatusFulfilled, "refunded", true},
	} {
		if got := isUpstreamTransitionAllowed(tc.current, tc.upstream); got != tc.want {
			t.Errorf("(%s -> %s) = %v, want %v", tc.current, tc.upstream, got, tc.want)
		}
	}
}

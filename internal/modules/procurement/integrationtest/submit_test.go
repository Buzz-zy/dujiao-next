package procurement_test

import (
	"encoding/json"
	"errors"
	"fmt"
	"net/http"
	"net/http/httptest"
	"strings"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	procurementaudit "github.com/dujiao-next/internal/bootstrap/procurementaudit"
	orderdomain "github.com/dujiao-next/internal/modules/order/domain"
	procurementapp "github.com/dujiao-next/internal/modules/procurement/application"

	mappingdomain "github.com/dujiao-next/internal/modules/catalog/mapping/domain"

	"github.com/dujiao-next/internal/constants"
	procurementcontract "github.com/dujiao-next/internal/modules/procurement/contract"
	procurementdomain "github.com/dujiao-next/internal/modules/procurement/domain"
	procs3 "github.com/dujiao-next/internal/modules/procurement/infrastructure/evidencestore/s3"
	procurementgormstore "github.com/dujiao-next/internal/modules/procurement/infrastructure/gormstore"
	siteconnectionapp "github.com/dujiao-next/internal/modules/siteconnection/application"
	"github.com/dujiao-next/internal/shared/jsonmap"
)

// ── SubmitToUpstream tests ──

func TestSubmitToUpstream_Success(t *testing.T) {
	db := setupProcurementTestDB(t)

	order := createProcTestOrder(t, db, "PROC-SUBMIT-001", constants.OrderStatusPaid, constants.FulfillmentTypeUpstream)
	// 创建 product mapping 和 sku mapping
	pm := &mappingdomain.Mapping{
		ConnectionID:      1,
		LocalProductID:    1,
		UpstreamProductID: 101,
		IsActive:          true,
	}
	db.Create(pm)
	sm := &mappingdomain.SKUMapping{
		ProductMappingID: pm.ID,
		LocalSKUID:       1,
		UpstreamSKUID:    201,
		UpstreamIsActive: true,
	}
	db.Create(sm)

	// mock upstream server
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		w.Header().Set("Content-Type", "application/json")
		json.NewEncoder(w).Encode(map[string]any{
			"ok":       true,
			"order_id": 999,
			"order_no": "UP-999",
			"status":   "accepted",
			"amount":   "50.00",
			"currency": "CNY",
		})
	}))
	defer server.Close()

	connSvc := newTestSiteConnectionService(db, "test-key", t.TempDir())
	conn, err := connSvc.Create(siteconnectionapp.CreateInput{
		Name:      "test-upstream",
		BaseURL:   server.URL,
		ApiKey:    "key",
		ApiSecret: "secret",
		Protocol:  constants.ConnectionProtocolDujiaoNext,
	})
	if err != nil {
		t.Fatalf("create connection: %v", err)
	}

	proc := createTestProcurementOrder(t, db, conn.ID, order.ID, order.OrderNo, "pending")

	svc := newTestProcurementService(db, connSvc)

	if err := svc.SubmitToUpstream(proc.ID); err != nil {
		t.Fatalf("SubmitToUpstream: %v", err)
	}

	// 验证采购单状态 = accepted
	var updatedProc ProcurementOrder
	db.First(&updatedProc, proc.ID)
	if updatedProc.Status != "accepted" {
		t.Errorf("expected procurement status 'accepted', got %q", updatedProc.Status)
	}
	if updatedProc.UpstreamOrderID != 999 {
		t.Errorf("expected upstream_order_id=999, got %d", updatedProc.UpstreamOrderID)
	}

	// 验证本地订单状态 = fulfilling
	var updatedOrder orderdomain.Order
	db.First(&updatedOrder, order.ID)
	if updatedOrder.Status != constants.OrderStatusFulfilling {
		t.Errorf("expected order status %q, got %q", constants.OrderStatusFulfilling, updatedOrder.Status)
	}
}

func TestFakaAdmissionClaimsOnceAndTradesOnce(t *testing.T) {
	db := setupProcurementTestDB(t)
	order := createProcTestOrder(t, db, "PROC-FAKA-FROZEN-001", constants.OrderStatusPaid, constants.FulfillmentTypeUpstream)
	pm := &mappingdomain.Mapping{ConnectionID: 1, LocalProductID: 1, UpstreamProductID: 101, UpstreamProductCode: "persisted-code", IsActive: true, UpstreamStatus: mappingdomain.UpstreamStatusActive}
	db.Create(pm)
	db.Create(&mappingdomain.SKUMapping{ProductMappingID: pm.ID, LocalSKUID: 1, UpstreamSKUID: 201, UpstreamIsActive: true})
	var listCalls, detailCalls, tradeCalls atomic.Int32
	var procurementID uint
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		w.Header().Set("Content-Type", "application/json")
		switch r.URL.Path {
		case "/shared/commodity/items":
			listCalls.Add(1)
			_, _ = w.Write([]byte(`{"code":200,"data":[{"id":2,"children":[{"id":101,"code":"persisted-code","name":"Simple"}]}]}`))
		case "/shared/commodity/item":
			detailCalls.Add(1)
			var inFlight ProcurementOrder
			if err := db.First(&inFlight, procurementID).Error; err != nil || inFlight.Status != constants.ProcurementStatusSubmitting {
				t.Errorf("live verification observed procurement state %q, err=%v; want submitting", inFlight.Status, err)
			}
			_, _ = w.Write([]byte(`{"code":200,"data":{"id":101,"code":"persisted-code","name":"Simple","config":{},"draft_status":0,"password_status":0,"widget":null,"minimum":1,"maximum":4,"delivery_way":1}}`))
		case "/shared/commodity/trade":
			tradeCalls.Add(1)
			if err := r.ParseForm(); err != nil {
				t.Error(err)
			}
			if r.Form.Get("request_no") != "test-trace-id" || r.Form.Get("shared_code") != "persisted-code" || r.Form.Get("num") != "1" || r.Form.Get("app_key") != "" {
				t.Errorf("unsafe trade form: %v", r.Form)
			}
			_, _ = w.Write([]byte(`{"code":200,"data":{"tradeNo":"Faka-TR-1","amount":"8.25","secret":null,"stock":"3"}}`))
		default:
			t.Errorf("unexpected Faka endpoint %s", r.URL.Path)
			w.WriteHeader(http.StatusNotFound)
		}
	}))
	defer server.Close()
	connSvc := newTestSiteConnectionService(db, "test-key", t.TempDir())
	conn, err := connSvc.Create(siteconnectionapp.CreateInput{Name: "faka", BaseURL: server.URL, ApiKey: "app-id", ApiSecret: "app-key", Protocol: constants.ConnectionProtocolFaka})
	if err != nil {
		t.Fatal(err)
	}
	proc := createTestProcurementOrder(t, db, conn.ID, order.ID, order.OrderNo, constants.ProcurementStatusPending)
	procurementID = proc.ID
	svc := newTestProcurementService(db, connSvc, procurementaudit.New(db))
	start := make(chan struct{})
	var wg sync.WaitGroup
	for i := 0; i < 2; i++ {
		wg.Add(1)
		go func() { defer wg.Done(); <-start; _ = svc.SubmitToUpstream(proc.ID) }()
	}
	close(start)
	wg.Wait()
	var updated ProcurementOrder
	if err := db.First(&updated, proc.ID).Error; err != nil {
		t.Fatal(err)
	}
	if updated.Status != constants.ProcurementStatusAccepted || updated.TraceID != proc.TraceID || updated.UpstreamOrderNo != "Faka-TR-1" || updated.UpstreamAmount.String() != "8.25" {
		t.Fatalf("status=%q trace=%q trade=%q amount=%q; want accepted with persisted trade and amount", updated.Status, updated.TraceID, updated.UpstreamOrderNo, updated.UpstreamAmount.String())
	}
	if listCalls.Load() != 1 || detailCalls.Load() != 1 || tradeCalls.Load() != 1 {
		t.Fatalf("list=%d detail=%d trade=%d; one worker should validate and trade once", listCalls.Load(), detailCalls.Load(), tradeCalls.Load())
	}
	_ = svc.SubmitToUpstream(proc.ID)
	if tradeCalls.Load() != 1 {
		t.Fatalf("duplicate task sent trade %d times", tradeCalls.Load())
	}
}

func TestFakaTradeFailsClosedWithoutIndependentEvidenceConfirmer(t *testing.T) {
	fixture := newFakaCASFixture(t, &casErrorOnceRepository{}, nil)
	if err := fixture.svc.SubmitToUpstream(fixture.proc.ID); err == nil {
		t.Fatal("submission without independent evidence confirmer must report the frozen result")
	}
	var updated ProcurementOrder
	if err := fixture.db.First(&updated, fixture.proc.ID).Error; err != nil {
		t.Fatal(err)
	}
	if updated.Status != constants.ProcurementStatusAwaitingVerification {
		t.Fatalf("status=%q, want awaiting verification", updated.Status)
	}
	if got := fixture.tradeCalls.Load(); got != 0 {
		t.Fatalf("Faka Trade calls=%d, want 0 without confirmer", got)
	}
	if fixture.queue.submit != 0 || fixture.queue.poll != 0 {
		t.Fatalf("queue submit=%d poll=%d; evidence failure must not enqueue", fixture.queue.submit, fixture.queue.poll)
	}
}

func TestFakaTradeFailsClosedWhenIndependentEvidenceConfirmerErrors(t *testing.T) {
	confirmErr := errors.New("injected independent evidence outage")
	fixture := newFakaCASFixture(t, &casErrorOnceRepository{}, errorEvidenceConfirmer{err: confirmErr})
	err := fixture.svc.SubmitToUpstream(fixture.proc.ID)
	if err == nil || !errors.Is(err, confirmErr) {
		t.Fatalf("submit error=%v, want confirmer error", err)
	}
	var updated ProcurementOrder
	if err := fixture.db.First(&updated, fixture.proc.ID).Error; err != nil {
		t.Fatal(err)
	}
	if updated.Status != constants.ProcurementStatusAwaitingVerification {
		t.Fatalf("status=%q, want awaiting verification", updated.Status)
	}
	if got := fixture.tradeCalls.Load(); got != 0 {
		t.Fatalf("Faka Trade calls=%d, want 0 when confirmer fails", got)
	}
	if fixture.queue.submit != 0 || fixture.queue.poll != 0 {
		t.Fatalf("queue submit=%d poll=%d; evidence failure must not enqueue", fixture.queue.submit, fixture.queue.poll)
	}
}

func TestFakaTradeFailsClosedWhenS3ConfirmationFails(t *testing.T) {
	fixture := newFakaCASFixture(t, &casErrorOnceRepository{}, errorEvidenceConfirmer{err: fmt.Errorf("mock S3 unavailable: %w", procs3.ErrOutcomeUnknown)})
	if err := fixture.svc.SubmitToUpstream(fixture.proc.ID); err == nil {
		t.Fatal("S3 confirmation failure must be returned")
	}
	var updated ProcurementOrder
	if err := fixture.db.First(&updated, fixture.proc.ID).Error; err != nil {
		t.Fatal(err)
	}
	if updated.Status != constants.ProcurementStatusAwaitingVerification {
		t.Fatalf("status=%q, want awaiting verification", updated.Status)
	}
	if got := fixture.tradeCalls.Load(); got != 0 {
		t.Fatalf("Faka Trade calls=%d, want 0 after S3 confirmation failure", got)
	}
	if fixture.queue.submit != 0 || fixture.queue.poll != 0 {
		t.Fatalf("queue submit=%d poll=%d; S3 failure must not enqueue", fixture.queue.submit, fixture.queue.poll)
	}
}

type confirmedMemoryEvidenceSource struct{ *memoryEvidenceStore }

func (*confirmedMemoryEvidenceSource) IndependentlyConfirmed() bool { return true }

func TestJournalRecoveryLeavesProcurementStateAndQueueUntouched(t *testing.T) {
	db := setupProcurementTestDB(t)
	order := createProcTestOrder(t, db, "PROC-JOURNAL-RECOVERY", constants.OrderStatusPaid, constants.FulfillmentTypeUpstream)
	proc := createTestProcurementOrder(t, db, 1, order.ID, order.OrderNo, constants.ProcurementStatusAccepted)
	if err := db.Model(&procurementdomain.Order{}).Where("id = ?", proc.ID).Updates(map[string]interface{}{"upstream_order_no": "ADMIN-TRADE", "upstream_amount": "5.00"}).Error; err != nil {
		t.Fatal(err)
	}
	source := &confirmedMemoryEvidenceSource{newMemoryEvidenceStore()}
	intent := procurementcontract.EvidenceEvent{EventID: "intent-recovery", ProcurementOrderID: proc.ID, TraceID: proc.TraceID, EventType: "trade_intent", SchemaVersion: 1, Supplier: "faka"}
	response := procurementcontract.EvidenceEvent{EventID: "response-recovery", ProcurementOrderID: proc.ID, TraceID: proc.TraceID, EventType: "trade_response", TradeNo: "WORKER-TRADE", Amount: "8.25", ResponseAt: time.Date(2026, 11, 17, 12, 0, 0, 0, time.UTC), SchemaVersion: 1, Supplier: "faka", RelatedEventID: intent.EventID}
	if err := source.AppendIntent(intent); err != nil {
		t.Fatal(err)
	}
	if err := source.AppendResponse(response); err != nil {
		t.Fatal(err)
	}
	queue := &countingProcurementEnqueuer{}
	tradeCalls := atomic.Int32{}
	recovery, err := procurementapp.NewTradeResponseJournalRecovery(source, procurementaudit.New(db))
	if err != nil {
		t.Fatal(err)
	}
	report, err := recovery.ScanPending()
	if err != nil || report.Imported != 1 || report.Skipped != 0 {
		t.Fatalf("report=%+v err=%v", report, err)
	}
	var current procurementdomain.Order
	if err := db.First(&current, proc.ID).Error; err != nil {
		t.Fatal(err)
	}
	if current.Status != constants.ProcurementStatusAccepted || current.UpstreamOrderNo != "ADMIN-TRADE" || current.UpstreamAmount.String() != "5.00" {
		t.Fatalf("recovery changed procurement state or administrator data: %#v", current)
	}
	if tradeCalls.Load() != 0 || queue.submit != 0 || queue.poll != 0 {
		t.Fatalf("Trade calls=%d submit=%d poll=%d", tradeCalls.Load(), queue.submit, queue.poll)
	}
}

func TestFakaCASDatabaseErrorFreezesAndReportsPersistedEvidence(t *testing.T) {
	db := setupProcurementTemporaryFileDB(t)
	order := createProcTestOrder(t, db, "PROC-FAKA-CAS-DB-ERROR", constants.OrderStatusPaid, constants.FulfillmentTypeUpstream)
	pm := &mappingdomain.Mapping{ConnectionID: 1, LocalProductID: 1, UpstreamProductID: 101, UpstreamProductCode: "persisted-code", IsActive: true, UpstreamStatus: mappingdomain.UpstreamStatusActive}
	if err := db.Create(pm).Error; err != nil {
		t.Fatal(err)
	}
	if err := db.Create(&mappingdomain.SKUMapping{ProductMappingID: pm.ID, LocalSKUID: 1, UpstreamSKUID: 201, UpstreamIsActive: true}).Error; err != nil {
		t.Fatal(err)
	}
	var tradeCalls atomic.Int32
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		w.Header().Set("Content-Type", "application/json")
		switch r.URL.Path {
		case "/shared/commodity/items":
			_, _ = w.Write([]byte(`{"code":200,"data":[{"id":2,"children":[{"id":101,"code":"persisted-code","name":"Simple"}]}]}`))
		case "/shared/commodity/item":
			_, _ = w.Write([]byte(`{"code":200,"data":{"id":101,"code":"persisted-code","name":"Simple","config":{},"draft_status":0,"password_status":0,"widget":null,"minimum":1,"maximum":4,"delivery_way":1}}`))
		case "/shared/commodity/trade":
			tradeCalls.Add(1)
			_, _ = w.Write([]byte(`{"code":200,"data":{"tradeNo":"Faka-CAS-1","amount":"8.25"}}`))
		default:
			t.Errorf("unexpected endpoint %s", r.URL.Path)
		}
	}))
	defer server.Close()
	connections := newTestSiteConnectionService(db, "test-key", t.TempDir())
	conn, err := connections.Create(siteconnectionapp.CreateInput{Name: "faka", BaseURL: server.URL, ApiKey: "id", ApiSecret: "key", Protocol: constants.ConnectionProtocolFaka})
	if err != nil {
		t.Fatal(err)
	}
	proc := createTestProcurementOrder(t, db, conn.ID, order.ID, order.OrderNo, constants.ProcurementStatusPending)
	baseRepo := procurementgormstore.New(db)
	repo := &casErrorOnceRepository{Repository: baseRepo, failTargets: []string{constants.ProcurementStatusAccepted}, failCount: 1}
	svc := newTestProcurementServiceWithRepository(db, connections, repo, nil, acceptingEvidenceConfirmer{}, procurementaudit.New(db))
	err = svc.SubmitToUpstream(proc.ID)
	if err == nil || !strings.Contains(err.Error(), "Faka Trade response recovery") || !strings.Contains(err.Error(), "Faka-CAS-1") {
		t.Fatalf("error=%v; want recovery report containing persisted trade", err)
	}
	var stored ProcurementOrder
	if err := db.First(&stored, proc.ID).Error; err != nil {
		t.Fatal(err)
	}
	if stored.Status != constants.ProcurementStatusAwaitingVerification || stored.TraceID != proc.TraceID || stored.UpstreamOrderNo != "" {
		t.Fatalf("unexpected frozen procurement state: status=%q trace=%q trade=%q", stored.Status, stored.TraceID, stored.UpstreamOrderNo)
	}
	var events []procurementdomain.TradeResponseJournalEntry
	if err := db.Where("trace_id = ?", proc.TraceID).Find(&events).Error; err != nil {
		t.Fatal(err)
	}
	if len(events) != 1 || events[0].ProcurementOrderID != proc.ID || events[0].UpstreamTradeNo != "Faka-CAS-1" || events[0].Amount != "8.25" || events[0].SourceEventID == nil || events[0].SourceEventHash == nil {
		t.Fatalf("unexpected journal evidence: %#v", events)
	}
	if tradeCalls.Load() != 1 {
		t.Fatalf("Trade calls=%d; want one", tradeCalls.Load())
	}
	if err := svc.SubmitToUpstream(proc.ID); !errors.Is(err, procurementcontract.ErrStatusInvalid) {
		t.Fatalf("duplicate submit error=%v", err)
	}
	if tradeCalls.Load() != 1 {
		t.Fatalf("duplicate recovery triggered Trade; calls=%d", tradeCalls.Load())
	}
}

func TestFakaCASCommittedThenErrorRecognizesPersistedAcceptedState(t *testing.T) {
	repo := &casErrorOnceRepository{failTargets: []string{constants.ProcurementStatusAccepted}, failCount: 1, commitThenError: true}
	fixture := newFakaCASFixture(t, repo)
	err := fixture.svc.SubmitToUpstream(fixture.proc.ID)
	if err == nil || !strings.Contains(err.Error(), "already contains matching accepted state") {
		t.Fatalf("error=%v; want evidence of committed accepted state", err)
	}
	stored, err := procurementgormstore.New(fixture.db).GetByID(fixture.proc.ID)
	if err != nil {
		t.Fatal(err)
	}
	if stored.Status != constants.ProcurementStatusAccepted || stored.TraceID != fixture.proc.TraceID || stored.UpstreamOrderNo != "Faka-CAS-1" || stored.UpstreamAmount.String() != "8.25" {
		t.Fatalf("accepted state mismatch: status=%q trace=%q trade=%q amount=%q", stored.Status, stored.TraceID, stored.UpstreamOrderNo, stored.UpstreamAmount.String())
	}
	var entries []procurementdomain.TradeResponseJournalEntry
	if err := fixture.db.Where("trace_id = ?", fixture.proc.TraceID).Find(&entries).Error; err != nil {
		t.Fatal(err)
	}
	if len(entries) != 1 || entries[0].ProcurementOrderID != fixture.proc.ID || entries[0].UpstreamTradeNo != stored.UpstreamOrderNo || entries[0].Amount != stored.UpstreamAmount.String() {
		t.Fatalf("persisted CAS and journal disagree: %#v", entries)
	}
	if fixture.tradeCalls.Load() != 1 || fixture.queue.submit != 0 || fixture.queue.poll != 0 {
		t.Fatalf("trades=%d submit_enqueues=%d poll_enqueues=%d; want one Trade and no enqueues", fixture.tradeCalls.Load(), fixture.queue.submit, fixture.queue.poll)
	}
}

func TestFakaCASAndFreezeErrorsKeepJournalWithoutRetryOrEnqueue(t *testing.T) {
	repo := &casErrorOnceRepository{failTargets: []string{constants.ProcurementStatusAccepted, constants.ProcurementStatusAwaitingVerification}, failCount: 2}
	fixture := newFakaCASFixture(t, repo)
	err := fixture.svc.SubmitToUpstream(fixture.proc.ID)
	if err == nil || !strings.Contains(err.Error(), "freeze_succeeded=false") || !strings.Contains(err.Error(), "Faka-CAS-1") {
		t.Fatalf("error=%v; want explicit failed-freeze recovery evidence", err)
	}
	stored, err := procurementgormstore.New(fixture.db).GetByID(fixture.proc.ID)
	if err != nil {
		t.Fatal(err)
	}
	if stored.Status != constants.ProcurementStatusSubmitting || stored.TraceID != fixture.proc.TraceID || stored.UpstreamOrderNo != "" {
		t.Fatalf("freeze failure was misreported or state overwritten: status=%q trace=%q trade=%q", stored.Status, stored.TraceID, stored.UpstreamOrderNo)
	}
	var entries []procurementdomain.TradeResponseJournalEntry
	if err := fixture.db.Where("trace_id = ?", fixture.proc.TraceID).Find(&entries).Error; err != nil {
		t.Fatal(err)
	}
	if len(entries) != 1 || entries[0].UpstreamTradeNo != "Faka-CAS-1" || entries[0].Amount != "8.25" {
		t.Fatalf("journal evidence missing or changed: %#v", entries)
	}
	if fixture.tradeCalls.Load() != 1 || fixture.queue.submit != 0 || fixture.queue.poll != 0 {
		t.Fatalf("trades=%d submit_enqueues=%d poll_enqueues=%d", fixture.tradeCalls.Load(), fixture.queue.submit, fixture.queue.poll)
	}
	if retryErr := fixture.svc.SubmitToUpstream(fixture.proc.ID); !errors.Is(retryErr, procurementcontract.ErrStatusInvalid) {
		t.Fatalf("repeat worker error=%v; want state gate", retryErr)
	}
	if fixture.tradeCalls.Load() != 1 || fixture.queue.submit != 0 || fixture.queue.poll != 0 {
		t.Fatalf("repeat recovery caused side effects: trades=%d submit_enqueues=%d poll_enqueues=%d", fixture.tradeCalls.Load(), fixture.queue.submit, fixture.queue.poll)
	}
	var procurementCount, journalCount int64
	if err := fixture.db.Model(&procurementdomain.Order{}).Where("trace_id = ?", fixture.proc.TraceID).Count(&procurementCount).Error; err != nil {
		t.Fatal(err)
	}
	if err := fixture.db.Model(&procurementdomain.TradeResponseJournalEntry{}).Where("trace_id = ?", fixture.proc.TraceID).Count(&journalCount).Error; err != nil {
		t.Fatal(err)
	}
	if procurementCount != 1 || journalCount != 1 {
		t.Fatalf("repeat recovery created data: procurement=%d journal=%d", procurementCount, journalCount)
	}
}

func TestFakaLateTradeAfterAdminOrderFoundIsAuditedWithoutOverwrite(t *testing.T) {
	for _, workerTrade := range []string{"ADMIN-TRADE", "WORKER-OTHER"} {
		t.Run(workerTrade, func(t *testing.T) {
			db := setupProcurementTemporaryFileDB(t)
			order := createProcTestOrder(t, db, "PROC-LATE-"+workerTrade, constants.OrderStatusPaid, constants.FulfillmentTypeUpstream)
			pm := &mappingdomain.Mapping{ConnectionID: 1, LocalProductID: 1, UpstreamProductID: 101, UpstreamProductCode: "persisted-code", IsActive: true, UpstreamStatus: mappingdomain.UpstreamStatusActive}
			if err := db.Create(pm).Error; err != nil {
				t.Fatal(err)
			}
			if err := db.Create(&mappingdomain.SKUMapping{ProductMappingID: pm.ID, LocalSKUID: 1, UpstreamSKUID: 201, UpstreamIsActive: true}).Error; err != nil {
				t.Fatal(err)
			}
			tradeEntered, releaseTrade := make(chan struct{}), make(chan struct{})
			defer func() {
				select {
				case <-releaseTrade:
				default:
					close(releaseTrade)
				}
			}()
			var trades atomic.Int32
			server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
				w.Header().Set("Content-Type", "application/json")
				switch r.URL.Path {
				case "/shared/commodity/items":
					_, _ = w.Write([]byte(`{"code":200,"data":[{"id":2,"children":[{"id":101,"code":"persisted-code","name":"Simple"}]}]}`))
				case "/shared/commodity/item":
					_, _ = w.Write([]byte(`{"code":200,"data":{"id":101,"code":"persisted-code","name":"Simple","config":{},"draft_status":0,"password_status":0,"widget":null,"minimum":1,"maximum":4,"delivery_way":1}}`))
				case "/shared/commodity/trade":
					trades.Add(1)
					close(tradeEntered)
					<-releaseTrade
					_, _ = fmt.Fprintf(w, `{"code":200,"data":{"tradeNo":%q,"amount":"9.50"}}`, workerTrade)
				default:
					t.Errorf("unexpected endpoint %s", r.URL.Path)
				}
			}))
			defer server.Close()
			connections := newTestSiteConnectionService(db, "test-key", t.TempDir())
			conn, err := connections.Create(siteconnectionapp.CreateInput{Name: "faka", BaseURL: server.URL, ApiKey: "id", ApiSecret: "key", Protocol: constants.ConnectionProtocolFaka})
			if err != nil {
				t.Fatal(err)
			}
			proc := createTestProcurementOrder(t, db, conn.ID, order.ID, order.OrderNo, constants.ProcurementStatusPending)
			audit := &recordingProcurementDecisionAudit{}
			svc := newTestProcurementService(db, connections, audit)
			workerDone := make(chan error, 1)
			go func() { workerDone <- svc.SubmitToUpstream(proc.ID) }()
			<-tradeEntered
			// Force the administrator transition to commit while the HTTP Trade is blocked.
			if _, err := procurementgormstore.New(db).UpdateStatus(proc.ID, constants.ProcurementStatusSubmitting, constants.ProcurementStatusAwaitingVerification, map[string]interface{}{"updated_at": time.Now()}); err != nil {
				t.Fatal(err)
			}
			if err := svc.RetryManual(proc.ID, procurementcontract.ManualResolution{Mode: "order_found", OperatorAdminID: 4, TradeNo: "ADMIN-TRADE", Reason: "verified in supplier console"}); err != nil {
				t.Fatal(err)
			}
			close(releaseTrade)
			if err := <-workerDone; err == nil {
				t.Fatal("late response must not be reported as ordinary success")
			}
			var stored ProcurementOrder
			if err := db.First(&stored, proc.ID).Error; err != nil {
				t.Fatal(err)
			}
			if stored.Status != constants.ProcurementStatusAccepted || stored.UpstreamOrderNo != "ADMIN-TRADE" || stored.UpstreamAmount.String() != "0.00" || trades.Load() != 1 {
				t.Fatalf("admin data overwritten: status=%s trade=%s amount=%s trades=%d", stored.Status, stored.UpstreamOrderNo, stored.UpstreamAmount.String(), trades.Load())
			}
			var events []procurementdomain.TradeResponseJournalEntry
			if err := db.Where("procurement_order_id = ?", proc.ID).Order("id ASC").Find(&events).Error; err != nil {
				t.Fatal(err)
			}
			if len(events) != 2 {
				t.Fatalf("journal events=%d; want response evidence and conflict record", len(events))
			}
			conflict := events[1]
			if (workerTrade == "ADMIN-TRADE") != (conflict.EventType == "late_response_same_trade_no") || conflict.ConfirmedTradeNo != "ADMIN-TRADE" || conflict.UpstreamTradeNo != workerTrade {
				t.Fatalf("unexpected conflict journal event %#v for worker trade %q", conflict, workerTrade)
			}
		})
	}
}

func TestFakaTradeSubmissionOutcomesStaySafe(t *testing.T) {
	cases := []struct {
		name, tradeResponse, wantStatus, wantTradeNo string
		tradeHTTPStatus                              int
		timeout                                      bool
	}{
		{name: "business failure needs verification", tradeResponse: `{"code":0,"msg":"insufficient balance"}`, wantStatus: constants.ProcurementStatusAwaitingVerification},
		{name: "malformed response", tradeResponse: `{broken`, wantStatus: constants.ProcurementStatusAwaitingVerification},
		{name: "known trade number with invalid amount", tradeResponse: `{"code":200,"data":{"tradeNo":"TR-needs-review","amount":"invalid"}}`, wantStatus: constants.ProcurementStatusAwaitingVerification, wantTradeNo: "TR-needs-review"},
		{name: "server error with rejection body", tradeResponse: `{"code":0,"msg":"server error"}`, tradeHTTPStatus: 500, wantStatus: constants.ProcurementStatusAwaitingVerification},
		{name: "disconnect after trade received", timeout: true, wantStatus: constants.ProcurementStatusAwaitingVerification},
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			db := setupProcurementTestDB(t)
			order := createProcTestOrder(t, db, "PROC-FAKA-OUTCOME", constants.OrderStatusPaid, constants.FulfillmentTypeUpstream)
			pm := &mappingdomain.Mapping{ConnectionID: 1, LocalProductID: 1, UpstreamProductID: 101, UpstreamProductCode: "persisted-code", IsActive: true, UpstreamStatus: mappingdomain.UpstreamStatusActive}
			if err := db.Create(pm).Error; err != nil {
				t.Fatal(err)
			}
			if err := db.Create(&mappingdomain.SKUMapping{ProductMappingID: pm.ID, LocalSKUID: 1, UpstreamSKUID: 201, UpstreamIsActive: true}).Error; err != nil {
				t.Fatal(err)
			}
			var tradeCalls atomic.Int32
			server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
				w.Header().Set("Content-Type", "application/json")
				switch r.URL.Path {
				case "/shared/commodity/items":
					_, _ = w.Write([]byte(`{"code":200,"data":[{"id":2,"children":[{"id":101,"code":"persisted-code","name":"Simple"}]}]}`))
				case "/shared/commodity/item":
					_, _ = w.Write([]byte(`{"code":200,"data":{"id":101,"code":"persisted-code","name":"Simple","config":{},"draft_status":0,"password_status":0,"widget":null,"minimum":1,"maximum":4,"delivery_way":1}}`))
				case "/shared/commodity/trade":
					tradeCalls.Add(1)
					if tc.timeout {
						conn, _, err := w.(http.Hijacker).Hijack()
						if err != nil {
							t.Error(err)
						} else {
							_ = conn.Close()
						}
						return
					}
					if tc.tradeHTTPStatus != 0 {
						w.WriteHeader(tc.tradeHTTPStatus)
					}
					_, _ = w.Write([]byte(tc.tradeResponse))
				default:
					t.Errorf("unexpected endpoint: %s", r.URL.Path)
				}
			}))
			defer server.Close()
			connSvc := newTestSiteConnectionService(db, "test-key", t.TempDir())
			conn, err := connSvc.Create(siteconnectionapp.CreateInput{Name: "faka", BaseURL: server.URL, ApiKey: "id", ApiSecret: "key", Protocol: constants.ConnectionProtocolFaka})
			if err != nil {
				t.Fatal(err)
			}
			proc := createTestProcurementOrder(t, db, conn.ID, order.ID, order.OrderNo, constants.ProcurementStatusPending)
			svc := newTestProcurementService(db, connSvc)
			_ = svc.SubmitToUpstream(proc.ID)
			var stored ProcurementOrder
			if err := db.First(&stored, proc.ID).Error; err != nil {
				t.Fatal(err)
			}
			if stored.Status != tc.wantStatus || stored.UpstreamOrderNo != tc.wantTradeNo || tradeCalls.Load() != 1 {
				t.Fatalf("status=%q tradeNo=%q trades=%d; want %q/%q and one trade", stored.Status, stored.UpstreamOrderNo, tradeCalls.Load(), tc.wantStatus, tc.wantTradeNo)
			}
			_ = svc.SubmitToUpstream(proc.ID)
			if tradeCalls.Load() != 1 {
				t.Fatalf("duplicate trade: %d", tradeCalls.Load())
			}
		})
	}
}

func TestFakaTradeSuccessLocalWriteFailureRemainsFrozen(t *testing.T) {
	db := setupProcurementTestDB(t)
	order := createProcTestOrder(t, db, "PROC-FAKA-LOCAL-WRITE", constants.OrderStatusPaid, constants.FulfillmentTypeUpstream)
	pm := &mappingdomain.Mapping{ConnectionID: 1, LocalProductID: 1, UpstreamProductID: 101, UpstreamProductCode: "persisted-code", IsActive: true, UpstreamStatus: mappingdomain.UpstreamStatusActive}
	if err := db.Create(pm).Error; err != nil {
		t.Fatal(err)
	}
	if err := db.Create(&mappingdomain.SKUMapping{ProductMappingID: pm.ID, LocalSKUID: 1, UpstreamSKUID: 201, UpstreamIsActive: true}).Error; err != nil {
		t.Fatal(err)
	}
	var trades atomic.Int32
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		w.Header().Set("Content-Type", "application/json")
		switch r.URL.Path {
		case "/shared/commodity/items":
			_, _ = w.Write([]byte(`{"code":200,"data":[{"id":2,"children":[{"id":101,"code":"persisted-code","name":"Simple"}]}]}`))
		case "/shared/commodity/item":
			_, _ = w.Write([]byte(`{"code":200,"data":{"id":101,"code":"persisted-code","name":"Simple","config":{},"draft_status":0,"password_status":0,"widget":null,"minimum":1,"maximum":4,"delivery_way":1}}`))
		case "/shared/commodity/trade":
			trades.Add(1)
			_, _ = w.Write([]byte(`{"code":200,"data":{"tradeNo":"TR-write-failed","amount":"3.00"}}`))
		default:
			t.Errorf("unexpected endpoint %s", r.URL.Path)
		}
	}))
	defer server.Close()
	connSvc := newTestSiteConnectionService(db, "test-key", t.TempDir())
	conn, err := connSvc.Create(siteconnectionapp.CreateInput{Name: "faka", BaseURL: server.URL, ApiKey: "id", ApiSecret: "key", Protocol: constants.ConnectionProtocolFaka})
	if err != nil {
		t.Fatal(err)
	}
	proc := createTestProcurementOrder(t, db, conn.ID, order.ID, order.OrderNo, constants.ProcurementStatusPending)
	if err := db.Exec(`CREATE TRIGGER fail_faka_accept BEFORE UPDATE ON procurement_orders WHEN NEW.status = 'accepted' BEGIN SELECT RAISE(FAIL, 'mock accepted write failure'); END`).Error; err != nil {
		t.Fatal(err)
	}
	svc := newTestProcurementService(db, connSvc, procurementaudit.New(db))
	if err := svc.SubmitToUpstream(proc.ID); err == nil {
		t.Fatal("expected local write failure")
	}
	var stored ProcurementOrder
	if err := db.First(&stored, proc.ID).Error; err != nil {
		t.Fatal(err)
	}
	if stored.Status != constants.ProcurementStatusAwaitingVerification || stored.UpstreamOrderNo != "" || stored.UpstreamAmount.String() != "0.00" || stored.TraceID != proc.TraceID || trades.Load() != 1 {
		t.Fatalf("status=%q trade=%q amount=%q trace=%q trades=%d; failed acceptance must freeze without writing upstream fields", stored.Status, stored.UpstreamOrderNo, stored.UpstreamAmount.String(), stored.TraceID, trades.Load())
	}
	var audit procurementdomain.TradeResponseJournalEntry
	if err := db.Where("procurement_order_id = ?", proc.ID).First(&audit).Error; err != nil || audit.UpstreamTradeNo != "TR-write-failed" || audit.Amount != "3.00" {
		t.Fatalf("successful upstream response audit=%#v error=%v", audit, err)
	}
	_ = svc.SubmitToUpstream(proc.ID)
	if trades.Load() != 1 {
		t.Fatalf("duplicate trade after write failure: %d", trades.Load())
	}
}

func TestFakaLateTradeAuditFailureIsReturned(t *testing.T) {
	db := setupProcurementTestDB(t)
	order := createProcTestOrder(t, db, "PROC-LATE-AUDIT-FAIL", constants.OrderStatusPaid, constants.FulfillmentTypeUpstream)
	pm := &mappingdomain.Mapping{ConnectionID: 1, LocalProductID: 1, UpstreamProductID: 101, UpstreamProductCode: "persisted-code", IsActive: true, UpstreamStatus: mappingdomain.UpstreamStatusActive}
	if err := db.Create(pm).Error; err != nil {
		t.Fatal(err)
	}
	if err := db.Create(&mappingdomain.SKUMapping{ProductMappingID: pm.ID, LocalSKUID: 1, UpstreamSKUID: 201, UpstreamIsActive: true}).Error; err != nil {
		t.Fatal(err)
	}
	var tradeCalls atomic.Int32
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		w.Header().Set("Content-Type", "application/json")
		switch r.URL.Path {
		case "/shared/commodity/items":
			_, _ = w.Write([]byte(`{"code":200,"data":[{"id":2,"children":[{"id":101,"code":"persisted-code","name":"Simple"}]}]}`))
		case "/shared/commodity/item":
			_, _ = w.Write([]byte(`{"code":200,"data":{"id":101,"code":"persisted-code","name":"Simple","config":{},"draft_status":0,"password_status":0,"widget":null,"minimum":1,"maximum":4,"delivery_way":1}}`))
		case "/shared/commodity/trade":
			tradeCalls.Add(1)
			_, _ = w.Write([]byte(`{"code":200,"data":{"tradeNo":"TR-AUDIT-FAIL","amount":"4.00"}}`))
		}
	}))
	defer server.Close()
	connections := newTestSiteConnectionService(db, "test-key", t.TempDir())
	conn, err := connections.Create(siteconnectionapp.CreateInput{Name: "faka", BaseURL: server.URL, ApiKey: "id", ApiSecret: "key", Protocol: constants.ConnectionProtocolFaka})
	if err != nil {
		t.Fatal(err)
	}
	proc := createTestProcurementOrder(t, db, conn.ID, order.ID, order.OrderNo, constants.ProcurementStatusPending)
	if err := db.Exec(`CREATE TRIGGER fail_faka_accept BEFORE UPDATE ON procurement_orders WHEN NEW.status = 'accepted' BEGIN SELECT RAISE(FAIL, 'mock accepted write failure'); END`).Error; err != nil {
		t.Fatal(err)
	}
	if err := db.Exec(`CREATE TRIGGER fail_late_audit BEFORE INSERT ON procurement_trade_response_journal BEGIN SELECT RAISE(FAIL, 'mock journal write failure'); END`).Error; err != nil {
		t.Fatal(err)
	}
	notifier := &recordingFailureNotifier{}
	svc := newTestProcurementServiceWithFailureNotifier(db, connections, notifier, procurementaudit.New(db))
	err = svc.SubmitToUpstream(proc.ID)
	if err == nil || !strings.Contains(err.Error(), "persist Faka Trade response evidence") {
		t.Fatalf("error=%v; want explicit journal failure", err)
	}
	if tradeCalls.Load() != 1 {
		t.Fatalf("trade calls=%d, want one", tradeCalls.Load())
	}
	if notifier.calls != 1 || !strings.Contains(notifier.message, "CRITICAL:") {
		t.Fatalf("critical journal failure alert calls=%d message=%q", notifier.calls, notifier.message)
	}
	var stored ProcurementOrder
	if err := db.First(&stored, proc.ID).Error; err != nil {
		t.Fatal(err)
	}
	if stored.Status != constants.ProcurementStatusAwaitingVerification || stored.UpstreamOrderNo != "" || stored.UpstreamAmount.String() != "0.00" {
		t.Fatalf("unsafe local fields after audit failure: status=%s trade=%s", stored.Status, stored.UpstreamOrderNo)
	}
}

func TestTradeResponseJournalCanBeReconciledRepeatedlyWithoutSubmitting(t *testing.T) {
	db := setupProcurementTemporaryFileDB(t)
	order := createProcTestOrder(t, db, "PROC-JOURNAL-RECONCILE", constants.OrderStatusPaid, constants.FulfillmentTypeUpstream)
	proc := createTestProcurementOrder(t, db, 1, order.ID, order.OrderNo, constants.ProcurementStatusAwaitingVerification)
	svc := newTestProcurementService(db, nil, procurementaudit.New(db))
	for i := 0; i < 2; i++ {
		if err := svc.RecordTradeResponseReconciliation(proc.ID, proc.TraceID, "VERIFIED-TRADE", "6.25", "checked supplier console", 42); err != nil {
			t.Fatal(err)
		}
	}
	entries, err := svc.ListTradeResponseJournal(proc.TraceID)
	if err != nil || len(entries) != 2 {
		t.Fatalf("reconciliation entries=%d error=%v", len(entries), err)
	}
	for _, entry := range entries {
		if entry.EventType != "manual_reconciliation_recorded" || entry.OperatorAdminID == nil || *entry.OperatorAdminID != 42 || entry.UpstreamTradeNo != "VERIFIED-TRADE" || entry.Amount != "6.25" {
			t.Fatalf("unexpected reconciliation evidence: %#v", entry)
		}
	}
	var stored ProcurementOrder
	if err := db.First(&stored, proc.ID).Error; err != nil {
		t.Fatal(err)
	}
	if stored.Status != constants.ProcurementStatusAwaitingVerification || stored.UpstreamOrderNo != "" || stored.UpstreamAmount.String() != "0.00" {
		t.Fatalf("reconciliation changed procurement state or upstream fields: %#v", stored)
	}
}

func TestFakaLiveProcurementGateRejectsUnsafeRemoteDetailsWithoutTrade(t *testing.T) {
	cases := []struct {
		name       string
		config     string
		widget     string
		remoteID   int
		remoteCode string
		minimum    int
		maximum    int
		quantity   int
		detailFail bool
		status     string
		active     bool
		skuParent  uint
		skuMissing bool
		wantVerify bool
	}{
		{name: "new sku configuration", config: `{"sku":{}}`, minimum: 1, maximum: 4, quantity: 1, active: true, status: mappingdomain.UpstreamStatusActive, wantVerify: true},
		{name: "new race configuration", config: `{"race":{}}`, minimum: 1, maximum: 4, quantity: 1, active: true, status: mappingdomain.UpstreamStatusActive, wantVerify: true},
		{name: "new widget requirement", config: `{}`, widget: `[{"name":"email","required":true}]`, minimum: 1, maximum: 4, quantity: 1, active: true, status: mappingdomain.UpstreamStatusActive, wantVerify: true},
		{name: "remote numeric ID mismatch", config: `{}`, remoteID: 999, remoteCode: "persisted-code", minimum: 1, maximum: 4, quantity: 1, active: true, status: mappingdomain.UpstreamStatusActive, wantVerify: true},
		{name: "remote code mismatch", config: `{}`, remoteID: 101, remoteCode: "other-code", minimum: 1, maximum: 4, quantity: 1, active: true, status: mappingdomain.UpstreamStatusActive, wantVerify: true},
		{name: "detail API failure", config: `{}`, minimum: 1, maximum: 4, quantity: 1, detailFail: true, active: true, status: mappingdomain.UpstreamStatusActive, wantVerify: true},
		{name: "minimum exceeds maximum", config: `{}`, minimum: 5, maximum: 4, quantity: 5, active: true, status: mappingdomain.UpstreamStatusActive, wantVerify: true},
		{name: "quantity above maximum", config: `{}`, minimum: 1, maximum: 2, quantity: 3, active: true, status: mappingdomain.UpstreamStatusActive, wantVerify: true},
		{name: "mapping paused", config: `{}`, minimum: 1, maximum: 4, quantity: 1, active: false, status: mappingdomain.UpstreamStatusActive},
		{name: "identity error", config: `{}`, minimum: 1, maximum: 4, quantity: 1, active: true, status: mappingdomain.UpstreamStatusIdentityError},
		{name: "SKU mapping missing", config: `{}`, minimum: 1, maximum: 4, quantity: 1, active: true, status: mappingdomain.UpstreamStatusActive, skuMissing: true},
		{name: "SKU mapped to different product", config: `{}`, minimum: 1, maximum: 4, quantity: 1, active: true, status: mappingdomain.UpstreamStatusActive, skuParent: 999},
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			db := setupProcurementTestDB(t)
			order := createProcTestOrder(t, db, "PROC-FAKA-GATE-"+strings.ReplaceAll(tc.name, " ", "-"), constants.OrderStatusPaid, constants.FulfillmentTypeUpstream)
			pm := &mappingdomain.Mapping{ConnectionID: 1, LocalProductID: 1, UpstreamProductID: 101, UpstreamProductCode: "persisted-code", IsActive: tc.active, UpstreamStatus: tc.status, FakaProcurementProfile: jsonmap.JSON{"snapshot_valid": true, "assessment_complete": true, "capability": map[string]any{"status": "eligible", "can_auto_procure": true}}}
			if err := db.Create(pm).Error; err != nil {
				t.Fatal(err)
			}
			if !tc.active {
				if err := db.Model(pm).Update("is_active", false).Error; err != nil {
					t.Fatal(err)
				}
			}
			if !tc.skuMissing {
				parentID := pm.ID
				if tc.skuParent != 0 {
					parentID = tc.skuParent
				}
				if err := db.Create(&mappingdomain.SKUMapping{ProductMappingID: parentID, LocalSKUID: 1, UpstreamSKUID: 201, UpstreamIsActive: true}).Error; err != nil {
					t.Fatal(err)
				}
			}
			var listCalls, detailCalls, tradeCalls atomic.Int32
			server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
				w.Header().Set("Content-Type", "application/json")
				switch r.URL.Path {
				case "/shared/commodity/items":
					listCalls.Add(1)
					_, _ = w.Write([]byte(`{"code":200,"data":[{"id":2,"children":[{"id":101,"code":"persisted-code","name":"Simple"}]}]}`))
				case "/shared/commodity/item":
					detailCalls.Add(1)
					if tc.detailFail {
						w.WriteHeader(http.StatusServiceUnavailable)
						_, _ = w.Write([]byte(`service unavailable`))
						return
					}
					remoteID, remoteCode := tc.remoteID, tc.remoteCode
					if remoteID == 0 {
						remoteID = 101
					}
					if remoteCode == "" {
						remoteCode = "persisted-code"
					}
					widget := tc.widget
					if widget == "" {
						widget = "null"
					}
					_, _ = w.Write([]byte(fmt.Sprintf(`{"code":200,"data":{"id":%d,"code":%q,"name":"Simple","config":%s,"draft_status":0,"password_status":0,"widget":%s,"minimum":%d,"maximum":%d,"delivery_way":1}}`, remoteID, remoteCode, tc.config, widget, tc.minimum, tc.maximum)))
				case "/shared/commodity/trade":
					tradeCalls.Add(1)
					w.WriteHeader(http.StatusInternalServerError)
				default:
					t.Errorf("unexpected Faka endpoint %s", r.URL.Path)
					w.WriteHeader(http.StatusNotFound)
				}
			}))
			defer server.Close()
			connSvc := newTestSiteConnectionService(db, "test-key", t.TempDir())
			conn, err := connSvc.Create(siteconnectionapp.CreateInput{Name: "faka", BaseURL: server.URL, ApiKey: "app-id", ApiSecret: "app-key", Protocol: constants.ConnectionProtocolFaka})
			if err != nil {
				t.Fatal(err)
			}
			proc := createTestProcurementOrder(t, db, conn.ID, order.ID, order.OrderNo, constants.ProcurementStatusPending)
			if tc.quantity != 0 {
				db.Model(&orderdomain.OrderItem{}).Where("order_id = ?", order.ID).Update("quantity", tc.quantity)
			}
			if err := newTestProcurementService(db, connSvc).SubmitToUpstream(proc.ID); err != nil {
				t.Fatalf("SubmitToUpstream: %v", err)
			}
			var updated ProcurementOrder
			if err := db.First(&updated, proc.ID).Error; err != nil {
				t.Fatal(err)
			}
			if updated.Status != constants.ProcurementStatusRejected || updated.Status == constants.ProcurementStatusAwaitingVerification {
				t.Fatalf("status=%q error=%q; pre-trade failure must be rejected, not unknown", updated.Status, updated.ErrorMessage)
			}
			if tradeCalls.Load() != 0 {
				t.Fatalf("trade calls=%d, want zero", tradeCalls.Load())
			}
			if tc.wantVerify && listCalls.Load() == 0 {
				t.Fatal("expected live list/detail verification")
			}
			if !tc.wantVerify && listCalls.Load() != 0 {
				t.Fatalf("live verification unexpectedly called %d times", listCalls.Load())
			}
		})
	}
}

func TestFakaPreTradeStatusWriteFailureLeavesSubmissionFrozen(t *testing.T) {
	db := setupProcurementTestDB(t)
	order := createProcTestOrder(t, db, "PROC-FAKA-WRITE-FAIL", constants.OrderStatusPaid, constants.FulfillmentTypeUpstream)
	pm := &mappingdomain.Mapping{ConnectionID: 1, LocalProductID: 1, UpstreamProductID: 101, UpstreamProductCode: "persisted-code", IsActive: true, UpstreamStatus: mappingdomain.UpstreamStatusActive}
	if err := db.Create(pm).Error; err != nil {
		t.Fatal(err)
	}
	if err := db.Create(&mappingdomain.SKUMapping{ProductMappingID: pm.ID, LocalSKUID: 1, UpstreamSKUID: 201, UpstreamIsActive: true}).Error; err != nil {
		t.Fatal(err)
	}
	var listCalls, detailCalls, tradeCalls atomic.Int32
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		w.Header().Set("Content-Type", "application/json")
		switch r.URL.Path {
		case "/shared/commodity/items":
			listCalls.Add(1)
			_, _ = w.Write([]byte(`{"code":200,"data":[{"id":2,"children":[{"id":101,"code":"persisted-code","name":"Simple"}]}]}`))
		case "/shared/commodity/item":
			detailCalls.Add(1)
			_, _ = w.Write([]byte(`{"code":200,"data":{"id":101,"code":"persisted-code","name":"Simple","config":{"sku":{}},"draft_status":0,"password_status":0,"widget":null,"minimum":1,"maximum":4,"delivery_way":1}}`))
		case "/shared/commodity/trade":
			tradeCalls.Add(1)
		}
	}))
	defer server.Close()
	connSvc := newTestSiteConnectionService(db, "test-key", t.TempDir())
	conn, err := connSvc.Create(siteconnectionapp.CreateInput{Name: "faka", BaseURL: server.URL, ApiKey: "app-id", ApiSecret: "app-key", Protocol: constants.ConnectionProtocolFaka})
	if err != nil {
		t.Fatal(err)
	}
	proc := createTestProcurementOrder(t, db, conn.ID, order.ID, order.OrderNo, constants.ProcurementStatusPending)
	if err := db.Exec(`CREATE TRIGGER fail_faka_rejection BEFORE UPDATE ON procurement_orders WHEN NEW.status = 'rejected' BEGIN SELECT RAISE(FAIL, 'mock status write failure'); END`).Error; err != nil {
		t.Fatal(err)
	}
	service := newTestProcurementService(db, connSvc)
	if err := service.SubmitToUpstream(proc.ID); err == nil || !strings.Contains(err.Error(), "submission remains frozen") {
		t.Fatalf("SubmitToUpstream error=%v; expected conservative status-write failure", err)
	}
	var stored ProcurementOrder
	if err := db.First(&stored, proc.ID).Error; err != nil {
		t.Fatal(err)
	}
	if stored.Status != constants.ProcurementStatusSubmitting || tradeCalls.Load() != 0 {
		t.Fatalf("status=%q trade calls=%d; write failure must leave submitting frozen and never trade", stored.Status, tradeCalls.Load())
	}
	if err := service.SubmitToUpstream(proc.ID); !errors.Is(err, procurementcontract.ErrStatusInvalid) {
		t.Fatalf("duplicate submission error=%v; want frozen status", err)
	}
	if listCalls.Load() != 1 || detailCalls.Load() != 1 || tradeCalls.Load() != 0 {
		t.Fatalf("list=%d detail=%d trade=%d; duplicate task must not bypass frozen status", listCalls.Load(), detailCalls.Load(), tradeCalls.Load())
	}
}

func TestUnknownProcurementManualResolutionAuditsAndPreservesTraceID(t *testing.T) {
	db := setupProcurementTestDB(t)
	order := createProcTestOrder(t, db, "PROC-FAKA-UNKNOWN-002", constants.OrderStatusPaid, constants.FulfillmentTypeUpstream)
	server := httptest.NewServer(http.NotFoundHandler())
	defer server.Close()
	connSvc := newTestSiteConnectionService(db, "test-key", t.TempDir())
	conn, err := connSvc.Create(siteconnectionapp.CreateInput{Name: "faka", BaseURL: server.URL, ApiKey: "app-id", ApiSecret: "app-key", Protocol: constants.ConnectionProtocolFaka})
	if err != nil {
		t.Fatal(err)
	}
	proc := createTestProcurementOrder(t, db, conn.ID, order.ID, order.OrderNo, constants.ProcurementStatusAwaitingVerification)
	audit := &recordingProcurementDecisionAudit{}
	svc := newTestProcurementService(db, connSvc, audit)
	if err := svc.RetryManual(proc.ID); !errors.Is(err, procurementcontract.ErrStatusInvalid) {
		t.Fatalf("ordinary RetryManual error=%v, want status invalid", err)
	}
	if err := svc.RetryManual(proc.ID, procurementcontract.ManualResolution{Mode: "order_found", OperatorAdminID: 7, TradeNo: "F-123ABC", Reason: "verified in supplier console"}); err != nil {
		t.Fatalf("resolve existing order: %v", err)
	}
	var resolved ProcurementOrder
	if err := db.First(&resolved, proc.ID).Error; err != nil {
		t.Fatal(err)
	}
	if resolved.Status != constants.ProcurementStatusAccepted || resolved.UpstreamOrderNo != "F-123ABC" || resolved.TraceID != proc.TraceID {
		t.Fatalf("resolved procurement=%#v", resolved)
	}
	if audit.calls != 1 || audit.operatorID != 7 || audit.reason != "verified in supplier console" || audit.action != "upstream_order_confirmed" {
		t.Fatalf("audit=%#v", audit)
	}
	if err := db.Model(&ProcurementOrder{}).Where("id = ?", proc.ID).Update("status", constants.ProcurementStatusAwaitingVerification).Error; err != nil {
		t.Fatal(err)
	}
	if err := svc.RetryManual(proc.ID, procurementcontract.ManualResolution{Mode: "no_charge", OperatorAdminID: 8, Reason: "cannot retry while trade number exists"}); !errors.Is(err, procurementcontract.ErrStatusInvalid) {
		t.Fatalf("no_charge with known trade number error=%v; want status invalid", err)
	}
	if err := db.Model(&ProcurementOrder{}).Where("id = ?", proc.ID).Update("upstream_order_no", "").Error; err != nil {
		t.Fatal(err)
	}
	if err := svc.RetryManual(proc.ID, procurementcontract.ManualResolution{Mode: "no_charge", OperatorAdminID: 8, Reason: "supplier confirmed no debit"}); !errors.Is(err, procurementcontract.ErrStatusInvalid) {
		t.Fatalf("no_charge without both confirmations error=%v, want status invalid", err)
	}
	if err := svc.RetryManual(proc.ID, procurementcontract.ManualResolution{Mode: "no_charge", OperatorAdminID: 8, Reason: "supplier reports no debit, but request may still complete", ConfirmedNoCharge: true, ConfirmedNoOrder: true}); err != nil {
		t.Fatalf("record frozen no-charge review: %v", err)
	}
	if err := db.First(&resolved, proc.ID).Error; err != nil {
		t.Fatal(err)
	}
	if resolved.Status != constants.ProcurementStatusAwaitingVerification || resolved.TraceID != proc.TraceID {
		t.Fatalf("no-charge review must preserve frozen status/trace: %q/%q", resolved.Status, resolved.TraceID)
	}
	if audit.calls != 2 || audit.action != "no_charge_review_recorded_frozen" || audit.traceID != proc.TraceID {
		t.Fatalf("retry audit=%#v", audit)
	}
}

func TestNoChargeCannotAuthorizeSubmittingTrade(t *testing.T) {
	db := setupProcurementTemporaryFileDB(t)
	order := createProcTestOrder(t, db, "PROC-FAKA-INFLIGHT-001", constants.OrderStatusPaid, constants.FulfillmentTypeUpstream)
	server := httptest.NewServer(http.NotFoundHandler())
	defer server.Close()
	connections := newTestSiteConnectionService(db, "test-key", t.TempDir())
	conn, err := connections.Create(siteconnectionapp.CreateInput{Name: "faka", BaseURL: server.URL, ApiKey: "app-id", ApiSecret: "app-key", Protocol: constants.ConnectionProtocolFaka})
	if err != nil {
		t.Fatal(err)
	}
	proc := createTestProcurementOrder(t, db, conn.ID, order.ID, order.OrderNo, constants.ProcurementStatusSubmitting)
	audit := &recordingProcurementDecisionAudit{}
	svc := newTestProcurementService(db, connections, audit)
	err = svc.RetryManual(proc.ID, procurementcontract.ManualResolution{Mode: "no_charge", OperatorAdminID: 8, Reason: "supplier confirmed no debit and no order", ConfirmedNoCharge: true, ConfirmedNoOrder: true})
	if !errors.Is(err, procurementcontract.ErrStatusInvalid) {
		t.Fatalf("submitting no_charge error = %v, want status invalid", err)
	}
	var stored ProcurementOrder
	if err := db.First(&stored, proc.ID).Error; err != nil {
		t.Fatal(err)
	}
	if stored.Status != constants.ProcurementStatusSubmitting || audit.calls != 0 {
		t.Fatalf("submitting changed to %q or produced %d audit entries", stored.Status, audit.calls)
	}
}

func TestNoChargeAuthorizationCannotOverlapTradeOrDuplicateTask(t *testing.T) {
	db := setupProcurementTemporaryFileDB(t)
	order := createProcTestOrder(t, db, "PROC-FAKA-AUTH-RACE", constants.OrderStatusPaid, constants.FulfillmentTypeUpstream)
	mapping := &mappingdomain.Mapping{ConnectionID: 1, LocalProductID: 1, UpstreamProductID: 101, UpstreamProductCode: "persisted-code", IsActive: true, UpstreamStatus: mappingdomain.UpstreamStatusActive}
	if err := db.Create(mapping).Error; err != nil {
		t.Fatal(err)
	}
	if err := db.Create(&mappingdomain.SKUMapping{ProductMappingID: mapping.ID, LocalSKUID: 1, UpstreamSKUID: 201, UpstreamIsActive: true}).Error; err != nil {
		t.Fatal(err)
	}
	tradeStarted := make(chan struct{})
	releaseTrade := make(chan struct{})
	defer func() {
		select {
		case <-releaseTrade:
		default:
			close(releaseTrade)
		}
	}()
	var trades atomic.Int32
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		w.Header().Set("Content-Type", "application/json")
		switch r.URL.Path {
		case "/shared/commodity/items":
			_, _ = w.Write([]byte(`{"code":200,"data":[{"id":2,"children":[{"id":101,"code":"persisted-code","name":"Simple"}]}]}`))
		case "/shared/commodity/item":
			_, _ = w.Write([]byte(`{"code":200,"data":{"id":101,"code":"persisted-code","name":"Simple","config":{},"draft_status":0,"password_status":0,"widget":null,"minimum":1,"maximum":4,"delivery_way":1}}`))
		case "/shared/commodity/trade":
			if trades.Add(1) == 1 {
				close(tradeStarted)
			}
			<-releaseTrade
			_, _ = w.Write([]byte(`{"code":200,"data":{"tradeNo":"TR-RACE-1","amount":"8.25"}}`))
		default:
			t.Errorf("unexpected endpoint %s", r.URL.Path)
		}
	}))
	defer server.Close()
	connections := newTestSiteConnectionService(db, "test-key", t.TempDir())
	conn, err := connections.Create(siteconnectionapp.CreateInput{Name: "faka", BaseURL: server.URL, ApiKey: "app-id", ApiSecret: "app-key", Protocol: constants.ConnectionProtocolFaka})
	if err != nil {
		t.Fatal(err)
	}
	proc := createTestProcurementOrder(t, db, conn.ID, order.ID, order.OrderNo, constants.ProcurementStatusAwaitingVerification)
	audit := &recordingProcurementDecisionAudit{}
	svc := newTestProcurementService(db, connections, audit)
	decision := procurementcontract.ManualResolution{Mode: "no_charge", OperatorAdminID: 8, Reason: "supplier confirmed no debit and no order", ConfirmedNoCharge: true, ConfirmedNoOrder: true}
	if err := svc.RetryManual(proc.ID, decision); err != nil {
		t.Fatal(err)
	}
	if err := svc.SubmitToUpstream(proc.ID); !errors.Is(err, procurementcontract.ErrStatusInvalid) {
		t.Fatalf("no_charge review must not enqueue or permit another trade, error=%v", err)
	}
	var stored ProcurementOrder
	if err := db.First(&stored, proc.ID).Error; err != nil {
		t.Fatal(err)
	}
	if stored.Status != constants.ProcurementStatusAwaitingVerification || stored.TraceID != proc.TraceID || stored.UpstreamOrderNo != "" || trades.Load() != 0 || audit.calls != 1 {
		t.Fatalf("status=%q trace=%q tradeNo=%q trade calls=%d audit calls=%d", stored.Status, stored.TraceID, stored.UpstreamOrderNo, trades.Load(), audit.calls)
	}
}

func TestFakaSubmittingRecordRemainsFrozenAfterServiceRestart(t *testing.T) {
	db := setupProcurementTestDB(t)
	order := createProcTestOrder(t, db, "PROC-FAKA-CRASH-003", constants.OrderStatusPaid, constants.FulfillmentTypeUpstream)
	var calls atomic.Int32
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, _ *http.Request) { calls.Add(1); w.WriteHeader(http.StatusOK) }))
	defer server.Close()
	connSvc := newTestSiteConnectionService(db, "test-key", t.TempDir())
	conn, err := connSvc.Create(siteconnectionapp.CreateInput{Name: "faka", BaseURL: server.URL, ApiKey: "app-id", ApiSecret: "app-key", Protocol: constants.ConnectionProtocolFaka})
	if err != nil {
		t.Fatal(err)
	}
	proc := createTestProcurementOrder(t, db, conn.ID, order.ID, order.OrderNo, constants.ProcurementStatusSubmitting)
	// A new service instance models a worker process after restart.
	if err := newTestProcurementService(db, connSvc).SubmitToUpstream(proc.ID); !errors.Is(err, procurementcontract.ErrStatusInvalid) {
		t.Fatalf("SubmitToUpstream error=%v, want frozen status", err)
	}
	var stored ProcurementOrder
	if err := db.First(&stored, proc.ID).Error; err != nil {
		t.Fatal(err)
	}
	if calls.Load() != 0 || stored.Status != constants.ProcurementStatusSubmitting || stored.TraceID != proc.TraceID {
		t.Fatalf("calls=%d status=%q trace=%q; submitting record must remain frozen without a new request", calls.Load(), stored.Status, stored.TraceID)
	}
}

func TestSubmitToUpstream_NonRetryableError_Rejects(t *testing.T) {
	db := setupProcurementTestDB(t)

	order := createProcTestOrder(t, db, "PROC-NONRETRY-001", constants.OrderStatusFulfilling, constants.FulfillmentTypeUpstream)
	pm := &mappingdomain.Mapping{ConnectionID: 1, LocalProductID: 1, UpstreamProductID: 101, IsActive: true}
	db.Create(pm)
	sm := &mappingdomain.SKUMapping{ProductMappingID: pm.ID, LocalSKUID: 1, UpstreamSKUID: 201, UpstreamIsActive: true}
	db.Create(sm)

	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		w.Header().Set("Content-Type", "application/json")
		json.NewEncoder(w).Encode(map[string]any{
			"ok":            false,
			"error_code":    "product_out_of_stock",
			"error_message": "product out of stock",
		})
	}))
	defer server.Close()

	connSvc := newTestSiteConnectionService(db, "test-key", t.TempDir())
	conn, _ := connSvc.Create(siteconnectionapp.CreateInput{
		Name: "test-upstream", BaseURL: server.URL,
		ApiKey: "key", ApiSecret: "secret", Protocol: constants.ConnectionProtocolDujiaoNext,
	})

	proc := createTestProcurementOrder(t, db, conn.ID, order.ID, order.OrderNo, "pending")
	svc := newTestProcurementService(db, connSvc)

	// 不可重试错误应返回 error
	_ = svc.SubmitToUpstream(proc.ID)

	// 验证采购单状态 = rejected
	var updatedProc ProcurementOrder
	db.First(&updatedProc, proc.ID)
	if updatedProc.Status != "rejected" {
		t.Errorf("expected procurement status 'rejected', got %q", updatedProc.Status)
	}

	// 验证本地订单状态回退到 paid
	var updatedOrder orderdomain.Order
	db.First(&updatedOrder, order.ID)
	if updatedOrder.Status != constants.OrderStatusPaid {
		t.Errorf("expected order status %q after rejection, got %q", constants.OrderStatusPaid, updatedOrder.Status)
	}
}

func TestSubmitToUpstream_RetryableError_Retries(t *testing.T) {
	db := setupProcurementTestDB(t)

	order := createProcTestOrder(t, db, "PROC-RETRY-001", constants.OrderStatusFulfilling, constants.FulfillmentTypeUpstream)
	pm := &mappingdomain.Mapping{ConnectionID: 1, LocalProductID: 1, UpstreamProductID: 101, IsActive: true}
	db.Create(pm)
	sm := &mappingdomain.SKUMapping{ProductMappingID: pm.ID, LocalSKUID: 1, UpstreamSKUID: 201, UpstreamIsActive: true}
	db.Create(sm)

	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		w.Header().Set("Content-Type", "application/json")
		json.NewEncoder(w).Encode(map[string]any{
			"ok":            false,
			"error_code":    "server_error",
			"error_message": "temporary failure",
		})
	}))
	defer server.Close()

	connSvc := newTestSiteConnectionService(db, "test-key", t.TempDir())
	conn, _ := connSvc.Create(siteconnectionapp.CreateInput{
		Name: "test-upstream", BaseURL: server.URL,
		ApiKey: "key", ApiSecret: "secret", Protocol: constants.ConnectionProtocolDujiaoNext,
		RetryMax: 3,
	})

	proc := createTestProcurementOrder(t, db, conn.ID, order.ID, order.OrderNo, "pending")
	svc := newTestProcurementService(db, connSvc)

	// 可重试错误不应返回 error（已入队重试）
	if err := svc.SubmitToUpstream(proc.ID); err != nil {
		t.Fatalf("expected no error for retryable failure, got: %v", err)
	}

	// 验证采购单状态 = failed（而非 rejected）
	var updatedProc ProcurementOrder
	db.First(&updatedProc, proc.ID)
	if updatedProc.Status != "failed" {
		t.Errorf("expected procurement status 'failed', got %q", updatedProc.Status)
	}
	if updatedProc.RetryCount != 1 {
		t.Errorf("expected retry_count=1, got %d", updatedProc.RetryCount)
	}
}

func TestHandleSubmitFailure_MaxRetriesExhausted(t *testing.T) {
	db := setupProcurementTestDB(t)

	order := createProcTestOrder(t, db, "PROC-MAXRETRY-001", constants.OrderStatusFulfilling, constants.FulfillmentTypeUpstream)
	productMapping := &mappingdomain.Mapping{ConnectionID: 1, LocalProductID: 1, UpstreamProductID: 101, IsActive: true}
	db.Create(productMapping)
	db.Create(&mappingdomain.SKUMapping{
		ProductMappingID: productMapping.ID,
		LocalSKUID:       1,
		UpstreamSKUID:    201,
		UpstreamIsActive: true,
	})

	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, _ *http.Request) {
		w.Header().Set("Content-Type", "application/json")
		_ = json.NewEncoder(w).Encode(map[string]any{
			"ok":            false,
			"error_code":    "server_error",
			"error_message": "timeout after retries",
		})
	}))
	defer server.Close()

	connSvc := newTestSiteConnectionService(db, "test-key", t.TempDir())
	conn, err := connSvc.Create(siteconnectionapp.CreateInput{
		Name: "test-upstream", BaseURL: server.URL,
		ApiKey: "key", ApiSecret: "secret", Protocol: constants.ConnectionProtocolDujiaoNext,
		RetryMax: 2, RetryIntervals: "[30,60]",
	})
	if err != nil {
		t.Fatalf("create connection: %v", err)
	}

	proc := createTestProcurementOrder(t, db, conn.ID, order.ID, order.OrderNo, "failed")
	// 设置 retry_count 已达上限
	db.Model(proc).Update("retry_count", 2)

	svc := newTestProcurementService(db, connSvc)

	// 通过公开提交入口验证：可重试错误在次数耗尽后仍必须转为 rejected。
	_ = svc.SubmitToUpstream(proc.ID)

	// 验证采购单状态 = rejected
	var updatedProc ProcurementOrder
	db.First(&updatedProc, proc.ID)
	if updatedProc.Status != "rejected" {
		t.Errorf("expected procurement status 'rejected', got %q", updatedProc.Status)
	}

	// 验证本地订单回退到 paid
	var updatedOrder orderdomain.Order
	db.First(&updatedOrder, order.ID)
	if updatedOrder.Status != constants.OrderStatusPaid {
		t.Errorf("expected order status %q, got %q", constants.OrderStatusPaid, updatedOrder.Status)
	}
}

func TestCancelManual_AcceptedKeepsStatusWhenUpstreamRefuses(t *testing.T) {
	db := setupProcurementTestDB(t)
	order := createProcTestOrder(t, db, "PROC-CANCEL-001", constants.OrderStatusFulfilling, constants.FulfillmentTypeUpstream)

	// 上游已支付的订单不允许取消
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		w.Header().Set("Content-Type", "application/json")
		w.WriteHeader(http.StatusConflict)
		json.NewEncoder(w).Encode(map[string]any{"ok": false, "error_code": "cancel_not_allowed"})
	}))
	defer server.Close()

	connSvc := newTestSiteConnectionService(db, "test-key", t.TempDir())
	conn, err := connSvc.Create(siteconnectionapp.CreateInput{
		Name: "test-upstream", BaseURL: server.URL, ApiKey: "key", ApiSecret: "secret",
		Protocol: constants.ConnectionProtocolDujiaoNext,
	})
	if err != nil {
		t.Fatalf("create connection: %v", err)
	}
	proc := createTestProcurementOrder(t, db, conn.ID, order.ID, order.OrderNo, constants.ProcurementStatusAccepted)
	db.Model(proc).Update("upstream_order_id", 999)

	svc := newTestProcurementService(db, connSvc)
	if err := svc.CancelManual(proc.ID); !errors.Is(err, procurementcontract.ErrUpstreamCancelRefused) {
		t.Fatalf("expected ErrUpstreamCancelRefused, got %v", err)
	}
	var reloaded ProcurementOrder
	db.First(&reloaded, proc.ID)
	if reloaded.Status != constants.ProcurementStatusAccepted {
		t.Fatalf("expected procurement to stay accepted, got %q", reloaded.Status)
	}
}

// 上游以非 200 + 错误码拒单（如余额不足）时按错误码判断，不可重试的直接拒绝，不反复打上游。
func TestSubmitToUpstream_HTTPErrorWithNonRetryableCode_Rejects(t *testing.T) {
	db := setupProcurementTestDB(t)
	order := createProcTestOrder(t, db, "PROC-HTTP402-001", constants.OrderStatusFulfilling, constants.FulfillmentTypeUpstream)
	pm := &mappingdomain.Mapping{ConnectionID: 1, LocalProductID: 1, UpstreamProductID: 101, IsActive: true}
	db.Create(pm)
	db.Create(&mappingdomain.SKUMapping{ProductMappingID: pm.ID, LocalSKUID: 1, UpstreamSKUID: 201, UpstreamIsActive: true})

	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		w.Header().Set("Content-Type", "application/json")
		w.WriteHeader(http.StatusPaymentRequired)
		json.NewEncoder(w).Encode(map[string]any{"ok": false, "error_code": "insufficient_balance", "error_message": "balance not enough"})
	}))
	defer server.Close()

	connSvc := newTestSiteConnectionService(db, "test-key", t.TempDir())
	conn, _ := connSvc.Create(siteconnectionapp.CreateInput{
		Name: "test-upstream", BaseURL: server.URL,
		ApiKey: "key", ApiSecret: "secret", Protocol: constants.ConnectionProtocolDujiaoNext,
	})
	proc := createTestProcurementOrder(t, db, conn.ID, order.ID, order.OrderNo, "pending")
	_ = newTestProcurementService(db, connSvc).SubmitToUpstream(proc.ID)

	var updated ProcurementOrder
	db.First(&updated, proc.ID)
	if updated.Status != constants.ProcurementStatusRejected || updated.RetryCount != 0 {
		t.Fatalf("expected rejected without retries, got status=%q retry_count=%d", updated.Status, updated.RetryCount)
	}
}

// 本地订单已退款时（如巡检补投历史积压单），不能再去上游下单扣款。
func TestSubmitToUpstream_LocalOrderRefunded_RejectsWithoutCallingUpstream(t *testing.T) {
	db := setupProcurementTestDB(t)
	order := createProcTestOrder(t, db, "PROC-REFUNDED-001", constants.OrderStatusRefunded, constants.FulfillmentTypeUpstream)
	pm := &mappingdomain.Mapping{ConnectionID: 1, LocalProductID: 1, UpstreamProductID: 101, IsActive: true}
	db.Create(pm)
	db.Create(&mappingdomain.SKUMapping{ProductMappingID: pm.ID, LocalSKUID: 1, UpstreamSKUID: 201, UpstreamIsActive: true})

	called := false
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		called = true
		w.Header().Set("Content-Type", "application/json")
		json.NewEncoder(w).Encode(map[string]any{"ok": true, "order_id": 999, "order_no": "UP-999"})
	}))
	defer server.Close()

	connSvc := newTestSiteConnectionService(db, "test-key", t.TempDir())
	conn, _ := connSvc.Create(siteconnectionapp.CreateInput{
		Name: "test-upstream", BaseURL: server.URL,
		ApiKey: "key", ApiSecret: "secret", Protocol: constants.ConnectionProtocolDujiaoNext,
	})
	proc := createTestProcurementOrder(t, db, conn.ID, order.ID, order.OrderNo, "failed")
	if err := newTestProcurementService(db, connSvc).SubmitToUpstream(proc.ID); err != nil {
		t.Fatalf("SubmitToUpstream: %v", err)
	}

	var updated ProcurementOrder
	db.First(&updated, proc.ID)
	if called || updated.Status != constants.ProcurementStatusRejected {
		t.Fatalf("expected rejected without upstream call, got called=%v status=%q", called, updated.Status)
	}
}

func TestSubmitToUpstream_InvalidMappingNeverCallsUpstream(t *testing.T) {
	cases := []struct {
		name      string
		protocol  string
		status    string
		active    bool
		skuExists bool
		skuParent uint
	}{
		{name: "mapping paused", status: mappingdomain.UpstreamStatusActive, active: false, skuExists: true},
		{name: "Faka identity mismatch paused by mapping sync", protocol: constants.ConnectionProtocolFaka, status: mappingdomain.UpstreamStatusIdentityError, active: false, skuExists: true},
		{name: "Faka identity error cannot be reactivated by toggling active", protocol: constants.ConnectionProtocolFaka, status: mappingdomain.UpstreamStatusIdentityError, active: true, skuExists: true},
		{name: "upstream inactive", status: mappingdomain.UpstreamStatusInactive, active: true, skuExists: true},
		{name: "upstream deleted", status: mappingdomain.UpstreamStatusDeleted, active: true, skuExists: true},
		{name: "SKU missing", status: mappingdomain.UpstreamStatusActive, active: true},
		{name: "SKU belongs to another mapping", status: mappingdomain.UpstreamStatusActive, active: true, skuExists: true, skuParent: 999},
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			db := setupProcurementTestDB(t)
			order := createProcTestOrder(t, db, "PROC-INVALID-"+tc.name, constants.OrderStatusFulfilling, constants.FulfillmentTypeUpstream)
			pm := &mappingdomain.Mapping{
				ConnectionID: 1, LocalProductID: 1, UpstreamProductID: 101,
				UpstreamStatus: tc.status, IsActive: tc.active,
			}
			if err := db.Create(pm).Error; err != nil {
				t.Fatal(err)
			}
			if !tc.active {
				if err := db.Model(pm).Update("is_active", false).Error; err != nil {
					t.Fatal(err)
				}
			}
			if tc.skuExists {
				parentID := pm.ID
				if tc.skuParent != 0 {
					parentID = tc.skuParent
				}
				if err := db.Create(&mappingdomain.SKUMapping{ProductMappingID: parentID, LocalSKUID: 1, UpstreamSKUID: 201, UpstreamIsActive: true}).Error; err != nil {
					t.Fatal(err)
				}
			}

			called := false
			server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
				called = true
				w.Header().Set("Content-Type", "application/json")
				_ = json.NewEncoder(w).Encode(map[string]any{"ok": true, "order_id": 999, "order_no": "UP-999"})
			}))
			defer server.Close()
			protocol := tc.protocol
			if protocol == "" {
				protocol = constants.ConnectionProtocolDujiaoNext
			}
			connSvc := newTestSiteConnectionService(db, "test-key", t.TempDir())
			conn, err := connSvc.Create(siteconnectionapp.CreateInput{
				Name: "test-upstream", BaseURL: server.URL, ApiKey: "key", ApiSecret: "secret",
				Protocol: protocol,
			})
			if err != nil {
				t.Fatal(err)
			}
			if conn.ID != pm.ConnectionID {
				t.Fatalf("fixture connection id=%d, mapping connection id=%d", conn.ID, pm.ConnectionID)
			}
			proc := createTestProcurementOrder(t, db, conn.ID, order.ID, order.OrderNo, constants.ProcurementStatusPending)
			if err := newTestProcurementService(db, connSvc).SubmitToUpstream(proc.ID); err != nil {
				t.Fatalf("SubmitToUpstream: %v", err)
			}
			var updated ProcurementOrder
			if err := db.First(&updated, proc.ID).Error; err != nil {
				t.Fatal(err)
			}
			if called || updated.Status != constants.ProcurementStatusRejected || updated.RetryCount != 0 {
				t.Fatalf("called=%v status=%q retries=%d, want no call and permanent rejection", called, updated.Status, updated.RetryCount)
			}
		})
	}
}

func TestRetryManual_DoesNotRequeueWhenMappingIsPaused(t *testing.T) {
	db := setupProcurementTestDB(t)
	order := createProcTestOrder(t, db, "PROC-MANUAL-PAUSED", constants.OrderStatusFulfilling, constants.FulfillmentTypeUpstream)
	pm := &mappingdomain.Mapping{ConnectionID: 1, LocalProductID: 1, UpstreamProductID: 101, IsActive: true}
	if err := db.Create(pm).Error; err != nil {
		t.Fatal(err)
	}
	db.Create(&mappingdomain.SKUMapping{ProductMappingID: pm.ID, LocalSKUID: 1, UpstreamSKUID: 201, UpstreamIsActive: true})
	if err := db.Model(pm).Update("is_active", false).Error; err != nil {
		t.Fatal(err)
	}
	connSvc := newTestSiteConnectionService(db, "test-key", t.TempDir())
	conn, err := connSvc.Create(siteconnectionapp.CreateInput{
		Name: "test-upstream", BaseURL: "http://127.0.0.1:1", ApiKey: "key", ApiSecret: "secret",
		Protocol: constants.ConnectionProtocolDujiaoNext,
	})
	if err != nil {
		t.Fatal(err)
	}
	proc := createTestProcurementOrder(t, db, conn.ID, order.ID, order.OrderNo, constants.ProcurementStatusFailed)
	svc := newTestProcurementService(db, connSvc)
	if err := svc.RetryManual(proc.ID); err == nil {
		t.Fatal("expected manual retry to reject paused mapping")
	}
	var updated ProcurementOrder
	if err := db.First(&updated, proc.ID).Error; err != nil {
		t.Fatal(err)
	}
	if updated.Status != constants.ProcurementStatusRejected || updated.RetryCount != 0 {
		t.Fatalf("status=%q retries=%d, want rejected without retries", updated.Status, updated.RetryCount)
	}
}

func TestSubmitToUpstream_MappingPausedAfterProcurementCreationRejectsWithoutCallingUpstream(t *testing.T) {
	db := setupProcurementTestDB(t)
	order := createProcTestOrder(t, db, "PROC-PAUSE-AFTER-CREATE", constants.OrderStatusFulfilling, constants.FulfillmentTypeUpstream)
	pm := &mappingdomain.Mapping{ConnectionID: 1, LocalProductID: 1, UpstreamProductID: 101, IsActive: true}
	if err := db.Create(pm).Error; err != nil {
		t.Fatal(err)
	}
	if err := db.Create(&mappingdomain.SKUMapping{ProductMappingID: pm.ID, LocalSKUID: 1, UpstreamSKUID: 201, UpstreamIsActive: true}).Error; err != nil {
		t.Fatal(err)
	}
	called := false
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, _ *http.Request) {
		called = true
		w.Header().Set("Content-Type", "application/json")
		_ = json.NewEncoder(w).Encode(map[string]any{"ok": true})
	}))
	defer server.Close()
	connSvc := newTestSiteConnectionService(db, "test-key", t.TempDir())
	conn, err := connSvc.Create(siteconnectionapp.CreateInput{
		Name: "test-upstream", BaseURL: server.URL, ApiKey: "key", ApiSecret: "secret",
		Protocol: constants.ConnectionProtocolDujiaoNext,
	})
	if err != nil {
		t.Fatal(err)
	}
	proc := createTestProcurementOrder(t, db, conn.ID, order.ID, order.OrderNo, constants.ProcurementStatusPending)
	if err := db.Model(pm).Update("is_active", false).Error; err != nil {
		t.Fatal(err)
	}
	if err := newTestProcurementService(db, connSvc).SubmitToUpstream(proc.ID); err != nil {
		t.Fatalf("SubmitToUpstream: %v", err)
	}
	var updated ProcurementOrder
	if err := db.First(&updated, proc.ID).Error; err != nil {
		t.Fatal(err)
	}
	if called || updated.Status != constants.ProcurementStatusRejected || updated.RetryCount != 0 {
		t.Fatalf("called=%v status=%q retries=%d, want no upstream call and permanent rejection", called, updated.Status, updated.RetryCount)
	}
}

// 下单响应回来前采购单已被回调推进或被管理员取消：不回退状态，但必须补记上游订单号供对账与退款同步。
func TestSubmitToUpstream_StatusChangedDuringRequest_KeepsStatusAndRecordsUpstreamOrder(t *testing.T) {
	for _, concurrent := range []string{constants.ProcurementStatusFulfilled, constants.ProcurementStatusCanceled} {
		t.Run(concurrent, func(t *testing.T) {
			db := setupProcurementTestDB(t)
			order := createProcTestOrder(t, db, "PROC-RACE-"+concurrent, constants.OrderStatusPaid, constants.FulfillmentTypeUpstream)
			pm := &mappingdomain.Mapping{ConnectionID: 1, LocalProductID: 1, UpstreamProductID: 101, IsActive: true}
			db.Create(pm)
			db.Create(&mappingdomain.SKUMapping{ProductMappingID: pm.ID, LocalSKUID: 1, UpstreamSKUID: 201, UpstreamIsActive: true})

			var procID uint
			server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
				db.Model(&ProcurementOrder{}).Where("id = ?", procID).Update("status", concurrent)
				w.Header().Set("Content-Type", "application/json")
				json.NewEncoder(w).Encode(map[string]any{"ok": true, "order_id": 999, "order_no": "UP-999", "amount": "50.00", "currency": "CNY"})
			}))
			defer server.Close()

			connSvc := newTestSiteConnectionService(db, "test-key", t.TempDir())
			conn, _ := connSvc.Create(siteconnectionapp.CreateInput{
				Name: "test-upstream", BaseURL: server.URL,
				ApiKey: "key", ApiSecret: "secret", Protocol: constants.ConnectionProtocolDujiaoNext,
			})
			procID = createTestProcurementOrder(t, db, conn.ID, order.ID, order.OrderNo, "pending").ID
			if err := newTestProcurementService(db, connSvc).SubmitToUpstream(procID); err != nil {
				t.Fatalf("SubmitToUpstream: %v", err)
			}

			var updated ProcurementOrder
			db.First(&updated, procID)
			if updated.Status != concurrent || updated.UpstreamOrderID != 999 || updated.UpstreamOrderNo != "UP-999" {
				t.Fatalf("expected status=%q with upstream order recorded, got status=%q upstream_order_id=%d no=%q",
					concurrent, updated.Status, updated.UpstreamOrderID, updated.UpstreamOrderNo)
			}
		})
	}
}

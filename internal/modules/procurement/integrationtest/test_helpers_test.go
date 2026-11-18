package procurement_test

import (
	"errors"
	"fmt"
	"net/http"
	"net/http/httptest"
	"path/filepath"
	"sync/atomic"
	"testing"
	"time"

	fulfillmentdomain "github.com/dujiao-next/internal/modules/fulfillment/domain"
	orderdomain "github.com/dujiao-next/internal/modules/order/domain"
	ordergormstore "github.com/dujiao-next/internal/modules/order/infrastructure/gormstore"

	mappingdomain "github.com/dujiao-next/internal/modules/catalog/mapping/domain"

	siteconnectiondomain "github.com/dujiao-next/internal/modules/siteconnection/domain"

	procurementaudit "github.com/dujiao-next/internal/bootstrap/procurementaudit"
	"github.com/dujiao-next/internal/config"
	"github.com/dujiao-next/internal/constants"
	mappinggormstore "github.com/dujiao-next/internal/modules/catalog/mapping/infrastructure/gormstore"
	procurementapp "github.com/dujiao-next/internal/modules/procurement/application"
	procurementcontract "github.com/dujiao-next/internal/modules/procurement/contract"
	procurementdomain "github.com/dujiao-next/internal/modules/procurement/domain"
	procurementgormstore "github.com/dujiao-next/internal/modules/procurement/infrastructure/gormstore"
	procurementmapping "github.com/dujiao-next/internal/modules/procurement/infrastructure/mappingreader"
	procurementorder "github.com/dujiao-next/internal/modules/procurement/infrastructure/orderreader"
	procurementupstream "github.com/dujiao-next/internal/modules/procurement/infrastructure/upstreamgateway"
	siteconnectionapp "github.com/dujiao-next/internal/modules/siteconnection/application"
	siteconnectiongormstore "github.com/dujiao-next/internal/modules/siteconnection/infrastructure/gormstore"
	"github.com/dujiao-next/internal/platform/database/gormdb"
	"github.com/dujiao-next/internal/shared/jsonmap"
	"github.com/dujiao-next/internal/shared/money"

	"github.com/glebarez/sqlite"
	"github.com/shopspring/decimal"
	"gorm.io/gorm"
)

type ListFilter = procurementcontract.ListFilter
type ProcurementOrder = procurementdomain.Order

type fakaCASFixture struct {
	db         *gorm.DB
	proc       *procurementdomain.Order
	svc        *procurementapp.Service
	queue      *countingProcurementEnqueuer
	tradeCalls *atomic.Int32
}

func newFakaCASFixture(t *testing.T, repo *casErrorOnceRepository, confirmers ...procurementcontract.IndependentEvidenceConfirmer) *fakaCASFixture {
	t.Helper()
	db := setupProcurementTemporaryFileDB(t)
	order := createProcTestOrder(t, db, "PROC-FAKA-CAS-FAULT", constants.OrderStatusPaid, constants.FulfillmentTypeUpstream)
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
			_, _ = w.Write([]byte(`{"code":200,"data":{"tradeNo":"Faka-CAS-1","amount":"8.25"}}`))
		default:
			t.Errorf("unexpected Faka endpoint %s", r.URL.Path)
			w.WriteHeader(http.StatusNotFound)
		}
	}))
	t.Cleanup(server.Close)
	connections := newTestSiteConnectionService(db, "test-key", t.TempDir())
	conn, err := connections.Create(siteconnectionapp.CreateInput{Name: "faka-cas", BaseURL: server.URL, ApiKey: "id", ApiSecret: "key", Protocol: constants.ConnectionProtocolFaka})
	if err != nil {
		t.Fatal(err)
	}
	proc := createTestProcurementOrder(t, db, conn.ID, order.ID, order.OrderNo, constants.ProcurementStatusPending)
	baseRepo := procurementgormstore.New(db)
	repo.Repository = baseRepo
	queue := &countingProcurementEnqueuer{}
	confirmer := procurementcontract.IndependentEvidenceConfirmer(acceptingEvidenceConfirmer{})
	if len(confirmers) > 0 {
		confirmer = confirmers[0]
	}
	svc := newTestProcurementServiceWithRepository(db, connections, repo, queue, confirmer, procurementaudit.New(db))
	return &fakaCASFixture{db: db, proc: proc, svc: svc, queue: queue, tradeCalls: &trades}
}

var (
	ErrExists = procurementcontract.ErrExists
)

func newTestSiteConnectionService(db *gorm.DB, secretKey, uploadsDir string) *siteconnectionapp.Service {
	return siteconnectionapp.NewService(siteconnectiongormstore.New(db), secretKey, uploadsDir)
}

func setupProcurementTestDB(t *testing.T) *gorm.DB {
	t.Helper()
	dsn := fmt.Sprintf("file:procurement_test_%d?mode=memory&cache=shared", time.Now().UnixNano())
	db, err := gorm.Open(sqlite.Open(dsn), &gorm.Config{
		DisableForeignKeyConstraintWhenMigrating: true,
	})
	if err != nil {
		t.Fatalf("open sqlite failed: %v", err)
	}
	if err := db.AutoMigrate(
		&orderdomain.Order{},
		&orderdomain.OrderItem{},
		&orderdomain.OrderRefundRecord{},
		&fulfillmentdomain.Fulfillment{},
		&procurementdomain.Order{},
		&procurementdomain.TradeResponseJournalEntry{},
		&siteconnectiondomain.Connection{},
		&mappingdomain.Mapping{},
		&mappingdomain.SKUMapping{},
	); err != nil {
		t.Fatalf("auto migrate failed: %v", err)
	}
	gormdb.DB = db
	return db
}

func setupProcurementTemporaryFileDB(t *testing.T) *gorm.DB {
	t.Helper()
	db, err := gorm.Open(sqlite.Open(filepath.Join(t.TempDir(), "procurement.db")), &gorm.Config{DisableForeignKeyConstraintWhenMigrating: true})
	if err != nil {
		t.Fatal(err)
	}
	if err := db.AutoMigrate(&orderdomain.Order{}, &orderdomain.OrderItem{}, &orderdomain.OrderRefundRecord{}, &fulfillmentdomain.Fulfillment{}, &procurementdomain.Order{}, &procurementdomain.TradeResponseJournalEntry{}, &siteconnectiondomain.Connection{}, &mappingdomain.Mapping{}, &mappingdomain.SKUMapping{}); err != nil {
		t.Fatal(err)
	}
	sqlDB, err := db.DB()
	if err != nil {
		t.Fatal(err)
	}
	sqlDB.SetMaxOpenConns(1)
	t.Cleanup(func() { _ = sqlDB.Close() })
	gormdb.DB = db
	return db
}

func createProcTestOrder(t *testing.T, db *gorm.DB, orderNo, status, fulfillmentType string) *orderdomain.Order {
	t.Helper()
	order := &orderdomain.Order{
		OrderNo:        orderNo,
		UserID:         1,
		Status:         status,
		Currency:       "CNY",
		OriginalAmount: money.FromDecimal(decimal.NewFromInt(100)),
		TotalAmount:    money.FromDecimal(decimal.NewFromInt(100)),
	}
	if err := db.Create(order).Error; err != nil {
		t.Fatalf("create order failed: %v", err)
	}
	item := &orderdomain.OrderItem{
		OrderID:         order.ID,
		ProductID:       1,
		SKUID:           1,
		Quantity:        1,
		FulfillmentType: fulfillmentType,
		TitleJSON:       jsonmap.JSON{"zh-CN": "Test Product"},
		UnitPrice:       money.FromDecimal(decimal.NewFromInt(100)),
		TotalPrice:      money.FromDecimal(decimal.NewFromInt(100)),
	}
	if err := db.Create(item).Error; err != nil {
		t.Fatalf("create order item failed: %v", err)
	}
	var loaded orderdomain.Order
	if err := db.Preload("Items").First(&loaded, order.ID).Error; err != nil {
		t.Fatalf("reload order failed: %v", err)
	}
	return &loaded
}

func createTestProcurementOrder(t *testing.T, db *gorm.DB, connID, localOrderID uint, localOrderNo, status string) *ProcurementOrder {
	t.Helper()
	order := &ProcurementOrder{
		ConnectionID:    connID,
		LocalOrderID:    localOrderID,
		LocalOrderNo:    localOrderNo,
		Status:          status,
		LocalSellAmount: money.FromDecimal(decimal.NewFromInt(100)),
		Currency:        "CNY",
		TraceID:         "test-trace-id",
	}
	if err := db.Create(order).Error; err != nil {
		t.Fatalf("create procurement order failed: %v", err)
	}
	return order
}

func newTestProcurementService(db *gorm.DB, connections *siteconnectionapp.Service, audits ...procurementcontract.ManualDecisionAudit) *procurementapp.Service {
	return newTestProcurementServiceWithFailureNotifier(db, connections, nil, audits...)
}

func newTestProcurementServiceWithRepository(db *gorm.DB, connections *siteconnectionapp.Service, repository procurementcontract.Repository, queue procurementcontract.Enqueuer, confirmer procurementcontract.IndependentEvidenceConfirmer, audits ...procurementcontract.ManualDecisionAudit) *procurementapp.Service {
	orders := ordergormstore.New(db, "test-guest-credential-secret-with-32-bytes")
	var audit procurementcontract.ManualDecisionAudit
	if len(audits) > 0 {
		audit = audits[0]
		if recording, ok := audit.(*recordingProcurementDecisionAudit); ok {
			recording.db = db
		}
	}
	return procurementapp.NewService(procurementapp.Options{
		Repository: repository, Orders: procurementorder.New(orders), Queue: queue,
		ProductMappings:   procurementmapping.NewProducts(mappinggormstore.NewMappingStore(db)),
		SKUMappings:       procurementmapping.NewSKUs(mappinggormstore.NewSKUMappingStore(db)),
		Connections:       procurementupstream.New(connections),
		OrderLifecycle:    procurementgormstore.NewLifecycle(db, nil, nil, config.EmailConfig{}),
		DecisionAudit:     audit,
		EvidenceStore:     newMemoryEvidenceStore(),
		EvidenceConfirmer: confirmer,
	})
}

type memoryEvidenceStore struct {
	events map[string]procurementcontract.EvidenceEvent
}

func newMemoryEvidenceStore() *memoryEvidenceStore {
	return &memoryEvidenceStore{events: map[string]procurementcontract.EvidenceEvent{}}
}
func (s *memoryEvidenceStore) append(event procurementcontract.EvidenceEvent) error {
	if existing, ok := s.events[event.EventID]; ok {
		if existing == event {
			return nil
		}
		return errors.New("evidence conflict")
	}
	s.events[event.EventID] = event
	return nil
}
func (s *memoryEvidenceStore) AppendIntent(event procurementcontract.EvidenceEvent) error {
	return s.append(event)
}
func (s *memoryEvidenceStore) AppendResponse(event procurementcontract.EvidenceEvent) error {
	return s.append(event)
}
func (s *memoryEvidenceStore) GetByEventID(id string) (*procurementcontract.EvidenceEvent, error) {
	event, ok := s.events[id]
	if !ok {
		return nil, nil
	}
	return &event, nil
}
func (s *memoryEvidenceStore) GetByTraceID(trace string) ([]procurementcontract.EvidenceEvent, error) {
	var out []procurementcontract.EvidenceEvent
	for _, event := range s.events {
		if event.TraceID == trace {
			out = append(out, event)
		}
	}
	return out, nil
}
func (s *memoryEvidenceStore) ListPending() ([]procurementcontract.EvidenceEvent, error) {
	var out []procurementcontract.EvidenceEvent
	for _, event := range s.events {
		if event.EventType == "trade_response" {
			out = append(out, event)
		}
	}
	return out, nil
}

type acceptingEvidenceConfirmer struct{}

func (acceptingEvidenceConfirmer) ConfirmIndependent(string) error { return nil }

type errorEvidenceConfirmer struct{ err error }

func (c errorEvidenceConfirmer) ConfirmIndependent(string) error { return c.err }

type countingProcurementEnqueuer struct{ submit, poll int }

func (e *countingProcurementEnqueuer) EnqueueSubmit(uint, time.Duration) error {
	e.submit++
	return nil
}
func (e *countingProcurementEnqueuer) EnqueuePoll(uint, time.Duration) error { e.poll++; return nil }

func newTestProcurementServiceWithFailureNotifier(db *gorm.DB, connections *siteconnectionapp.Service, notifications procurementcontract.FailureNotifier, audits ...procurementcontract.ManualDecisionAudit) *procurementapp.Service {
	orders := ordergormstore.New(db, "test-guest-credential-secret-with-32-bytes")
	var audit procurementcontract.ManualDecisionAudit
	if len(audits) > 0 {
		audit = audits[0]
		if recording, ok := audit.(*recordingProcurementDecisionAudit); ok {
			recording.db = db
		}
	}
	return procurementapp.NewService(procurementapp.Options{
		Repository:        procurementgormstore.New(db),
		Orders:            procurementorder.New(orders),
		ProductMappings:   procurementmapping.NewProducts(mappinggormstore.NewMappingStore(db)),
		SKUMappings:       procurementmapping.NewSKUs(mappinggormstore.NewSKUMappingStore(db)),
		Connections:       procurementupstream.New(connections),
		OrderLifecycle:    procurementgormstore.NewLifecycle(db, nil, nil, config.EmailConfig{}),
		Notifications:     notifications,
		DecisionAudit:     audit,
		EvidenceStore:     newMemoryEvidenceStore(),
		EvidenceConfirmer: acceptingEvidenceConfirmer{},
	})
}

type casErrorOnceRepository struct {
	procurementcontract.Repository
	failTargets     []string
	commitThenError bool
	failCount       int
	failCalls       int
}

func (r *casErrorOnceRepository) UpdateStatus(id uint, fromStatus, toStatus string, updates map[string]interface{}) (bool, error) {
	failTarget := false
	for _, target := range r.failTargets {
		if target == toStatus {
			failTarget = true
			break
		}
	}
	if failTarget && r.failCalls < r.failCount {
		r.failCalls++
		if r.commitThenError {
			if _, err := r.Repository.UpdateStatus(id, fromStatus, toStatus, updates); err != nil {
				return false, err
			}
		}
		return false, fmt.Errorf("injected database result uncertainty")
	}
	return r.Repository.UpdateStatus(id, fromStatus, toStatus, updates)
}

type recordingFailureNotifier struct {
	calls   int
	message string
}

func (n *recordingFailureNotifier) NotifyFailure(_ *procurementdomain.Order, message string) error {
	n.calls++
	n.message = message
	return nil
}

type recordingProcurementDecisionAudit struct {
	db                               *gorm.DB
	fail                             bool
	listErr                          error
	listEvents                       []procurementdomain.TradeResponseJournalEntry
	calls                            int
	operatorID, orderID              uint
	traceID, action, reason, tradeNo string
}

func (a *recordingProcurementDecisionAudit) AppendTradeResponseJournal(entry procurementdomain.TradeResponseJournalEntry) error {
	if a.fail {
		return fmt.Errorf("audit write failed")
	}
	if a.db != nil {
		if err := a.db.Create(&entry).Error; err != nil {
			return err
		}
	}
	a.calls++
	a.orderID, a.traceID, a.action = entry.ProcurementOrderID, entry.TraceID, entry.EventType
	a.tradeNo = entry.UpstreamTradeNo
	return nil
}

func (a *recordingProcurementDecisionAudit) ListTradeResponseJournalByTraceID(traceID string) ([]procurementdomain.TradeResponseJournalEntry, error) {
	if a.listErr != nil {
		return nil, a.listErr
	}
	if a.listEvents != nil {
		return append([]procurementdomain.TradeResponseJournalEntry(nil), a.listEvents...), nil
	}
	if a.db == nil {
		return nil, fmt.Errorf("test journal database unavailable")
	}
	var entries []procurementdomain.TradeResponseJournalEntry
	err := a.db.Where("trace_id = ?", traceID).Order("id ASC").Find(&entries).Error
	return entries, err
}

func (a *recordingProcurementDecisionAudit) RecordTradeResponseReconciliation(orderID uint, traceID, confirmedTradeNo, confirmedAmount, note string, operatorAdminID uint, respondedAt time.Time) error {
	return a.AppendTradeResponseJournal(procurementdomain.TradeResponseJournalEntry{ProcurementOrderID: orderID, TraceID: traceID, UpstreamTradeNo: confirmedTradeNo, Amount: confirmedAmount, ConfirmedTradeNo: confirmedTradeNo, ConfirmedAmount: confirmedAmount, OperatorAdminID: &operatorAdminID, ReconciliationNote: note, EventType: "manual_reconciliation_recorded", ResponseTime: respondedAt})
}

func (a *recordingProcurementDecisionAudit) RecordTradeEvidenceConflict(conflict procurementdomain.TradeEvidenceConflict) error {
	return nil
}
func (a *recordingProcurementDecisionAudit) GetTradeEvidenceConflictsByEventID(eventID string) ([]procurementdomain.TradeEvidenceConflict, error) {
	return nil, nil
}
func (a *recordingProcurementDecisionAudit) ResolveTradeEvidenceConflict(id, adminID uint, note string) error {
	return nil
}

func (a *recordingProcurementDecisionAudit) ResolveProcurementDecision(orderID uint, traceID, action, reason, tradeNo string, operatorID uint, toStatus string, updates map[string]interface{}) (bool, error) {
	if a.fail {
		return false, fmt.Errorf("audit write failed")
	}
	if a.db == nil {
		return false, fmt.Errorf("test audit database is unavailable")
	}
	updated, err := procurementgormstore.New(a.db).UpdateStatus(orderID, "awaiting_verification", toStatus, updates)
	if err != nil || !updated {
		return updated, err
	}
	return true, a.RecordProcurementDecision(operatorID, orderID, traceID, action, reason, tradeNo)
}

func (a *recordingProcurementDecisionAudit) RecordProcurementDecision(operatorID, orderID uint, traceID, action, reason, tradeNo string) error {
	a.calls++
	a.operatorID, a.orderID, a.traceID = operatorID, orderID, traceID
	a.action, a.reason, a.tradeNo = action, reason, tradeNo
	return nil
}

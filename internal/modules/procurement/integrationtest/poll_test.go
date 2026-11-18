package procurement_test

import (
	"encoding/json"
	"fmt"
	"io"
	"net/http"
	"net/http/httptest"
	"sync/atomic"
	"testing"
	"time"

	fulfillmentdomain "github.com/dujiao-next/internal/modules/fulfillment/domain"
	orderdomain "github.com/dujiao-next/internal/modules/order/domain"

	"github.com/dujiao-next/internal/constants"
	ordergormstore "github.com/dujiao-next/internal/modules/order/infrastructure/gormstore"
	procurementapp "github.com/dujiao-next/internal/modules/procurement/application"
	procurementcontract "github.com/dujiao-next/internal/modules/procurement/contract"
	procurementgormstore "github.com/dujiao-next/internal/modules/procurement/infrastructure/gormstore"
	procurementorder "github.com/dujiao-next/internal/modules/procurement/infrastructure/orderreader"
	procurementupstream "github.com/dujiao-next/internal/modules/procurement/infrastructure/upstreamgateway"
	siteconnectionapp "github.com/dujiao-next/internal/modules/siteconnection/application"
)

// ── PollUpstreamStatus test ──

func TestPollUpstreamStatus_Delivered(t *testing.T) {
	db := setupProcurementTestDB(t)

	order := createProcTestOrder(t, db, "PROC-POLL-001", constants.OrderStatusFulfilling, constants.FulfillmentTypeUpstream)

	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		w.Header().Set("Content-Type", "application/json")
		now := time.Now()
		json.NewEncoder(w).Encode(map[string]any{
			"order_id": 999,
			"order_no": "UP-999",
			"status":   "delivered",
			"amount":   "50.00",
			"currency": "CNY",
			"fulfillment": map[string]any{
				"type":         "auto",
				"status":       "delivered",
				"payload":      "KEY-001\nKEY-002",
				"delivered_at": now.Format(time.RFC3339),
			},
		})
	}))
	defer server.Close()

	connSvc := newTestSiteConnectionService(db, "test-key", t.TempDir())
	conn, _ := connSvc.Create(siteconnectionapp.CreateInput{
		Name: "poll-upstream", BaseURL: server.URL,
		ApiKey: "key", ApiSecret: "secret", Protocol: constants.ConnectionProtocolDujiaoNext,
	})

	proc := createTestProcurementOrder(t, db, conn.ID, order.ID, order.OrderNo, "accepted")
	db.Model(proc).Updates(map[string]interface{}{
		"upstream_order_id": uint(999),
		"upstream_order_no": "UP-999",
	})

	svc := newTestProcurementService(db, connSvc)

	if err := svc.PollUpstreamStatus(proc.ID); err != nil {
		t.Fatalf("PollUpstreamStatus: %v", err)
	}

	// 验证采购单状态 = fulfilled
	var updatedProc ProcurementOrder
	db.First(&updatedProc, proc.ID)
	if updatedProc.Status != "fulfilled" {
		t.Errorf("expected procurement status 'fulfilled', got %q", updatedProc.Status)
	}

	// 验证本地订单状态 = delivered
	var updatedOrder orderdomain.Order
	db.First(&updatedOrder, order.ID)
	if updatedOrder.Status != constants.OrderStatusDelivered {
		t.Errorf("expected order status %q, got %q", constants.OrderStatusDelivered, updatedOrder.Status)
	}
}

func TestPollUpstreamStatus_FulfilledMappedToDelivered(t *testing.T) {
	db := setupProcurementTestDB(t)

	order := createProcTestOrder(t, db, "PROC-POLL-FULLFILLED-001", constants.OrderStatusFulfilling, constants.FulfillmentTypeUpstream)

	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		w.Header().Set("Content-Type", "application/json")
		now := time.Now()
		_ = json.NewEncoder(w).Encode(map[string]any{
			"order_id": 1001,
			"order_no": "UP-1001",
			"status":   "fulfilled",
			"amount":   "50.00",
			"currency": "CNY",
			"fulfillment": map[string]any{
				"type":         "auto",
				"status":       "delivered",
				"payload":      "KEY-003\nKEY-004",
				"delivered_at": now.Format(time.RFC3339),
			},
		})
	}))
	defer server.Close()

	connSvc := newTestSiteConnectionService(db, "test-key", t.TempDir())
	conn, _ := connSvc.Create(siteconnectionapp.CreateInput{
		Name: "poll-upstream-fulfilled", BaseURL: server.URL,
		ApiKey: "key", ApiSecret: "secret", Protocol: constants.ConnectionProtocolDujiaoNext,
	})

	proc := createTestProcurementOrder(t, db, conn.ID, order.ID, order.OrderNo, "accepted")
	db.Model(proc).Updates(map[string]interface{}{
		"upstream_order_id": uint(1001),
		"upstream_order_no": "UP-1001",
	})

	svc := newTestProcurementService(db, connSvc)
	if err := svc.PollUpstreamStatus(proc.ID); err != nil {
		t.Fatalf("PollUpstreamStatus: %v", err)
	}

	var updatedProc ProcurementOrder
	db.First(&updatedProc, proc.ID)
	if updatedProc.Status != "fulfilled" {
		t.Errorf("expected procurement status 'fulfilled', got %q", updatedProc.Status)
	}

	var updatedOrder orderdomain.Order
	db.First(&updatedOrder, order.ID)
	if updatedOrder.Status != constants.OrderStatusDelivered {
		t.Errorf("expected order status %q, got %q", constants.OrderStatusDelivered, updatedOrder.Status)
	}
}

func TestFakaPollUsesPersistedStringTradeNoWithoutAutoDelivery(t *testing.T) {
	cases := []struct{ name, response string }{
		{"unpaid", `{"code":200,"data":{"status":0,"secret":"CARD-001","widget":null}}`},
		{"paid empty secret", `{"code":200,"data":{"status":1,"secret":"","widget":null}}`},
		{"paid wait prompt", `{"code":200,"data":{"status":1,"secret":"等待人工发货","widget":null}}`},
		{"paid unverified secret", `{"code":200,"data":{"status":1,"secret":"CARD-001","widget":{"email":"buyer@example.com"}}}`},
		{"malformed response", `{broken`},
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			db := setupProcurementTestDB(t)
			order := createProcTestOrder(t, db, "FAKA-QUERY-CHILD", constants.OrderStatusFulfilling, constants.FulfillmentTypeUpstream)
			var queryCalls, tradeCalls atomic.Int32
			server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
				switch r.URL.Path {
				case "/shared/commodity/query":
					queryCalls.Add(1)
					if err := r.ParseForm(); err != nil {
						t.Error(err)
					}
					if r.Form.Get("tradeNo") != "TR-string-abc" || r.Form.Get("app_key") != "" {
						t.Errorf("unsafe form: %v", r.Form)
					}
					_, _ = io.WriteString(w, tc.response)
				case "/shared/commodity/trade":
					tradeCalls.Add(1)
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
			proc := createTestProcurementOrder(t, db, conn.ID, order.ID, order.OrderNo, constants.ProcurementStatusAccepted)
			if err := db.Model(proc).Updates(map[string]any{"upstream_order_no": "TR-string-abc", "upstream_order_id": 0}).Error; err != nil {
				t.Fatal(err)
			}
			svc := newTestProcurementService(db, connSvc)
			if err := svc.PollUpstreamStatus(proc.ID); err != nil {
				t.Fatal(err)
			}
			if err := svc.PollUpstreamStatus(proc.ID); err != nil {
				t.Fatal(err)
			}
			var stored ProcurementOrder
			if err := db.First(&stored, proc.ID).Error; err != nil {
				t.Fatal(err)
			}
			var fulfillmentCount int64
			if err := db.Model(&fulfillmentdomain.Fulfillment{}).Where("order_id = ?", order.ID).Count(&fulfillmentCount).Error; err != nil {
				t.Fatal(err)
			}
			if stored.Status != constants.ProcurementStatusAccepted || fulfillmentCount != 0 || queryCalls.Load() != 2 || tradeCalls.Load() != 0 {
				t.Fatalf("status=%q fulfillments=%d queries=%d trades=%d", stored.Status, fulfillmentCount, queryCalls.Load(), tradeCalls.Load())
			}
		})
	}
}

func TestFakaManualTradeNoRecoveryQueriesWithoutRetrading(t *testing.T) {
	db := setupProcurementTestDB(t)
	order := createProcTestOrder(t, db, "FAKA-MANUAL-QUERY", constants.OrderStatusFulfilling, constants.FulfillmentTypeUpstream)
	var queryCalls, tradeCalls atomic.Int32
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		switch r.URL.Path {
		case "/shared/commodity/query":
			queryCalls.Add(1)
			if err := r.ParseForm(); err != nil {
				t.Error(err)
			}
			if r.Form.Get("tradeNo") != "TR-recovered-A" {
				t.Errorf("tradeNo=%q", r.Form.Get("tradeNo"))
			}
			_, _ = io.WriteString(w, `{"code":200,"data":{"status":0,"secret":"","widget":null}}`)
		case "/shared/commodity/trade":
			tradeCalls.Add(1)
		}
	}))
	defer server.Close()
	connSvc := newTestSiteConnectionService(db, "test-key", t.TempDir())
	conn, err := connSvc.Create(siteconnectionapp.CreateInput{Name: "faka", BaseURL: server.URL, ApiKey: "id", ApiSecret: "key", Protocol: constants.ConnectionProtocolFaka})
	if err != nil {
		t.Fatal(err)
	}
	proc := createTestProcurementOrder(t, db, conn.ID, order.ID, order.OrderNo, constants.ProcurementStatusAwaitingVerification)
	audit := &recordingProcurementDecisionAudit{}
	svc := newTestProcurementService(db, connSvc, audit)
	if err := svc.PollUpstreamStatus(proc.ID); err != nil {
		t.Fatal(err)
	}
	if queryCalls.Load() != 0 {
		t.Fatal("unknown procurement was queried without tradeNo")
	}
	if err := svc.RetryManual(proc.ID, procurementcontract.ManualResolution{Mode: "order_found", OperatorAdminID: 7, TradeNo: "TR-recovered-A", Reason: "verified in supplier console"}); err != nil {
		t.Fatal(err)
	}
	if err := svc.PollUpstreamStatus(proc.ID); err != nil {
		t.Fatal(err)
	}
	if queryCalls.Load() != 1 || tradeCalls.Load() != 0 || audit.calls != 1 {
		t.Fatalf("queries=%d trades=%d audits=%d", queryCalls.Load(), tradeCalls.Load(), audit.calls)
	}
}

func TestFakaAcceptedWithoutTradeNoStaysFrozenFromQueryAndTrade(t *testing.T) {
	db := setupProcurementTestDB(t)
	order := createProcTestOrder(t, db, "FAKA-MISSING-TRADE-NO", constants.OrderStatusFulfilling, constants.FulfillmentTypeUpstream)
	var calls atomic.Int32
	server := httptest.NewServer(http.HandlerFunc(func(http.ResponseWriter, *http.Request) { calls.Add(1) }))
	defer server.Close()
	connSvc := newTestSiteConnectionService(db, "test-key", t.TempDir())
	conn, err := connSvc.Create(siteconnectionapp.CreateInput{Name: "faka", BaseURL: server.URL, ApiKey: "id", ApiSecret: "key", Protocol: constants.ConnectionProtocolFaka})
	if err != nil {
		t.Fatal(err)
	}
	proc := createTestProcurementOrder(t, db, conn.ID, order.ID, order.OrderNo, constants.ProcurementStatusAccepted)
	svc := newTestProcurementService(db, connSvc)
	if err := svc.PollUpstreamStatus(proc.ID); err != nil {
		t.Fatal(err)
	}
	if calls.Load() != 0 {
		t.Fatalf("query or trade was sent without tradeNo: %d", calls.Load())
	}
	var stored ProcurementOrder
	if err := db.First(&stored, proc.ID).Error; err != nil {
		t.Fatal(err)
	}
	if stored.Status != constants.ProcurementStatusAwaitingVerification {
		t.Fatalf("status=%q", stored.Status)
	}
}

func TestFakaPeriodicAcceptedSweepUsesStringTradeNo(t *testing.T) {
	db := setupProcurementTestDB(t)
	order := createProcTestOrder(t, db, "FAKA-PERIODIC-QUERY", constants.OrderStatusFulfilling, constants.FulfillmentTypeUpstream)
	var queries atomic.Int32
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		if r.URL.Path != "/shared/commodity/query" {
			t.Errorf("unexpected endpoint %s", r.URL.Path)
			return
		}
		queries.Add(1)
		if err := r.ParseForm(); err != nil {
			t.Error(err)
		}
		if r.Form.Get("tradeNo") != "TR-periodic" {
			t.Errorf("tradeNo=%q", r.Form.Get("tradeNo"))
		}
		_, _ = io.WriteString(w, `{"code":200,"data":{"status":1,"secret":"CARD-001","widget":null}}`)
	}))
	defer server.Close()
	connSvc := newTestSiteConnectionService(db, "test-key", t.TempDir())
	conn, err := connSvc.Create(siteconnectionapp.CreateInput{Name: "faka", BaseURL: server.URL, ApiKey: "id", ApiSecret: "key", Protocol: constants.ConnectionProtocolFaka})
	if err != nil {
		t.Fatal(err)
	}
	proc := createTestProcurementOrder(t, db, conn.ID, order.ID, order.OrderNo, constants.ProcurementStatusAccepted)
	if err := db.Model(proc).Update("upstream_order_no", "TR-periodic").Error; err != nil {
		t.Fatal(err)
	}
	newTestProcurementService(db, connSvc).SyncAcceptedOrders()
	if queries.Load() != 1 {
		t.Fatalf("periodic Faka queries=%d", queries.Load())
	}
}

// 巡检必须覆盖全部 accepted 采购单，而不是只看最新的一页。
func TestSyncAcceptedOrders_WalksAllPages(t *testing.T) {
	db := setupProcurementTestDB(t)
	var calls atomic.Int64
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		calls.Add(1)
		w.Header().Set("Content-Type", "application/json")
		json.NewEncoder(w).Encode(map[string]any{"ok": true, "status": "processing"})
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
	const total = 250
	for i := 0; i < total; i++ {
		proc := createTestProcurementOrder(t, db, conn.ID, uint(i+1), fmt.Sprintf("PROC-SWEEP-%03d", i), constants.ProcurementStatusAccepted)
		db.Model(proc).Update("upstream_order_id", 1000+i)
	}

	newTestProcurementService(db, connSvc).SyncAcceptedOrders()
	if got := calls.Load(); got != total {
		t.Fatalf("expected %d upstream checks, got %d", total, got)
	}
}

type recordingEnqueuer struct{ submitted []uint }

func (r *recordingEnqueuer) EnqueueSubmit(id uint, _ time.Duration) error {
	r.submitted = append(r.submitted, id)
	return nil
}
func (r *recordingEnqueuer) EnqueuePoll(uint, time.Duration) error { return nil }

// 入队失败（如 Redis 重启）留下的 pending 采购单必须被巡检补投，否则订单永远不发货。
func TestSyncAcceptedOrders_RequeuesStalledSubmissions(t *testing.T) {
	db := setupProcurementTestDB(t)
	order := createProcTestOrder(t, db, "PROC-STALLED-001", constants.OrderStatusPaid, constants.FulfillmentTypeUpstream)
	stalled := createTestProcurementOrder(t, db, 1, order.ID, order.OrderNo, constants.ProcurementStatusPending)
	db.Model(stalled).UpdateColumn("updated_at", time.Now().Add(-time.Hour))
	fresh := createTestProcurementOrder(t, db, 1, order.ID+1, "PROC-STALLED-002", constants.ProcurementStatusPending)
	submitting := createTestProcurementOrder(t, db, 1, order.ID+2, "PROC-SUBMITTING-003", constants.ProcurementStatusSubmitting)
	unknown := createTestProcurementOrder(t, db, 1, order.ID+3, "PROC-UNKNOWN-004", constants.ProcurementStatusAwaitingVerification)
	db.Model(submitting).UpdateColumn("updated_at", time.Now().Add(-time.Hour))
	db.Model(unknown).UpdateColumn("updated_at", time.Now().Add(-time.Hour))

	queue := &recordingEnqueuer{}
	svc := procurementapp.NewService(procurementapp.Options{
		Repository:  procurementgormstore.New(db),
		Orders:      procurementorder.New(ordergormstore.New(db, "test-guest-credential-secret-with-32-bytes")),
		Connections: procurementupstream.New(newTestSiteConnectionService(db, "test-key", t.TempDir())),
		Queue:       queue,
	})
	svc.SyncAcceptedOrders()
	if len(queue.submitted) != 1 || queue.submitted[0] != stalled.ID {
		t.Fatalf("expected only stalled pending procurement %d requeued (fresh %d, submitting %d, unknown %d skipped), got %v", stalled.ID, fresh.ID, submitting.ID, unknown.ID, queue.submitted)
	}
}

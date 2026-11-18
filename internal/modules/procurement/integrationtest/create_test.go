package procurement_test

import (
	"context"
	"encoding/json"
	"testing"

	mappingdomain "github.com/dujiao-next/internal/modules/catalog/mapping/domain"
	orderdomain "github.com/dujiao-next/internal/modules/order/domain"
	procurementcontract "github.com/dujiao-next/internal/modules/procurement/contract"
	siteconnectiondomain "github.com/dujiao-next/internal/modules/siteconnection/domain"
	"github.com/dujiao-next/internal/shared/jsonmap"
	"github.com/dujiao-next/internal/upstream"

	"github.com/dujiao-next/internal/constants"
	procurementupstreamgateway "github.com/dujiao-next/internal/modules/procurement/infrastructure/upstreamgateway"
	siteconnectionapp "github.com/dujiao-next/internal/modules/siteconnection/application"
)

// ── CreateForOrder tests ──

func TestCreateForOrder_SkipsNonUpstreamItems(t *testing.T) {
	db := setupProcurementTestDB(t)

	order := createProcTestOrder(t, db, "PROC-SKIP-001", constants.OrderStatusPaid, constants.FulfillmentTypeAuto)

	connSvc := newTestSiteConnectionService(db, "test-key", t.TempDir())
	svc := newTestProcurementService(db, connSvc)

	if err := svc.CreateForOrder(order.ID); err != nil {
		t.Fatalf("CreateForOrder: %v", err)
	}

	// 验证没有创建采购单
	var count int64
	db.Model(&ProcurementOrder{}).Count(&count)
	if count != 0 {
		t.Errorf("expected no procurement orders for auto fulfillment, got %d", count)
	}
}

func TestCreateForOrder_IdempotentSkipsDuplicate(t *testing.T) {
	db := setupProcurementTestDB(t)

	order := createProcTestOrder(t, db, "PROC-DUP-001", constants.OrderStatusPaid, constants.FulfillmentTypeUpstream)
	connSvc := newTestSiteConnectionService(db, "test-key", t.TempDir())
	conn, err := connSvc.Create(siteconnectionapp.CreateInput{
		Name: "test-upstream", BaseURL: "http://127.0.0.1:1", ApiKey: "key", ApiSecret: "secret",
		Protocol: constants.ConnectionProtocolDujiaoNext,
	})
	if err != nil {
		t.Fatalf("create connection: %v", err)
	}
	pm := &mappingdomain.Mapping{ConnectionID: conn.ID, LocalProductID: 1, UpstreamProductID: 101, IsActive: true}
	db.Create(pm)
	db.Create(&mappingdomain.SKUMapping{ProductMappingID: pm.ID, LocalSKUID: 1, UpstreamSKUID: 201, UpstreamIsActive: true})

	svc := newTestProcurementService(db, connSvc)

	// 第一次创建成功
	if err := svc.CreateForOrder(order.ID); err != nil {
		t.Fatalf("first CreateForOrder: %v", err)
	}

	// 第二次应该返回 ErrExists
	err = svc.CreateForOrder(order.ID)
	if err != ErrExists {
		t.Errorf("expected ErrExists on duplicate, got: %v", err)
	}
}

func TestCreateForOrder_InactiveMappingDoesNotCreateProcurement(t *testing.T) {
	db := setupProcurementTestDB(t)
	order := createProcTestOrder(t, db, "PROC-INACTIVE-MAPPING", constants.OrderStatusPaid, constants.FulfillmentTypeUpstream)
	pm := &mappingdomain.Mapping{ConnectionID: 1, LocalProductID: 1, UpstreamProductID: 101, IsActive: true}
	if err := db.Create(pm).Error; err != nil {
		t.Fatal(err)
	}
	if err := db.Model(pm).Update("is_active", false).Error; err != nil {
		t.Fatal(err)
	}

	connSvc := newTestSiteConnectionService(db, "test-key", t.TempDir())
	svc := newTestProcurementService(db, connSvc)
	if err := svc.CreateForOrder(order.ID); err == nil {
		t.Fatal("expected inactive mapping to be rejected")
	}

	var count int64
	if err := db.Model(&ProcurementOrder{}).Count(&count).Error; err != nil {
		t.Fatal(err)
	}
	if count != 0 {
		t.Fatalf("created %d procurement records for inactive mapping", count)
	}
}

func TestCreateForOrder_RejectsMultipleItemsInOneChild(t *testing.T) {
	db := setupProcurementTestDB(t)
	order := createProcTestOrder(t, db, "PROC-MULTI-ITEM", constants.OrderStatusPaid, constants.FulfillmentTypeUpstream)
	second := orderdomain.OrderItem{
		OrderID: order.ID, ProductID: 2, SKUID: 2, Quantity: 1,
		FulfillmentType: constants.FulfillmentTypeUpstream,
		TitleJSON:       jsonmap.JSON{"zh-CN": "Second"},
	}
	if err := db.Create(&second).Error; err != nil {
		t.Fatal(err)
	}

	svc := newTestProcurementService(db, newTestSiteConnectionService(db, "test-key", t.TempDir()))
	if err := svc.CreateForOrder(order.ID); err == nil {
		t.Fatal("expected multi-item procurement order to be rejected")
	}
	var count int64
	db.Model(&ProcurementOrder{}).Count(&count)
	if count != 0 {
		t.Fatalf("created %d procurement records for unsupported multi-item order", count)
	}
}

func TestCreateForOrder_ParentWithSingleItemChildrenCreatesOneRecordPerChild(t *testing.T) {
	db := setupProcurementTestDB(t)
	connSvc := newTestSiteConnectionService(db, "test-key", t.TempDir())
	conn, err := connSvc.Create(siteconnectionapp.CreateInput{
		Name: "test-upstream", BaseURL: "http://127.0.0.1:1", ApiKey: "key", ApiSecret: "secret",
		Protocol: constants.ConnectionProtocolDujiaoNext,
	})
	if err != nil {
		t.Fatal(err)
	}
	parent := &orderdomain.Order{OrderNo: "PROC-PARENT", Status: constants.OrderStatusPaid, Currency: "CNY"}
	if err := db.Create(parent).Error; err != nil {
		t.Fatal(err)
	}
	child := createProcTestOrder(t, db, "PROC-CHILD", constants.OrderStatusPaid, constants.FulfillmentTypeUpstream)
	if err := db.Model(child).Update("parent_id", parent.ID).Error; err != nil {
		t.Fatal(err)
	}
	pm := &mappingdomain.Mapping{ConnectionID: conn.ID, LocalProductID: 1, UpstreamProductID: 101, IsActive: true}
	if err := db.Create(pm).Error; err != nil {
		t.Fatal(err)
	}
	if err := db.Create(&mappingdomain.SKUMapping{ProductMappingID: pm.ID, LocalSKUID: 1, UpstreamSKUID: 201, UpstreamIsActive: true}).Error; err != nil {
		t.Fatal(err)
	}

	svc := newTestProcurementService(db, connSvc)
	if err := svc.CreateForOrder(parent.ID); err != nil {
		t.Fatalf("CreateForOrder(parent): %v", err)
	}
	var count int64
	if err := db.Model(&ProcurementOrder{}).Where("local_order_id = ?", child.ID).Count(&count).Error; err != nil {
		t.Fatal(err)
	}
	if count != 1 {
		t.Fatalf("procurement records for single-item child = %d, want 1", count)
	}
}

func TestCreateForOrder_RejectsUnsafeMappings(t *testing.T) {
	cases := []struct {
		name       string
		protocol   string
		active     bool
		status     string
		code       string
		skuPresent bool
		skuParent  bool
	}{
		{name: "inactive mapping", protocol: constants.ConnectionProtocolDujiaoNext, active: false, status: mappingdomain.UpstreamStatusActive, skuPresent: true, skuParent: true},
		{name: "upstream inactive", protocol: constants.ConnectionProtocolDujiaoNext, active: true, status: mappingdomain.UpstreamStatusInactive, skuPresent: true, skuParent: true},
		{name: "upstream deleted", protocol: constants.ConnectionProtocolDujiaoNext, active: true, status: mappingdomain.UpstreamStatusDeleted, skuPresent: true, skuParent: true},
		{name: "Faka identity error cannot be reactivated by toggling active", protocol: constants.ConnectionProtocolFaka, active: true, status: mappingdomain.UpstreamStatusIdentityError, code: "mismatched-code", skuPresent: true, skuParent: true},
		{name: "faka code missing", protocol: constants.ConnectionProtocolFaka, active: true, status: mappingdomain.UpstreamStatusActive, skuPresent: true, skuParent: true},
		{name: "sku missing", protocol: constants.ConnectionProtocolDujiaoNext, active: true, status: mappingdomain.UpstreamStatusActive},
		{name: "sku attached to other mapping", protocol: constants.ConnectionProtocolDujiaoNext, active: true, status: mappingdomain.UpstreamStatusActive, skuPresent: true},
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			db := setupProcurementTestDB(t)
			order := createProcTestOrder(t, db, "PROC-UNSAFE-"+tc.name, constants.OrderStatusPaid, constants.FulfillmentTypeUpstream)
			connSvc := newTestSiteConnectionService(db, "test-key", t.TempDir())
			conn, err := connSvc.Create(siteconnectionapp.CreateInput{
				Name: "test-upstream", BaseURL: "http://127.0.0.1:1", ApiKey: "key", ApiSecret: "secret",
				Protocol: tc.protocol,
			})
			if err != nil {
				t.Fatal(err)
			}
			pm := &mappingdomain.Mapping{
				ConnectionID: conn.ID, LocalProductID: 1, UpstreamProductID: 101,
				UpstreamProductCode: tc.code, UpstreamStatus: tc.status, IsActive: true,
			}
			if err := db.Create(pm).Error; err != nil {
				t.Fatal(err)
			}
			if !tc.active {
				if err := db.Model(pm).Update("is_active", false).Error; err != nil {
					t.Fatal(err)
				}
			}
			if tc.skuPresent {
				parentID := uint(999)
				if tc.skuParent {
					parentID = pm.ID
				}
				if err := db.Create(&mappingdomain.SKUMapping{ProductMappingID: parentID, LocalSKUID: 1, UpstreamSKUID: 201, UpstreamIsActive: true}).Error; err != nil {
					t.Fatal(err)
				}
			}

			svc := newTestProcurementService(db, connSvc)
			if err := svc.CreateForOrder(order.ID); err == nil {
				t.Fatal("expected unsafe mapping to be rejected")
			}
			var count int64
			if err := db.Model(&ProcurementOrder{}).Count(&count).Error; err != nil {
				t.Fatal(err)
			}
			if count != 0 {
				t.Fatalf("created %d procurement records for invalid mapping", count)
			}
		})
	}
}

func TestCreateForOrder_ActiveFakaMappingWithCodeCreatesPendingRecord(t *testing.T) {
	db := setupProcurementTestDB(t)
	order := createProcTestOrder(t, db, "PROC-FAKA-MAPPED", constants.OrderStatusPaid, constants.FulfillmentTypeUpstream)
	connSvc := newTestSiteConnectionService(db, "test-key", t.TempDir())
	conn, err := connSvc.Create(siteconnectionapp.CreateInput{
		Name: "test-faka", BaseURL: "http://127.0.0.1:1", ApiKey: "app-id", ApiSecret: "app-key",
		Protocol: constants.ConnectionProtocolFaka,
	})
	if err != nil {
		t.Fatal(err)
	}
	pm := &mappingdomain.Mapping{
		ConnectionID: conn.ID, LocalProductID: 1, UpstreamProductID: 101,
		UpstreamProductCode: "persisted-code", UpstreamStatus: mappingdomain.UpstreamStatusActive, IsActive: true,
	}
	if err := db.Create(pm).Error; err != nil {
		t.Fatal(err)
	}
	if err := db.Create(&mappingdomain.SKUMapping{ProductMappingID: pm.ID, LocalSKUID: 1, UpstreamSKUID: 201, UpstreamIsActive: true}).Error; err != nil {
		t.Fatal(err)
	}
	if err := newTestProcurementService(db, connSvc).CreateForOrder(order.ID); err != nil {
		t.Fatalf("CreateForOrder: %v", err)
	}
	var proc ProcurementOrder
	if err := db.Where("local_order_id = ?", order.ID).First(&proc).Error; err != nil {
		t.Fatal(err)
	}
	if proc.Status != constants.ProcurementStatusPending {
		t.Fatalf("status=%q, want pending", proc.Status)
	}
}

type procurementGatewayProvider struct {
	connection *siteconnectiondomain.Connection
	adapter    upstream.Adapter
}

func (p procurementGatewayProvider) GetByID(uint) (*siteconnectiondomain.Connection, error) {
	return p.connection, nil
}
func (p procurementGatewayProvider) GetAdapter(*siteconnectiondomain.Connection) (upstream.Adapter, error) {
	return p.adapter, nil
}

type procurementGatewayCapture struct {
	request upstream.CreateUpstreamOrderReq
}

func (*procurementGatewayCapture) Ping(context.Context) (*upstream.PingResult, error) {
	return nil, nil
}
func (*procurementGatewayCapture) ListCategories(context.Context) (*upstream.CategoryListResult, error) {
	return nil, nil
}
func (*procurementGatewayCapture) ListProducts(context.Context, upstream.ListProductsOpts) (*upstream.ProductListResult, error) {
	return nil, nil
}
func (*procurementGatewayCapture) GetProduct(context.Context, uint) (*upstream.UpstreamProduct, error) {
	return nil, nil
}
func (a *procurementGatewayCapture) CreateOrder(_ context.Context, req upstream.CreateUpstreamOrderReq) (*upstream.CreateUpstreamOrderResp, error) {
	a.request = req
	return &upstream.CreateUpstreamOrderResp{OK: true}, nil
}
func (*procurementGatewayCapture) GetOrder(context.Context, uint) (*upstream.UpstreamOrderDetail, error) {
	return nil, nil
}
func (*procurementGatewayCapture) CancelOrder(context.Context, uint) error { return nil }
func (*procurementGatewayCapture) DownloadImage(context.Context, string) (string, error) {
	return "", nil
}

func TestProcurementGatewayPassesSharedCodeOnlyToFaka(t *testing.T) {
	for _, tc := range []struct {
		protocol string
		wantCode string
	}{
		{protocol: constants.ConnectionProtocolFaka, wantCode: "raw-faka-code"},
		{protocol: constants.ConnectionProtocolDujiaoNext},
	} {
		t.Run(tc.protocol, func(t *testing.T) {
			adapter := &procurementGatewayCapture{}
			gateway := procurementupstreamgateway.New(procurementGatewayProvider{
				connection: &siteconnectiondomain.Connection{ID: 1, Protocol: tc.protocol},
				adapter:    adapter,
			})
			connection, err := gateway.Open(1)
			if err != nil {
				t.Fatal(err)
			}
			_, err = connection.CreateOrder(context.Background(), procurementcontract.CreateOrderRequest{
				SKUID: 17, UpstreamProductCode: "raw-faka-code", Quantity: 2,
			})
			if err != nil {
				t.Fatal(err)
			}
			if adapter.request.SharedCode != tc.wantCode {
				t.Fatalf("shared_code=%q, want %q", adapter.request.SharedCode, tc.wantCode)
			}
			if adapter.request.SKUID != 17 {
				t.Fatalf("sku_id=%d, want 17", adapter.request.SKUID)
			}
			if tc.protocol == constants.ConnectionProtocolDujiaoNext {
				body, err := json.Marshal(adapter.request)
				if err != nil {
					t.Fatal(err)
				}
				var fields map[string]any
				if err := json.Unmarshal(body, &fields); err != nil {
					t.Fatal(err)
				}
				if _, exists := fields["shared_code"]; exists {
					t.Fatalf("Dujiao-Next request unexpectedly contains shared_code: %s", body)
				}
			}
		})
	}
}

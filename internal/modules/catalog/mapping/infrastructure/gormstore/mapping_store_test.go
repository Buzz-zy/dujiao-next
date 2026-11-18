package gormstore

import (
	"fmt"
	"path/filepath"
	"strings"
	"testing"
	"time"

	mappingdomain "github.com/dujiao-next/internal/modules/catalog/mapping/domain"

	siteconnectiondomain "github.com/dujiao-next/internal/modules/siteconnection/domain"

	productdomain "github.com/dujiao-next/internal/modules/catalog/product/domain"

	categorydomain "github.com/dujiao-next/internal/modules/catalog/category/domain"

	"github.com/dujiao-next/internal/constants"
	mappingcontract "github.com/dujiao-next/internal/modules/catalog/mapping/contract"
	"github.com/dujiao-next/internal/shared/jsonmap"
	"github.com/dujiao-next/internal/shared/money"

	"github.com/glebarez/sqlite"
	"github.com/shopspring/decimal"
	"gorm.io/gorm"
)

func setupMappingStoreTest(t *testing.T) (*MappingStore, *SKUMappingStore, *gorm.DB) {
	t.Helper()
	dsn := fmt.Sprintf("file:mapping_store_test_%d?mode=memory&cache=shared", time.Now().UnixNano())
	db, err := gorm.Open(sqlite.Open(dsn), &gorm.Config{})
	if err != nil {
		t.Fatalf("open sqlite failed: %v", err)
	}
	if err := db.AutoMigrate(
		&categorydomain.Category{},
		&productdomain.Product{},
		&productdomain.ProductSKU{},
		&siteconnectiondomain.Connection{},
		&mappingdomain.Mapping{},
		&mappingdomain.SKUMapping{},
	); err != nil {
		t.Fatalf("migrate mapping models failed: %v", err)
	}
	defaultCategory := categorydomain.Category{
		ID:       1,
		Slug:     "default-test-category",
		NameJSON: jsonmap.JSON{"zh-CN": "default"},
		IsActive: true,
	}
	if err := db.Create(&defaultCategory).Error; err != nil {
		t.Fatalf("seed default category failed: %v", err)
	}
	return NewMappingStore(db), NewSKUMappingStore(db), db
}

type legacyProductMappingSchema struct {
	ID                      uint   `gorm:"primarykey"`
	ConnectionID            uint   `gorm:"index;not null"`
	LocalProductID          uint   `gorm:"uniqueIndex;not null"`
	UpstreamProductID       uint   `gorm:"not null"`
	UpstreamFulfillmentType string `gorm:"type:varchar(20);not null;default:'manual'"`
	UpstreamStatus          string `gorm:"type:varchar(16);not null;default:'active';index"`
	IsActive                bool   `gorm:"not null;default:true"`
	LastSyncedAt            *time.Time
	CreatedAt               time.Time
	UpdatedAt               time.Time
	DeletedAt               *time.Time `gorm:"index"`
}

func (legacyProductMappingSchema) TableName() string { return "product_mappings" }

func TestMappingStorePersistsUpstreamProductCodeAcrossDatabaseReopen(t *testing.T) {
	dbPath := filepath.Join(t.TempDir(), "mapping.db")
	openDB := func() *gorm.DB {
		db, err := gorm.Open(sqlite.Open(dbPath), &gorm.Config{})
		if err != nil {
			t.Fatalf("open temp sqlite: %v", err)
		}
		return db
	}

	firstDB := openDB()
	if err := firstDB.AutoMigrate(&legacyProductMappingSchema{}); err != nil {
		t.Fatalf("create legacy mapping schema: %v", err)
	}
	legacy := &legacyProductMappingSchema{ConnectionID: 1, LocalProductID: 101, UpstreamProductID: 7, IsActive: true}
	if err := firstDB.Create(legacy).Error; err != nil {
		t.Fatalf("create legacy mapping: %v", err)
	}
	if err := firstDB.AutoMigrate(&mappingdomain.Mapping{}); err != nil {
		t.Fatalf("migrate legacy table with nullable code: %v", err)
	}
	var nullCodeRows int64
	if err := firstDB.Model(&mappingdomain.Mapping{}).Where("id = ? AND upstream_product_code IS NULL", legacy.ID).Count(&nullCodeRows).Error; err != nil {
		t.Fatalf("check legacy NULL code: %v", err)
	}
	if nullCodeRows != 1 {
		t.Fatalf("legacy rows with NULL code = %d, want 1", nullCodeRows)
	}
	store := NewMappingStore(firstDB)
	stableCode := strings.Repeat("faka-code-", 40)
	withCode := &mappingdomain.Mapping{
		ConnectionID: 1, LocalProductID: 102, UpstreamProductID: 8, UpstreamProductCode: stableCode, IsActive: true,
		FakaProcurementProfile: jsonmap.JSON{
			"raw_config": `{"sku":{}}`, "raw_widget": `null`,
			"raw_draft_status": `0`, "raw_password_status": `0`,
			"assessment_complete": true, "snapshot_valid": false,
			"capability": map[string]interface{}{"status": "unsupported", "can_auto_procure": false},
		},
	}
	if err := store.Create(withCode); err != nil {
		t.Fatalf("create coded mapping: %v", err)
	}
	if sqlDB, err := firstDB.DB(); err == nil {
		_ = sqlDB.Close()
	}

	reopened := openDB()
	defer func() {
		if sqlDB, err := reopened.DB(); err == nil {
			_ = sqlDB.Close()
		}
	}()
	var gotLegacy, gotCoded mappingdomain.Mapping
	if err := reopened.First(&gotLegacy, legacy.ID).Error; err != nil {
		t.Fatalf("read legacy row after reopen: %v", err)
	}
	if gotLegacy.UpstreamProductCode != "" {
		t.Fatalf("legacy code = %q, want empty", gotLegacy.UpstreamProductCode)
	}
	if err := reopened.First(&gotCoded, withCode.ID).Error; err != nil {
		t.Fatalf("read coded row after reopen: %v", err)
	}
	if gotCoded.UpstreamProductCode != stableCode {
		t.Fatalf("reopened code length/value mismatch: got length %d, want %d", len(gotCoded.UpstreamProductCode), len(stableCode))
	}
	capability, ok := gotCoded.FakaProcurementProfile["capability"].(map[string]interface{})
	if !ok || capability["status"] != "unsupported" || capability["can_auto_procure"] != false {
		t.Fatalf("reopened Faka capability = %#v", gotCoded.FakaProcurementProfile)
	}
	if gotCoded.FakaProcurementProfile["raw_config"] != `{"sku":{}}` || gotCoded.FakaProcurementProfile["raw_widget"] != `null` {
		t.Fatalf("reopened Faka raw configuration = %#v", gotCoded.FakaProcurementProfile)
	}
	if gotCoded.FakaProcurementProfile["raw_draft_status"] != `0` || gotCoded.FakaProcurementProfile["raw_password_status"] != `0` || gotCoded.FakaProcurementProfile["assessment_complete"] != true || gotCoded.FakaProcurementProfile["snapshot_valid"] != false {
		t.Fatalf("reopened Faka metadata = %#v", gotCoded.FakaProcurementProfile)
	}
}

func createMappedProduct(t *testing.T, db *gorm.DB, slug, title string) *productdomain.Product {
	t.Helper()
	product := &productdomain.Product{
		CategoryID:      1,
		Slug:            slug,
		TitleJSON:       jsonmap.JSON{"zh-CN": title},
		PriceAmount:     money.FromDecimal(decimal.NewFromInt(100)),
		PurchaseType:    constants.ProductPurchaseMember,
		FulfillmentType: constants.FulfillmentTypeUpstream,
		IsMapped:        true,
		IsActive:        true,
	}
	if err := db.Create(product).Error; err != nil {
		t.Fatalf("create product failed: %v", err)
	}
	return product
}

func createMapping(t *testing.T, store *MappingStore, connectionID, localProductID, upstreamProductID uint, upstreamStatus string, isActive bool) *mappingdomain.Mapping {
	t.Helper()
	mapping := &mappingdomain.Mapping{
		ConnectionID:      connectionID,
		LocalProductID:    localProductID,
		UpstreamProductID: upstreamProductID,
		UpstreamStatus:    upstreamStatus,
		IsActive:          true,
	}
	if err := store.Create(mapping); err != nil {
		t.Fatalf("create mapping failed: %v", err)
	}
	// IsActive 带 default:true 标签，Create 会忽略 false 零值，停用需走 Update
	if !isActive {
		mapping.IsActive = false
		if err := store.Update(mapping); err != nil {
			t.Fatalf("deactivate mapping failed: %v", err)
		}
	}
	return mapping
}

func TestMappingStoreListFiltersAndPaginates(t *testing.T) {
	store, _, db := setupMappingStoreTest(t)

	rechargeCard := createMappedProduct(t, db, "recharge-card", "上游充值卡")
	plainProduct := createMappedProduct(t, db, "plain-product", "普通商品")
	otherSite := createMappedProduct(t, db, "other-site", "另一站点商品")

	createMapping(t, store, 1, rechargeCard.ID, 1001, mappingdomain.UpstreamStatusActive, true)
	inactive := createMapping(t, store, 1, plainProduct.ID, 1002, mappingdomain.UpstreamStatusInactive, false)
	createMapping(t, store, 2, otherSite.ID, 2001, mappingdomain.UpstreamStatusActive, true)

	if _, total, err := store.List(mappingcontract.ListFilter{}); err != nil || total != 3 {
		t.Fatalf("list all: total=%d err=%v, want 3", total, err)
	}
	if _, total, err := store.List(mappingcontract.ListFilter{ConnectionID: 1}); err != nil || total != 2 {
		t.Fatalf("list by connection: total=%d err=%v, want 2", total, err)
	}
	rows, total, err := store.List(mappingcontract.ListFilter{UpstreamStatus: mappingdomain.UpstreamStatusInactive})
	if err != nil || total != 1 || len(rows) != 1 || rows[0].ID != inactive.ID {
		t.Fatalf("list by upstream status: rows=%d total=%d err=%v, want the inactive mapping", len(rows), total, err)
	}
	if _, total, err := store.List(mappingcontract.ListFilter{ProductStatus: "active"}); err != nil || total != 2 {
		t.Fatalf("list by product status active: total=%d err=%v, want 2", total, err)
	}
	if _, total, err := store.List(mappingcontract.ListFilter{ProductStatus: "inactive"}); err != nil || total != 1 {
		t.Fatalf("list by product status inactive: total=%d err=%v, want 1", total, err)
	}
	rows, total, err = store.List(mappingcontract.ListFilter{Search: "充值"})
	if err != nil || total != 1 || len(rows) != 1 || rows[0].LocalProductID != rechargeCard.ID {
		t.Fatalf("list by search: rows=%d total=%d err=%v, want the recharge card mapping", len(rows), total, err)
	}

	rows, total, err = store.List(mappingcontract.ListFilter{Page: 1, PageSize: 2})
	if err != nil || total != 3 || len(rows) != 2 {
		t.Fatalf("list page 1: rows=%d total=%d err=%v, want 2 rows of 3", len(rows), total, err)
	}
	rows, total, err = store.List(mappingcontract.ListFilter{Page: 2, PageSize: 2})
	if err != nil || total != 3 || len(rows) != 1 {
		t.Fatalf("list page 2: rows=%d total=%d err=%v, want 1 row of 3", len(rows), total, err)
	}
}

func TestMappingStoreDeleteByLocalProductRemovesSKUMappings(t *testing.T) {
	store, skuStore, db := setupMappingStoreTest(t)

	removed := createMappedProduct(t, db, "removed-product", "待删除商品")
	kept := createMappedProduct(t, db, "kept-product", "保留商品")
	removedMapping := createMapping(t, store, 1, removed.ID, 1001, mappingdomain.UpstreamStatusActive, true)
	keptMapping := createMapping(t, store, 1, kept.ID, 1002, mappingdomain.UpstreamStatusActive, true)

	for i, mappingID := range []uint{removedMapping.ID, removedMapping.ID, keptMapping.ID} {
		if err := skuStore.Create(&mappingdomain.SKUMapping{
			ProductMappingID: mappingID,
			LocalSKUID:       uint(100 + i),
			UpstreamSKUID:    uint(200 + i),
		}); err != nil {
			t.Fatalf("create sku mapping failed: %v", err)
		}
	}

	if err := store.DeleteByLocalProduct(removed.ID); err != nil {
		t.Fatalf("delete by local product failed: %v", err)
	}

	if mapping, err := store.GetByLocalProductID(removed.ID); err != nil || mapping != nil {
		t.Fatalf("removed mapping should be gone: mapping=%v err=%v", mapping, err)
	}
	if rows, err := skuStore.ListByProductMapping(removedMapping.ID); err != nil || len(rows) != 0 {
		t.Fatalf("removed sku mappings should be gone: rows=%d err=%v", len(rows), err)
	}
	var deletedMapping mappingdomain.Mapping
	if err := db.Where("id = ? AND deleted_at IS NOT NULL", removedMapping.ID).First(&deletedMapping).Error; err != nil {
		t.Fatalf("mapping must remain persisted with deleted_at: %v", err)
	}
	var deletedSKUCount int64
	if err := db.Model(&mappingdomain.SKUMapping{}).
		Where("product_mapping_id = ? AND deleted_at IS NOT NULL", removedMapping.ID).
		Count(&deletedSKUCount).Error; err != nil {
		t.Fatalf("count soft-deleted sku mappings: %v", err)
	}
	if deletedSKUCount != 2 {
		t.Fatalf("soft-deleted sku mappings = %d, want 2", deletedSKUCount)
	}
	if mapping, err := store.GetByLocalProductID(kept.ID); err != nil || mapping == nil {
		t.Fatalf("kept mapping should survive: mapping=%v err=%v", mapping, err)
	}
	if rows, err := skuStore.ListByProductMapping(keptMapping.ID); err != nil || len(rows) != 1 {
		t.Fatalf("kept sku mapping should survive: rows=%d err=%v", len(rows), err)
	}
}

func TestMappingStoreDeleteHidesMappingFromEveryReadPath(t *testing.T) {
	store, _, db := setupMappingStoreTest(t)
	product := createMappedProduct(t, db, "single-delete", "单条删除")
	mapping := createMapping(t, store, 9, product.ID, 9001, mappingdomain.UpstreamStatusActive, true)

	if err := store.Delete(mapping.ID); err != nil {
		t.Fatalf("delete mapping: %v", err)
	}

	if got, err := store.GetByID(mapping.ID); err != nil || got != nil {
		t.Fatalf("GetByID after delete = %#v, %v; want nil, nil", got, err)
	}
	if got, err := store.GetByConnectionAndUpstreamID(mapping.ConnectionID, mapping.UpstreamProductID); err != nil || got != nil {
		t.Fatalf("GetByConnectionAndUpstreamID after delete = %#v, %v; want nil, nil", got, err)
	}
	if rows, total, err := store.List(mappingcontract.ListFilter{}); err != nil || total != 0 || len(rows) != 0 {
		t.Fatalf("List after delete = rows %d total %d err %v; want empty", len(rows), total, err)
	}
	if rows, err := store.ListActiveByConnection(mapping.ConnectionID); err != nil || len(rows) != 0 {
		t.Fatalf("ListActiveByConnection after delete = rows %d err %v; want empty", len(rows), err)
	}
	if ids, err := store.ListUpstreamIDsByConnection(mapping.ConnectionID); err != nil || len(ids) != 0 {
		t.Fatalf("ListUpstreamIDsByConnection after delete = ids %v err %v; want empty", ids, err)
	}
}

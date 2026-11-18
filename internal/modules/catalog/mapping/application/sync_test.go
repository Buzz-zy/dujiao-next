package application

import (
	"context"
	"errors"
	"testing"
	"time"

	mappingcontract "github.com/dujiao-next/internal/modules/catalog/mapping/contract"
	mappingdomain "github.com/dujiao-next/internal/modules/catalog/mapping/domain"
	productdomain "github.com/dujiao-next/internal/modules/catalog/product/domain"
	siteconnectiondomain "github.com/dujiao-next/internal/modules/siteconnection/domain"
	"github.com/dujiao-next/internal/shared/jsonmap"
	"github.com/dujiao-next/internal/upstream"

	settingsintegration "github.com/dujiao-next/internal/modules/settings/schema/integration"
)

// fakeSettingsProvider 以固定同步间隔实现 SettingsProvider。
type fakeSettingsProvider struct {
	interval time.Duration
}

func (f fakeSettingsProvider) GetUpstreamSyncConfig(fallbackInterval string) (settingsintegration.UpstreamSyncConfig, error) {
	cfg := settingsintegration.DefaultUpstreamSyncConfig()
	cfg.IntervalMinutes = int(f.interval / time.Minute)
	return cfg, nil
}

func (f fakeSettingsProvider) GetUpstreamSyncInterval(fallbackInterval string) (time.Duration, error) {
	return f.interval, nil
}

func TestComputeFullSyncIntervalFloorsAt24h(t *testing.T) {
	svc := &Service{settings: fakeSettingsProvider{interval: 5 * time.Minute}}

	// 5m 同步间隔 × 3 = 15m < 24h，期望落到 24h floor
	got := svc.computeFullSyncInterval()
	if got != fullSyncIntervalFloor {
		t.Fatalf("expected floor=24h, got %v", got)
	}
}

func TestComputeFullSyncIntervalScalesWithLongInterval(t *testing.T) {
	svc := &Service{settings: fakeSettingsProvider{interval: 12 * time.Hour}}

	// 12h * 3 = 36h，应使用 scaled 值
	got := svc.computeFullSyncInterval()
	want := 36 * time.Hour
	if got != want {
		t.Fatalf("expected %v, got %v", want, got)
	}
}

func TestComputeFullSyncIntervalWithoutSettings(t *testing.T) {
	svc := &Service{}
	got := svc.computeFullSyncInterval()
	if got != fullSyncIntervalFloor {
		t.Fatalf("expected floor when settings=nil, got %v", got)
	}
}

type identityMappingRepo struct {
	mappingcontract.MappingRepository
	mapping *mappingdomain.Mapping
	updates int
}

func (r *identityMappingRepo) GetByID(uint) (*mappingdomain.Mapping, error) { return r.mapping, nil }
func (r *identityMappingRepo) ListActiveByConnection(uint) ([]mappingdomain.Mapping, error) {
	if r.mapping == nil || !r.mapping.IsActive {
		return nil, nil
	}
	return []mappingdomain.Mapping{*r.mapping}, nil
}
func (r *identityMappingRepo) Update(mapping *mappingdomain.Mapping) error {
	r.updates++
	copy := *mapping
	r.mapping = &copy
	return nil
}

type identityConnectionProvider struct{ adapter upstream.Adapter }

func (p identityConnectionProvider) GetByID(uint) (*siteconnectiondomain.Connection, error) {
	return &siteconnectiondomain.Connection{ID: 5}, nil
}
func (p identityConnectionProvider) GetAdapter(*siteconnectiondomain.Connection) (upstream.Adapter, error) {
	return p.adapter, nil
}

type identityCodeAdapter struct {
	upstream.Adapter
	product *upstream.UpstreamProduct
	err     error
	calls   int
}

func (a *identityCodeAdapter) GetProductByCode(context.Context, string) (*upstream.UpstreamProduct, error) {
	a.calls++
	return a.product, a.err
}

type numericIdentityAdapter struct {
	upstream.Adapter
	gotID uint
}

func (a *numericIdentityAdapter) GetProduct(_ context.Context, id uint) (*upstream.UpstreamProduct, error) {
	a.gotID = id
	return &upstream.UpstreamProduct{ID: id}, nil
}

type identityProductRepo struct{ reads, updates int }

func (r *identityProductRepo) GetByID(string) (*productdomain.Product, error) {
	r.reads++
	return &productdomain.Product{IsActive: true}, nil
}
func (r *identityProductRepo) Update(*productdomain.Product) error            { r.updates++; return nil }
func (*identityProductRepo) QuickUpdate(string, map[string]interface{}) error { return nil }
func (*identityProductRepo) CountBySlug(string, *string) (int64, error)       { return 0, nil }

type identitySKURepo struct{ reads, updates int }

func (r *identitySKURepo) GetByID(uint) (*productdomain.ProductSKU, error) {
	r.reads++
	return &productdomain.ProductSKU{IsActive: true}, nil
}
func (*identitySKURepo) ListByProduct(uint, bool) ([]productdomain.ProductSKU, error) {
	return nil, nil
}
func (*identitySKURepo) Create(*productdomain.ProductSKU) error   { return nil }
func (r *identitySKURepo) Update(*productdomain.ProductSKU) error { r.updates++; return nil }

func TestSyncProductIdentityMismatchPausesMappingWithoutUpdatingProductOrSKU(t *testing.T) {
	mapping := &mappingdomain.Mapping{ID: 11, ConnectionID: 5, LocalProductID: 21, UpstreamProductID: 7, UpstreamProductCode: "stored-code", IsActive: true}
	repo := &identityMappingRepo{mapping: mapping}
	adapter := &identityCodeAdapter{product: &upstream.UpstreamProduct{ID: 8, Code: "different-code", IsActive: true}}
	products, skus := &identityProductRepo{}, &identitySKURepo{}
	service := &Service{mappings: repo, connections: identityConnectionProvider{adapter: adapter}, products: products, skus: skus}

	err := service.SyncProduct(mapping.ID)
	if err == nil || !isFakaIdentityError(err) {
		t.Fatalf("SyncProduct() error = %v, want identity mismatch", err)
	}
	if repo.mapping.IsActive || repo.mapping.UpstreamStatus != mappingdomain.UpstreamStatusIdentityError || repo.updates != 1 {
		t.Fatalf("mapping active=%v upstream_status=%q updates=%d, want identity_error and one persistence update", repo.mapping.IsActive, repo.mapping.UpstreamStatus, repo.updates)
	}
	if adapter.calls != 1 {
		t.Fatalf("GetProductByCode calls = %d, want 1", adapter.calls)
	}
	if products.reads != 0 || products.updates != 0 || skus.reads != 0 || skus.updates != 0 {
		t.Fatalf("mismatch touched product/SKU repositories: products=(%d,%d) skus=(%d,%d)", products.reads, products.updates, skus.reads, skus.updates)
	}
}

func TestIdentityErrorCannotBeRestoredByOrdinarySyncOrEnable(t *testing.T) {
	mapping := &mappingdomain.Mapping{ID: 31, ConnectionID: 5, LocalProductID: 41, UpstreamProductID: 7, UpstreamProductCode: "stored-code", IsActive: false, UpstreamStatus: mappingdomain.UpstreamStatusIdentityError}
	repo := &identityMappingRepo{mapping: mapping}
	adapter := &identityCodeAdapter{product: &upstream.UpstreamProduct{ID: 7, Code: "stored-code", IsActive: true}}
	service := &Service{mappings: repo, connections: identityConnectionProvider{adapter: adapter}}
	if err := service.SetActive(mapping.ID, true); !errors.Is(err, mappingcontract.ErrIdentityVerificationRequired) {
		t.Fatalf("SetActive error=%v, want identity verification required", err)
	}
	if err := service.SyncProduct(mapping.ID); !errors.Is(err, mappingcontract.ErrIdentityVerificationRequired) {
		t.Fatalf("SyncProduct error=%v, want identity verification required", err)
	}
	if adapter.calls != 0 || repo.mapping.UpstreamStatus != mappingdomain.UpstreamStatusIdentityError || repo.mapping.IsActive {
		t.Fatalf("adapter calls=%d mapping=%#v; identity_error must remain paused without an explicit verification", adapter.calls, repo.mapping)
	}
}

func TestVerifyFakaIdentityRequiresMatchingPersistentIDAndCode(t *testing.T) {
	for _, tc := range []struct {
		name    string
		product *upstream.UpstreamProduct
		wantErr bool
	}{
		{name: "matching identity", product: &upstream.UpstreamProduct{ID: 7, Code: "stored-code", IsActive: true}},
		{name: "mismatched ID and code", product: &upstream.UpstreamProduct{ID: 8, Code: "other-code", IsActive: true}, wantErr: true},
	} {
		t.Run(tc.name, func(t *testing.T) {
			mapping := &mappingdomain.Mapping{ID: 32, ConnectionID: 5, LocalProductID: 42, UpstreamProductID: 7, UpstreamProductCode: "stored-code", IsActive: false, UpstreamStatus: mappingdomain.UpstreamStatusIdentityError}
			repo := &identityMappingRepo{mapping: mapping}
			adapter := &identityCodeAdapter{product: tc.product}
			service := &Service{mappings: repo, connections: identityConnectionProvider{adapter: adapter}}
			err := service.VerifyFakaIdentity(mapping.ID)
			if (err != nil) != tc.wantErr {
				t.Fatalf("VerifyFakaIdentity error=%v, wantErr=%v", err, tc.wantErr)
			}
			if adapter.calls != 1 {
				t.Fatalf("GetProductByCode calls=%d, want 1", adapter.calls)
			}
			if tc.wantErr {
				if repo.mapping.UpstreamStatus != mappingdomain.UpstreamStatusIdentityError || repo.mapping.IsActive {
					t.Fatalf("mismatch changed mapping: %#v", repo.mapping)
				}
			} else if repo.mapping.UpstreamStatus != mappingdomain.UpstreamStatusActive || !repo.mapping.IsActive {
				t.Fatalf("verified mapping=%#v, want active", repo.mapping)
			}
		})
	}
}

func TestSyncProductLegacyFakaMappingPausesWithoutInferringSlug(t *testing.T) {
	mapping := &mappingdomain.Mapping{ID: 12, ConnectionID: 5, LocalProductID: 22, UpstreamProductID: 7, IsActive: true}
	repo := &identityMappingRepo{mapping: mapping}
	adapter := &identityCodeAdapter{product: &upstream.UpstreamProduct{ID: 7, Code: "must-not-be-inferred", IsActive: true}}
	service := &Service{mappings: repo, connections: identityConnectionProvider{adapter: adapter}}

	err := service.SyncProduct(mapping.ID)
	if !errors.Is(err, upstream.ErrFakaProductCodeMissing) {
		t.Fatalf("SyncProduct() error = %v, want missing persistent code", err)
	}
	if adapter.calls != 0 || repo.mapping.IsActive || repo.mapping.UpstreamStatus != mappingdomain.UpstreamStatusIdentityError {
		t.Fatalf("code lookup calls=%d mapping active=%v upstream_status=%q, want no lookup and identity_error", adapter.calls, repo.mapping.IsActive, repo.mapping.UpstreamStatus)
	}
}

func TestSyncProductFailureInvalidatesOldFakaCapabilitySnapshot(t *testing.T) {
	mapping := &mappingdomain.Mapping{
		ID: 14, ConnectionID: 5, LocalProductID: 24, UpstreamProductID: 7,
		UpstreamProductCode: "stored-code", IsActive: true,
		FakaProcurementProfile: jsonmap.JSON{"assessment_complete": true, "snapshot_valid": true, "capability": map[string]any{"status": "eligible", "can_auto_procure": true}},
	}
	repo := &identityMappingRepo{mapping: mapping}
	adapter := &identityCodeAdapter{err: errors.New("mock detail unavailable")}
	service := &Service{mappings: repo, connections: identityConnectionProvider{adapter: adapter}}
	if err := service.SyncProduct(mapping.ID); err == nil {
		t.Fatal("SyncProduct should report mock detail failure")
	}
	if repo.mapping.FakaProcurementProfile["snapshot_valid"] != false {
		t.Fatalf("old snapshot was not invalidated: %#v", repo.mapping.FakaProcurementProfile)
	}
	if repo.mapping.FakaProcurementProfile["invalidated_reason"] == nil || adapter.calls != 1 {
		t.Fatalf("invalidation reason/call count missing: snapshot=%#v calls=%d", repo.mapping.FakaProcurementProfile, adapter.calls)
	}
}

func TestSyncProductDeletedFakaCodePausesWithoutProductOrSKUChanges(t *testing.T) {
	mapping := &mappingdomain.Mapping{ID: 13, ConnectionID: 5, LocalProductID: 23, UpstreamProductID: 7, UpstreamProductCode: "removed-code", IsActive: true}
	repo := &identityMappingRepo{mapping: mapping}
	adapter := &identityCodeAdapter{err: upstream.ErrUpstreamProductDeleted}
	products, skus := &identityProductRepo{}, &identitySKURepo{}
	service := &Service{mappings: repo, connections: identityConnectionProvider{adapter: adapter}, products: products, skus: skus}

	err := service.SyncProduct(mapping.ID)
	if !errors.Is(err, upstream.ErrUpstreamProductDeleted) {
		t.Fatalf("SyncProduct() error = %v, want upstream product deleted", err)
	}
	if repo.mapping.IsActive || repo.mapping.UpstreamStatus != mappingdomain.UpstreamStatusIdentityError || repo.updates != 1 {
		t.Fatalf("mapping active=%v upstream_status=%q updates=%d, want identity_error", repo.mapping.IsActive, repo.mapping.UpstreamStatus, repo.updates)
	}
	if products.reads != 0 || products.updates != 0 || skus.reads != 0 || skus.updates != 0 {
		t.Fatalf("deleted product touched product/SKU repositories: products=(%d,%d) skus=(%d,%d)", products.reads, products.updates, skus.reads, skus.updates)
	}
}

func TestFetchMappedUpstreamProductKeepsDujiaoNumericLookup(t *testing.T) {
	adapter := &numericIdentityAdapter{}
	mapping := &mappingdomain.Mapping{UpstreamProductID: 41, UpstreamProductCode: "ignored-for-dujiao"}
	product, err := fetchMappedUpstreamProduct(context.Background(), adapter, mapping)
	if err != nil {
		t.Fatalf("fetchMappedUpstreamProduct() error = %v", err)
	}
	if adapter.gotID != 41 || product.ID != 41 {
		t.Fatalf("numeric lookup id=%d product id=%d, want 41", adapter.gotID, product.ID)
	}
}

func TestPauseFakaMappingsForIdentityConflictOnlyPausesMatchingIdentity(t *testing.T) {
	unrelated := mappingdomain.Mapping{ID: 3, UpstreamProductID: 9, UpstreamProductCode: "safe-code", IsActive: true}
	items := []mappingdomain.Mapping{
		{ID: 1, UpstreamProductID: 7, UpstreamProductCode: "old-code", IsActive: true},
		{ID: 2, UpstreamProductID: 8, UpstreamProductCode: "reused-code", IsActive: true},
		unrelated,
	}
	repo := &identityMappingRepo{}
	service := &Service{mappings: repo}
	identityErr := &upstream.FakaIdentityError{Message: "duplicate identity", ProductIDs: []uint{7}, ProductCodes: []string{"reused-code"}}

	if err := service.pauseFakaMappingsForIdentityConflict(items, identityErr); err != nil {
		t.Fatalf("pauseFakaMappingsForIdentityConflict() error = %v", err)
	}
	if items[0].IsActive || items[1].IsActive || !items[2].IsActive {
		t.Fatalf("mapping active states = %v, %v, %v; want only conflicts paused", items[0].IsActive, items[1].IsActive, items[2].IsActive)
	}
	if repo.updates != 2 {
		t.Fatalf("mapping updates = %d, want only the two conflicting mappings", repo.updates)
	}
}

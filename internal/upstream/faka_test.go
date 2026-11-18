package upstream

import (
	"context"
	"encoding/json"
	"errors"
	"io"
	"net/http"
	"net/http/httptest"
	"net/url"
	"strings"
	"testing"
	"time"

	"github.com/dujiao-next/internal/constants"
	siteconnectiondomain "github.com/dujiao-next/internal/modules/siteconnection/domain"
)

func TestFakaSignMatchesPHPEncodingAndMD5(t *testing.T) {
	params := map[string]any{
		"sku":    map[string]any{"规格": "红色&蓝色"},
		"name":   "蜂蜜 & tea",
		"empty":  "",
		"app_id": "9",
		"sign":   "ignored",
	}

	if got, want := FakaSign("secret", params), "8d0847c224e5e9164bd129b4982557ac"; got != want {
		t.Fatalf("FakaSign() = %q, want %q", got, want)
	}
}

func TestFakaSignEncodesNumericArrayIndexes(t *testing.T) {
	params := map[string]any{"app_id": "1", "sku": []string{"x", "y"}}
	if got, want := FakaSign("k", params), "abade0c9046a29e40544e908e24da286"; got != want {
		t.Fatalf("FakaSign() = %q, want %q", got, want)
	}
}

func TestFakaRequestPostsFormWithoutAppKey(t *testing.T) {
	var gotForm url.Values
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		if r.Method != http.MethodPost {
			t.Errorf("method = %s, want POST", r.Method)
		}
		if got, want := r.Header.Get("Content-Type"), "application/x-www-form-urlencoded"; !strings.HasPrefix(got, want) {
			t.Errorf("Content-Type = %q, want prefix %q", got, want)
		}
		body, err := io.ReadAll(r.Body)
		if err != nil {
			t.Errorf("read body: %v", err)
		}
		gotForm, err = url.ParseQuery(string(body))
		if err != nil {
			t.Errorf("parse form: %v", err)
		}
		w.Header().Set("Content-Type", "application/json")
		_, _ = io.WriteString(w, `{"code":200,"data":{"ok":true}}`)
	}))
	defer server.Close()

	adapter := NewFakaAdapter(&siteconnectiondomain.Connection{BaseURL: server.URL, ApiKey: "app-id", ApiSecret: "app-key"})
	var data struct {
		OK bool `json:"ok"`
	}
	if err := adapter.doRequest(context.Background(), "/test", map[string]any{
		"name":    "小蜜蜂",
		"sku":     map[string]any{"颜色": "蓝色"},
		"app_key": "must-not-leak",
	}, &data); err != nil {
		t.Fatalf("doRequest() error = %v", err)
	}
	if !data.OK {
		t.Fatal("response data was not decoded")
	}
	if got := gotForm.Get("app_id"); got != "app-id" {
		t.Errorf("app_id = %q", got)
	}
	if got := gotForm.Get("name"); got != "小蜜蜂" {
		t.Errorf("name = %q", got)
	}
	if got := gotForm.Get("sku[颜色]"); got != "蓝色" {
		t.Errorf("sku[颜色] = %q", got)
	}
	if got := gotForm.Get("sign"); got != FakaSign("app-key", map[string]any{"app_id": "app-id", "name": "小蜜蜂", "sku": map[string]any{"颜色": "蓝色"}}) {
		t.Errorf("sign = %q", got)
	}
	if got := gotForm.Get("app_key"); got != "" {
		t.Errorf("app_key must not be sent; got %q", got)
	}
}

func TestFakaRequestDoesNotFollowRedirect(t *testing.T) {
	var secondCalled bool
	second := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, _ *http.Request) {
		secondCalled = true
		_, _ = io.WriteString(w, `{"code":200,"data":{}}`)
	}))
	defer second.Close()
	first := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		http.Redirect(w, r, second.URL, http.StatusFound)
	}))
	defer first.Close()

	adapter := NewFakaAdapter(&siteconnectiondomain.Connection{BaseURL: first.URL, ApiKey: "id", ApiSecret: "key"})
	err := adapter.doRequest(context.Background(), "/test", nil, nil)
	if err == nil || !strings.Contains(err.Error(), "HTTP 302") {
		t.Fatalf("doRequest() error = %v, want HTTP 302 error", err)
	}
	if secondCalled {
		t.Fatal("request followed redirect to another host")
	}
}

func TestFakaRequestReturnsHTTPAndJSONErrors(t *testing.T) {
	cases := []struct {
		name       string
		status     int
		body       string
		wantErr    string
		wantCode   string
		wantStatus int
	}{
		{name: "http", status: http.StatusBadGateway, body: `{"code":0,"msg":"gateway"}`, wantErr: "HTTP 502", wantCode: "0", wantStatus: http.StatusBadGateway},
		{name: "invalid json", status: http.StatusOK, body: `not-json`, wantErr: "decode", wantStatus: http.StatusOK},
		{name: "api error", status: http.StatusOK, body: `{"code":0,"msg":"余额不足"}`, wantErr: "余额不足", wantCode: "0", wantStatus: http.StatusOK},
	}

	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, _ *http.Request) {
				w.WriteHeader(tc.status)
				_, _ = io.WriteString(w, tc.body)
			}))
			defer server.Close()

			adapter := NewFakaAdapter(&siteconnectiondomain.Connection{BaseURL: server.URL, ApiKey: "id", ApiSecret: "key"})
			err := adapter.doRequest(context.Background(), "/test", nil, nil)
			if err == nil || !strings.Contains(err.Error(), tc.wantErr) {
				t.Fatalf("doRequest() error = %v, want containing %q", err, tc.wantErr)
			}
			var apiErr *FakaAPIError
			if errors.As(err, &apiErr) {
				if tc.wantCode != "" && apiErr.Code != tc.wantCode {
					t.Errorf("error code = %q, want %q", apiErr.Code, tc.wantCode)
				}
				if apiErr.HTTPStatus != tc.wantStatus {
					t.Errorf("HTTP status = %d, want %d", apiErr.HTTPStatus, tc.wantStatus)
				}
			} else if tc.wantCode != "" {
				t.Fatalf("error type = %T, want *FakaAPIError", err)
			}
		})
	}
}

func TestFakaFactoryRegistration(t *testing.T) {
	adapter, err := NewAdapter(&siteconnectiondomain.Connection{
		Protocol:  constants.ConnectionProtocolFaka,
		BaseURL:   "https://example.invalid",
		ApiKey:    "id",
		ApiSecret: "key",
	}, t.TempDir())
	if err != nil {
		t.Fatalf("NewAdapter() error = %v", err)
	}
	if _, ok := adapter.(*FakaAdapter); !ok {
		t.Fatalf("adapter type = %T, want *FakaAdapter", adapter)
	}
}

func TestFakaItemPreservesWidgetArray(t *testing.T) {
	var item fakaItem
	if err := json.Unmarshal([]byte(`{"id":7,"code":"gift-code","widget":[{"name":"email","type":"input"}]}`), &item); err != nil {
		t.Fatalf("unmarshal item: %v", err)
	}
	product := item.toProduct(2)
	fields, ok := product.ManualFormSchema["fields"].([]any)
	if !ok || len(fields) != 1 {
		t.Fatalf("widget fields = %#v, want one preserved field", product.ManualFormSchema["fields"])
	}
}

func TestFakaPurchaseCapabilityClassifiesOnlyExplicitSimpleProductsAsEligible(t *testing.T) {
	cases := []struct {
		name     string
		json     string
		status   FakaProcurementCapabilityStatus
		eligible bool
		check    func(*testing.T, FakaProcurementCapability)
	}{
		{name: "simple", json: `{"id":7,"code":"simple","config":{},"draft_status":0,"password_status":0,"widget":null,"minimum":2,"maximum":10,"delivery_way":1}`, status: FakaCapabilityEligible, eligible: true, check: func(t *testing.T, c FakaProcurementCapability) {
			if c.MinimumQuantity != 2 || c.MaximumQuantity != 10 || c.DeliveryWay != 1 {
				t.Fatalf("quantity/delivery constraints = (%d,%d,%d)", c.MinimumQuantity, c.MaximumQuantity, c.DeliveryWay)
			}
		}},
		{name: "missing config is unknown", json: `{"id":7,"code":"missing-config"}`, status: FakaCapabilityUnknown},
		{name: "missing draft status is unknown", json: `{"id":7,"code":"missing-draft","config":{},"password_status":0,"widget":null}`, status: FakaCapabilityUnknown},
		{name: "missing password status is unknown", json: `{"id":7,"code":"missing-password","config":{},"draft_status":0,"widget":null}`, status: FakaCapabilityUnknown},
		{name: "missing widget metadata is unknown", json: `{"id":7,"code":"missing-widget","config":{},"draft_status":0,"password_status":0}`, status: FakaCapabilityUnknown},
		{name: "sku config unsupported", json: `{"id":7,"code":"sku","config":{"sku":{"颜色":["红","蓝"]}},"draft_status":0,"password_status":0,"widget":null}`, status: FakaCapabilityUnsupported, check: func(t *testing.T, c FakaProcurementCapability) {
			if !c.RequiresSKU {
				t.Fatal("SKU requirement not detected")
			}
		}},
		{name: "race config unsupported", json: `{"id":7,"code":"race","config":{"category":{"月卡":{}}},"draft_status":0,"password_status":0,"widget":null}`, status: FakaCapabilityUnsupported, check: func(t *testing.T, c FakaProcurementCapability) {
			if !c.RequiresRace {
				t.Fatal("race requirement not detected")
			}
		}},
		{name: "draft card unsupported", json: `{"id":7,"code":"draft","config":{},"draft_status":1,"password_status":0,"widget":null}`, status: FakaCapabilityUnsupported, check: func(t *testing.T, c FakaProcurementCapability) {
			if !c.RequiresCardID {
				t.Fatal("card_id requirement not detected")
			}
		}},
		{name: "password unsupported", json: `{"id":7,"code":"password","config":{},"draft_status":0,"password_status":1,"widget":null}`, status: FakaCapabilityUnsupported, check: func(t *testing.T, c FakaProcurementCapability) {
			if !c.RequiresPassword {
				t.Fatal("password requirement not detected")
			}
		}},
		{name: "coupon config unsupported", json: `{"id":7,"code":"coupon","config":{"coupon":{"required":true}},"draft_status":0,"password_status":0,"widget":null}`, status: FakaCapabilityUnsupported, check: func(t *testing.T, c FakaProcurementCapability) {
			if !c.RequiresCoupon {
				t.Fatal("coupon requirement not detected")
			}
		}},
		{name: "widget unsupported", json: `{"id":7,"code":"widget","config":{},"draft_status":0,"password_status":0,"widget":[{"name":"email","required":true}]}`, status: FakaCapabilityUnsupported, check: func(t *testing.T, c FakaProcurementCapability) {
			if !c.RequiresWidget || len(c.WidgetFields) != 1 || !c.WidgetFields[0].Required {
				t.Fatalf("widget not recognized: %#v", c)
			}
		}},
		{name: "malformed widget is unknown", json: `{"id":7,"code":"bad-widget","config":{},"draft_status":0,"password_status":0,"widget":{"name":"email"}}`, status: FakaCapabilityUnknown},
		{name: "unknown config key is unsafe", json: `{"id":7,"code":"unknown","config":{"custom_mode":true},"draft_status":0,"password_status":0,"widget":null}`, status: FakaCapabilityUnknown},
		{name: "empty unknown config key is still unsafe", json: `{"id":7,"code":"unknown-empty","config":{"custom_mode":{}},"draft_status":0,"password_status":0,"widget":null}`, status: FakaCapabilityUnknown},
		{name: "malformed config is unsafe", json: `{"id":7,"code":"broken","config":"not-json","draft_status":0,"password_status":0,"widget":null}`, status: FakaCapabilityUnknown},
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			var item fakaItem
			input := strings.Replace(tc.json, "{", `{"minimum":0,"maximum":0,"delivery_way":1,`, 1)
			if err := json.Unmarshal([]byte(input), &item); err != nil {
				t.Fatal(err)
			}
			product := item.toProduct(1)
			if product.FakaProcurement == nil {
				t.Fatal("Faka procurement profile is nil")
			}
			capability := product.FakaProcurement.Capability
			if capability.Status != tc.status || capability.CanAutoProcure != tc.eligible {
				t.Fatalf("capability = %#v, want status=%q eligible=%v", capability, tc.status, tc.eligible)
			}
			if tc.check != nil {
				tc.check(t, capability)
			}
			if string(product.FakaProcurement.RawConfig) != string(item.Config) {
				t.Fatalf("raw config changed: got %s want %s", product.FakaProcurement.RawConfig, item.Config)
			}
			if string(product.FakaProcurement.RawWidget) != string(item.Widget) {
				t.Fatalf("raw widget changed: got %s want %s", product.FakaProcurement.RawWidget, item.Widget)
			}
		})
	}
}

func TestFakaCapabilityQuantityGate(t *testing.T) {
	var item fakaItem
	if err := json.Unmarshal([]byte(`{"id":7,"code":"simple","config":{},"draft_status":0,"password_status":0,"widget":null,"minimum":2,"maximum":4,"delivery_way":1}`), &item); err != nil {
		t.Fatal(err)
	}
	profile := item.toProduct(1).FakaProcurement
	for _, tc := range []struct {
		quantity int
		want     bool
	}{{1, false}, {2, true}, {4, true}, {5, false}, {0, false}} {
		if got := profile.AllowsAutomaticProcurement(tc.quantity); got != tc.want {
			t.Errorf("AllowsAutomaticProcurement(%d) = %v, want %v", tc.quantity, got, tc.want)
		}
	}
}

func TestFakaProfileCompletenessDistinguishesListSummaryFromDetail(t *testing.T) {
	var summary, detail fakaItem
	if err := json.Unmarshal([]byte(`{"id":7,"code":"summary","minimum":0,"maximum":0,"delivery_way":1}`), &summary); err != nil {
		t.Fatal(err)
	}
	if err := json.Unmarshal([]byte(`{"id":7,"code":"detail","config":{},"draft_status":0,"password_status":0,"widget":null,"minimum":0,"maximum":0,"delivery_way":1}`), &detail); err != nil {
		t.Fatal(err)
	}
	if profile := assessFakaProcurementCapability(summary); profile.AssessmentComplete || profile.Capability.CanAutoProcure {
		t.Fatalf("incomplete list summary was treated as complete/eligible: %#v", profile)
	}
	if profile := assessFakaProcurementCapability(detail); !profile.AssessmentComplete || !profile.Capability.CanAutoProcure {
		t.Fatalf("complete simple detail was not eligible: %#v", profile)
	}
}

func TestFakaCapabilityRejectsInvalidQuantityMetadataAndConfig(t *testing.T) {
	base := `"id":7,"code":"simple","config":{},"draft_status":0,"password_status":0,"widget":null,"minimum":0,"maximum":0,"delivery_way":1`
	for _, tc := range []struct {
		name, json string
	}{
		{"minimum missing", `{"id":7,"code":"x","config":{},"draft_status":0,"password_status":0,"widget":null,"maximum":0,"delivery_way":1}`},
		{"minimum null", `{"id":7,"code":"x","config":{},"draft_status":0,"password_status":0,"widget":null,"minimum":null,"maximum":0,"delivery_way":1}`},
		{"maximum missing", `{"id":7,"code":"x","config":{},"draft_status":0,"password_status":0,"widget":null,"minimum":0,"delivery_way":1}`},
		{"maximum null", `{"id":7,"code":"x","config":{},"draft_status":0,"password_status":0,"widget":null,"minimum":0,"maximum":null,"delivery_way":1}`},
		{"minimum negative", `{"id":7,"code":"x","config":{},"draft_status":0,"password_status":0,"widget":null,"minimum":-1,"maximum":0,"delivery_way":1}`},
		{"maximum negative", `{"id":7,"code":"x","config":{},"draft_status":0,"password_status":0,"widget":null,"minimum":0,"maximum":-1,"delivery_way":1}`},
		{"conflicting bounds", `{"id":7,"code":"x","config":{},"draft_status":0,"password_status":0,"widget":null,"minimum":5,"maximum":4,"delivery_way":1}`},
		{"null config", `{"id":7,"code":"x","config":null,"draft_status":0,"password_status":0,"widget":null,"minimum":0,"maximum":0,"delivery_way":1}`},
		{"sku empty object", `{"id":7,"code":"x","config":{"sku":{}},"draft_status":0,"password_status":0,"widget":null,"minimum":0,"maximum":0,"delivery_way":1}`},
		{"category config", `{"id":7,"code":"x","config":{"category":{}},"draft_status":0,"password_status":0,"widget":null,"minimum":0,"maximum":0,"delivery_way":1}`},
		{"unknown empty config", `{"id":7,"code":"x","config":{"custom":{}},"draft_status":0,"password_status":0,"widget":null,"minimum":0,"maximum":0,"delivery_way":1}`},
	} {
		t.Run(tc.name, func(t *testing.T) {
			var item fakaItem
			if err := json.Unmarshal([]byte(tc.json), &item); err != nil {
				t.Fatal(err)
			}
			profile := assessFakaProcurementCapability(item)
			if profile.AllowsAutomaticProcurement(1) || profile.Capability.Status == FakaCapabilityEligible {
				t.Fatalf("unsafe product was eligible: %#v", profile)
			}
		})
	}
	var simple fakaItem
	if err := json.Unmarshal([]byte("{"+base+"}"), &simple); err != nil {
		t.Fatal(err)
	}
	profile := assessFakaProcurementCapability(simple)
	if !profile.AllowsAutomaticProcurement(1) {
		t.Fatalf("explicit empty config and zero bounds must be supported: %#v", profile)
	}
	if string(profile.RawDraftStatus) != "0" || string(profile.RawPasswordStatus) != "0" {
		t.Fatalf("raw statuses not retained: draft=%s password=%s", profile.RawDraftStatus, profile.RawPasswordStatus)
	}
}

func TestFakaCapabilityGateRequiresFreshCompleteEligibleSnapshot(t *testing.T) {
	profile := FakaProductProcurementProfile{AssessmentComplete: true, SnapshotValid: true, Capability: FakaProcurementCapability{Status: FakaCapabilityEligible, CanAutoProcure: true}}
	if !profile.AllowsAutomaticProcurement(1) {
		t.Fatal("valid complete eligible profile should pass")
	}
	profile.SnapshotValid = false
	if profile.AllowsAutomaticProcurement(1) {
		t.Fatal("invalidated snapshot passed")
	}
	profile.SnapshotValid, profile.AssessmentComplete = true, false
	if profile.AllowsAutomaticProcurement(1) {
		t.Fatal("incomplete assessment passed")
	}
	profile.AssessmentComplete, profile.Capability.Status = true, FakaCapabilityUnknown
	if profile.AllowsAutomaticProcurement(1) {
		t.Fatal("unknown capability passed")
	}
}

func TestFakaDetailConfigChangeReplacesEligibleSnapshotWithUnsupported(t *testing.T) {
	var simple, changed fakaItem
	for target, body := range map[*fakaItem]string{
		&simple:  `{"id":7,"code":"x","config":{},"draft_status":0,"password_status":0,"widget":null,"minimum":0,"maximum":0,"delivery_way":1}`,
		&changed: `{"id":7,"code":"x","config":{"sku":{}},"draft_status":0,"password_status":0,"widget":null,"minimum":0,"maximum":0,"delivery_way":1}`,
	} {
		if err := json.Unmarshal([]byte(body), target); err != nil {
			t.Fatal(err)
		}
	}
	oldSnapshot := assessFakaProcurementCapability(simple).MappingSnapshot()
	newProfile := assessFakaProcurementCapability(changed)
	newSnapshot := newProfile.MappingSnapshot()
	if oldSnapshot["snapshot_valid"] != true || oldSnapshot["capability"].(FakaProcurementCapability).Status != FakaCapabilityEligible {
		t.Fatalf("old simple snapshot not eligible: %#v", oldSnapshot)
	}
	if newSnapshot["snapshot_valid"] != true || newProfile.AllowsAutomaticProcurement(1) || newProfile.Capability.Status != FakaCapabilityUnsupported {
		t.Fatalf("changed SKU config was not replaced with unsupported snapshot: %#v", newSnapshot)
	}
}

func TestFakaGetProductByCodeUsesPersistentCodeAcrossListReordering(t *testing.T) {
	var detailCode string
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		_ = r.ParseForm()
		w.Header().Set("Content-Type", "application/json")
		switch r.URL.Path {
		case "/shared/commodity/items":
			_, _ = io.WriteString(w, `{"code":200,"data":[{"id":3,"children":[{"id":8,"code":"other-code","name":"Other"}]},{"id":2,"children":[{"id":7,"code":"stored-code","name":"Stored"}]}]}`)
		case "/shared/commodity/item":
			detailCode = r.Form.Get("code")
			_, _ = io.WriteString(w, `{"code":200,"data":{"id":7,"code":"stored-code","name":"Stored","price":10}}`)
		default:
			t.Errorf("unexpected path %s", r.URL.Path)
			w.WriteHeader(http.StatusNotFound)
		}
	}))
	defer server.Close()

	adapter := NewFakaAdapter(&siteconnectiondomain.Connection{BaseURL: server.URL, ApiKey: "id", ApiSecret: "key"})
	product, err := adapter.GetProductByCode(context.Background(), "stored-code")
	if err != nil {
		t.Fatalf("GetProductByCode() error = %v", err)
	}
	if product.ID != 7 || product.Code != "stored-code" || detailCode != "stored-code" {
		t.Fatalf("product = (%d, %q), requested code = %q", product.ID, product.Code, detailCode)
	}
}

func TestFakaLiveCapabilityVerifierChecksDetailIdentityAndEligibility(t *testing.T) {
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		w.Header().Set("Content-Type", "application/json")
		switch r.URL.Path {
		case "/shared/commodity/items":
			_, _ = io.WriteString(w, `{"code":200,"data":[{"id":3,"children":[{"id":7,"code":"stored-code","name":"Simple"}]}]}`)
		case "/shared/commodity/item":
			_, _ = io.WriteString(w, `{"code":200,"data":{"id":7,"code":"stored-code","name":"Simple","config":{},"draft_status":0,"password_status":0,"widget":null,"minimum":1,"maximum":4,"delivery_way":1}}`)
		default:
			t.Errorf("unexpected endpoint %s", r.URL.Path)
			w.WriteHeader(http.StatusNotFound)
		}
	}))
	defer server.Close()
	adapter := NewFakaAdapter(&siteconnectiondomain.Connection{BaseURL: server.URL, ApiKey: "id", ApiSecret: "key"})
	profile, err := adapter.VerifyFakaProcurementCapability(context.Background(), "stored-code", 7, 2)
	if err != nil || profile == nil || !profile.AllowsAutomaticProcurement(2) {
		t.Fatalf("live verifier profile=%#v error=%v", profile, err)
	}
	if _, err := adapter.VerifyFakaProcurementCapability(context.Background(), "stored-code", 8, 2); err == nil {
		t.Fatal("live verifier accepted mismatched numeric ID")
	}
}

func TestFakaListProductsRejectsDuplicateIDAndCode(t *testing.T) {
	cases := []struct {
		name string
		data string
		want string
	}{
		{name: "same ID different code", data: `[{"id":1,"children":[{"id":7,"code":"code-a"}]},{"id":2,"children":[{"id":7,"code":"code-b"}]}]`, want: "duplicate IDs [7]"},
		{name: "same code different ID", data: `[{"id":1,"children":[{"id":7,"code":"same-code"},{"id":8,"code":"same-code"}]}]`, want: "duplicate codes [\"same-code\"]"},
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, _ *http.Request) {
				w.Header().Set("Content-Type", "application/json")
				_, _ = io.WriteString(w, `{"code":200,"data":`+tc.data+`}`)
			}))
			defer server.Close()
			adapter := NewFakaAdapter(&siteconnectiondomain.Connection{BaseURL: server.URL, ApiKey: "id", ApiSecret: "key"})
			_, err := adapter.ListProducts(context.Background(), ListProductsOpts{Page: 1, PageSize: 100})
			if err == nil || !strings.Contains(err.Error(), tc.want) {
				t.Fatalf("ListProducts() error = %v, want containing %q", err, tc.want)
			}
		})
	}
}

func TestFakaTradeRequestAndResult(t *testing.T) {
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		if r.URL.Path != "/shared/commodity/trade" || r.Method != http.MethodPost {
			t.Errorf("unexpected request %s %s", r.Method, r.URL.Path)
		}
		if got := r.Header.Get("Content-Type"); !strings.HasPrefix(got, "application/x-www-form-urlencoded") {
			t.Errorf("unexpected Content-Type %q", got)
		}
		if err := r.ParseForm(); err != nil {
			t.Error(err)
		}
		if got := r.Form.Get("app_key"); got != "" {
			t.Errorf("app_key leaked: %q", got)
		}
		if r.Form.Get("shared_code") != "persisted-code" || r.Form.Get("num") != "2" || r.Form.Get("request_no") != "persistent-trace" || r.Form.Get("app_id") != "app-id" {
			t.Errorf("unexpected form: %v", r.Form)
		}
		params := map[string]any{"app_id": "app-id", "shared_code": "persisted-code", "num": 2, "request_no": "persistent-trace"}
		if r.Form.Get("sign") != FakaSign("app-key", params) {
			t.Errorf("invalid trade signature")
		}
		_, _ = io.WriteString(w, `{"code":200,"data":{"tradeNo":"TR-abc-001","amount":"12.50","secret":"card data","stock":"4"}}`)
	}))
	defer server.Close()
	adapter := NewFakaAdapter(&siteconnectiondomain.Connection{BaseURL: server.URL, ApiKey: "app-id", ApiSecret: "app-key"})
	result, err := adapter.CreateOrder(context.Background(), CreateUpstreamOrderReq{SharedCode: "persisted-code", Quantity: 2, TraceID: "persistent-trace"})
	if err != nil || !result.OK || result.OrderNo != "TR-abc-001" || result.Amount != "12.50" {
		t.Fatalf("CreateOrder result=%+v error=%v", result, err)
	}
}

func TestFakaTradeKeepsExactJSONAmountDigits(t *testing.T) {
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, _ *http.Request) {
		_, _ = io.WriteString(w, `{"code":200,"data":{"tradeNo":"TR-exact","amount":123456789.01,"secret":null,"stock":1}}`)
	}))
	defer server.Close()
	adapter := NewFakaAdapter(&siteconnectiondomain.Connection{BaseURL: server.URL, ApiKey: "id", ApiSecret: "key"})
	result, err := adapter.CreateOrder(context.Background(), CreateUpstreamOrderReq{SharedCode: "code", Quantity: 1, TraceID: "trace"})
	if err != nil || result == nil || result.Amount != "123456789.01" {
		t.Fatalf("result=%+v error=%v; amount digits changed", result, err)
	}
}

func TestFakaTradeOutcomeClassification(t *testing.T) {
	cases := []struct {
		name, body string
		status     int
	}{
		{"business failure is uncertain", `{"code":0,"msg":"insufficient balance"}`, 200},
		{"server error is uncertain", `{"code":0,"msg":"error"}`, 500},
		{"invalid json is uncertain", `{broken`, 200},
		{"missing trade number is uncertain", `{"code":200,"data":{"amount":"12.50"}}`, 200},
		{"invalid amount is uncertain", `{"code":200,"data":{"tradeNo":"TR-1","amount":"oops"}}`, 200},
		{"invalid secret type is uncertain", `{"code":200,"data":{"tradeNo":"TR-1","amount":"12.50","secret":{"unexpected":1}}}`, 200},
		{"invalid stock type is uncertain", `{"code":200,"data":{"tradeNo":"TR-1","amount":"12.50","stock":[]}}`, 200},
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, _ *http.Request) {
				w.WriteHeader(tc.status)
				_, _ = io.WriteString(w, tc.body)
			}))
			defer server.Close()
			adapter := NewFakaAdapter(&siteconnectiondomain.Connection{BaseURL: server.URL, ApiKey: "id", ApiSecret: "key"})
			result, err := adapter.CreateOrder(context.Background(), CreateUpstreamOrderReq{SharedCode: "code", Quantity: 1, TraceID: "trace"})
			if err == nil || errors.Is(err, ErrFakaTradeNotSent) {
				t.Fatalf("result=%+v error=%v; want uncertain outcome", result, err)
			}
		})
	}
}

func TestFakaTradeDialFailureAndSentTimeout(t *testing.T) {
	server := httptest.NewServer(http.HandlerFunc(func(http.ResponseWriter, *http.Request) {}))
	closedURL := server.URL
	server.Close()
	adapter := NewFakaAdapter(&siteconnectiondomain.Connection{BaseURL: closedURL, ApiKey: "id", ApiSecret: "key"})
	_, err := adapter.CreateOrder(context.Background(), CreateUpstreamOrderReq{SharedCode: "code", Quantity: 1, TraceID: "trace"})
	if !errors.Is(err, ErrFakaTradeNotSent) {
		t.Fatalf("dial failure error=%v; want proven not sent", err)
	}

	started := make(chan struct{})
	server = httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, _ *http.Request) {
		close(started)
		time.Sleep(100 * time.Millisecond)
		_, _ = io.WriteString(w, `{"code":200,"data":{"tradeNo":"TR-1","amount":"1.00"}}`)
	}))
	defer server.Close()
	adapter = NewFakaAdapter(&siteconnectiondomain.Connection{BaseURL: server.URL, ApiKey: "id", ApiSecret: "key"})
	ctx, cancel := context.WithTimeout(context.Background(), 30*time.Millisecond)
	defer cancel()
	_, err = adapter.CreateOrder(ctx, CreateUpstreamOrderReq{SharedCode: "code", Quantity: 1, TraceID: "trace"})
	if err == nil || errors.Is(err, ErrFakaTradeNotSent) {
		t.Fatalf("sent timeout error=%v; want uncertain outcome", err)
	}
	<-started
}

func TestFakaNonTradeMethodsAreExplicitlyDisabled(t *testing.T) {
	adapter := NewFakaAdapter(&siteconnectiondomain.Connection{BaseURL: "https://example.invalid", ApiKey: "id", ApiSecret: "key"})
	if _, err := adapter.CreateOrder(context.Background(), CreateUpstreamOrderReq{}); !errors.Is(err, ErrFakaProductCodeMissing) || !errors.Is(err, ErrFakaTradeNotSent) {
		t.Fatalf("CreateOrder() without shared_code error = %v, want ErrFakaProductCodeMissing", err)
	}
	if err := adapter.CancelOrder(context.Background(), 1); !errors.Is(err, ErrFakaProcurementNotImplemented) {
		t.Fatalf("CancelOrder() error = %v, want ErrFakaProcurementNotImplemented", err)
	}
}

func TestFakaQueryUsesStringTradeNoAndParsesPaymentOnly(t *testing.T) {
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		if r.URL.Path != "/shared/commodity/query" || r.Method != http.MethodPost {
			t.Errorf("unexpected request %s %s", r.Method, r.URL.Path)
		}
		if err := r.ParseForm(); err != nil {
			t.Fatal(err)
		}
		if r.Form.Get("tradeNo") != "TR-abc-001" || r.Form.Get("trade_no") != "" || r.Form.Get("app_key") != "" {
			t.Errorf("unsafe query form: %v", r.Form)
		}
		if r.Form.Get("sign") != FakaSign("key", map[string]any{"app_id": "id", "tradeNo": "TR-abc-001"}) {
			t.Error("invalid query signature")
		}
		_, _ = io.WriteString(w, `{"code":200,"data":{"status":1,"secret":"卡密内容","widget":{"email":"test@example.com"}}}`)
	}))
	defer server.Close()
	adapter := NewFakaAdapter(&siteconnectiondomain.Connection{BaseURL: server.URL, ApiKey: "id", ApiSecret: "key"})
	result, err := adapter.QueryOrderByTradeNo(context.Background(), "TR-abc-001")
	if err != nil || result == nil || result.PaymentStatus != 1 || result.Secret != "卡密内容" || string(result.Widget) != `{"email":"test@example.com"}` {
		t.Fatalf("query result=%+v error=%v", result, err)
	}
}

func TestFakaQueryRejectsUnknownShapeAndStatus(t *testing.T) {
	for _, body := range []string{
		`{"code":200,"data":{"secret":"x"}}`,
		`{"code":200,"data":{"status":null,"secret":"x"}}`,
		`{"code":200,"data":{"status":"1","secret":"x"}}`,
		`{"code":200,"data":{"status":2,"secret":"x"}}`,
		`{"code":200,"data":{"status":1,"secret":{"unexpected":"value"}}}`,
		`{"code":200,"data":{"status":1,"secret":"x","widget":["unexpected"]}}`,
		`{broken`,
	} {
		t.Run(body, func(t *testing.T) {
			server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, _ *http.Request) {
				_, _ = io.WriteString(w, body)
			}))
			defer server.Close()
			adapter := NewFakaAdapter(&siteconnectiondomain.Connection{BaseURL: server.URL, ApiKey: "id", ApiSecret: "key"})
			if _, err := adapter.QueryOrderByTradeNo(context.Background(), "TR-1"); err == nil {
				t.Fatalf("query unexpectedly accepted %s", body)
			}
		})
	}
}

func TestFakaQueryTimeoutDoesNotReturnDelivery(t *testing.T) {
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, _ *http.Request) {
		time.Sleep(80 * time.Millisecond)
		_, _ = io.WriteString(w, `{"code":200,"data":{"status":1,"secret":"CARD-001"}}`)
	}))
	defer server.Close()
	adapter := NewFakaAdapter(&siteconnectiondomain.Connection{BaseURL: server.URL, ApiKey: "id", ApiSecret: "key"})
	ctx, cancel := context.WithTimeout(context.Background(), 20*time.Millisecond)
	defer cancel()
	result, err := adapter.QueryOrderByTradeNo(ctx, "TR-A")
	if err == nil || result != nil {
		t.Fatalf("query result=%+v error=%v; want timeout without delivery", result, err)
	}
}

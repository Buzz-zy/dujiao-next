package upstream

import (
	"bytes"
	"context"
	"crypto/md5"
	"encoding/hex"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"net"
	"net/http"
	"net/url"
	"reflect"
	"sort"
	"strconv"
	"strings"
	"time"

	siteconnectiondomain "github.com/dujiao-next/internal/modules/siteconnection/domain"
	"github.com/dujiao-next/internal/shared/jsonmap"
	"github.com/shopspring/decimal"
)

const (
	fakaRequestTimeout = 20 * time.Second
	fakaMaxResponse    = 4 << 20
)

var ErrFakaProcurementNotImplemented = errors.New("faka procurement is not implemented")
var ErrFakaProductCodeMissing = errors.New("faka upstream product code is missing")
var ErrFakaProcurementCapabilityUnavailable = errors.New("faka product is not eligible for automatic procurement")

// ErrFakaTradeNotSent is returned only when the adapter can prove that no trade request reached the server.
var ErrFakaTradeNotSent = errors.New("faka trade request was not sent")

// FakaCapabilityVerifier is reserved for live detail checks before procurement.
type FakaCapabilityVerifier interface {
	VerifyFakaProcurementCapability(context.Context, string, uint, int) (*FakaProductProcurementProfile, error)
}

type FakaProcurementCapabilityStatus string

const (
	FakaCapabilityEligible    FakaProcurementCapabilityStatus = "eligible"
	FakaCapabilityUnsupported FakaProcurementCapabilityStatus = "unsupported"
	FakaCapabilityUnknown     FakaProcurementCapabilityStatus = "unknown"
)

// FakaProductProcurementProfile keeps the original Faka config separate from
// the conservative, parsed purchase-capability assessment.
type FakaProductProcurementProfile struct {
	RawConfig          json.RawMessage           `json:"raw_config"`
	RawWidget          json.RawMessage           `json:"raw_widget"`
	RawDraftStatus     json.RawMessage           `json:"raw_draft_status"`
	RawPasswordStatus  json.RawMessage           `json:"raw_password_status"`
	AssessmentComplete bool                      `json:"assessment_complete"`
	SnapshotValid      bool                      `json:"snapshot_valid"`
	AssessedAt         time.Time                 `json:"assessed_at"`
	Capability         FakaProcurementCapability `json:"capability"`
}

type FakaProcurementCapability struct {
	Status           FakaProcurementCapabilityStatus `json:"status"`
	CanAutoProcure   bool                            `json:"can_auto_procure"`
	MinimumQuantity  int                             `json:"minimum_quantity"`
	MaximumQuantity  int                             `json:"maximum_quantity"`
	DeliveryWay      int                             `json:"delivery_way"`
	RequiresSKU      bool                            `json:"requires_sku"`
	RequiresRace     bool                            `json:"requires_race"`
	RequiresCardID   bool                            `json:"requires_card_id"`
	RequiresPassword bool                            `json:"requires_password"`
	RequiresCoupon   bool                            `json:"requires_coupon"`
	RequiresWidget   bool                            `json:"requires_widget"`
	WidgetFields     []FakaWidgetField               `json:"widget_fields,omitempty"`
	Reasons          []string                        `json:"reasons,omitempty"`
}

type FakaWidgetField struct {
	Name     string `json:"name"`
	Required bool   `json:"required"`
}

func (profile FakaProductProcurementProfile) AllowsAutomaticProcurement(quantity int) bool {
	capability := profile.Capability
	if !profile.AssessmentComplete || !profile.SnapshotValid || capability.Status != FakaCapabilityEligible || !capability.CanAutoProcure || quantity <= 0 {
		return false
	}
	if capability.MinimumQuantity < 0 || capability.MaximumQuantity < 0 {
		return false
	}
	if capability.MinimumQuantity > 0 && quantity < capability.MinimumQuantity {
		return false
	}
	return capability.MaximumQuantity <= 0 || quantity <= capability.MaximumQuantity
}

// FakaIdentityError reports duplicate or inconsistent upstream product identity.
// The IDs and codes identify mappings that must be paused by the caller.
type FakaIdentityError struct {
	Message      string
	ProductIDs   []uint
	ProductCodes []string
}

func (e *FakaIdentityError) Error() string { return e.Message }

// FakaAPIError carries both transport and API-level error details.
type FakaAPIError struct {
	HTTPStatus int
	Code       string
	Message    string
	Body       string
}

func (e *FakaAPIError) Error() string {
	if e.Message != "" {
		return fmt.Sprintf("faka API error (HTTP %d, code %s): %s", e.HTTPStatus, e.Code, e.Message)
	}
	if e.Code != "" {
		return fmt.Sprintf("faka API error (HTTP %d, code %s)", e.HTTPStatus, e.Code)
	}
	return fmt.Sprintf("faka API error (HTTP %d): %s", e.HTTPStatus, e.Body)
}

// FakaSign implements the PHP http_build_query + urldecode + MD5 signature.
// The app key is appended locally and is never part of the returned parameters.
func FakaSign(appKey string, params map[string]any) string {
	keys := make([]string, 0, len(params))
	for key, value := range params {
		if key == "sign" || key == "app_key" {
			continue
		}
		if str, ok := value.(string); ok && str == "" {
			continue
		}
		if value == nil {
			continue
		}
		keys = append(keys, key)
	}
	sort.Strings(keys)

	pairs := make([]string, 0, len(keys))
	for _, key := range keys {
		appendFakaQueryPairs(&pairs, key, params[key])
	}
	encoded := strings.Join(pairs, "&")
	decoded, err := url.QueryUnescape(encoded)
	if err != nil {
		// QueryEscape creates valid URL-encoded data, so this is unreachable.
		decoded = encoded
	}
	sum := md5.Sum([]byte(decoded + "&key=" + appKey))
	return hex.EncodeToString(sum[:])
}

func appendFakaQueryPairs(pairs *[]string, key string, value any) {
	if value == nil {
		return
	}
	v := reflect.ValueOf(value)
	switch v.Kind() {
	case reflect.Map:
		keys := v.MapKeys()
		sort.Slice(keys, func(i, j int) bool { return fmt.Sprint(keys[i].Interface()) < fmt.Sprint(keys[j].Interface()) })
		for _, childKey := range keys {
			appendFakaQueryPairs(pairs, key+"["+fmt.Sprint(childKey.Interface())+"]", v.MapIndex(childKey).Interface())
		}
	case reflect.Slice, reflect.Array:
		for i := 0; i < v.Len(); i++ {
			appendFakaQueryPairs(pairs, key+"["+strconv.Itoa(i)+"]", v.Index(i).Interface())
		}
	default:
		encodedKey := phpFormEscape(key)
		encodedValue := phpFormEscape(fmt.Sprint(value))
		*pairs = append(*pairs, encodedKey+"="+encodedValue)
	}
}

func phpFormEscape(value string) string {
	return strings.ReplaceAll(url.QueryEscape(value), "~", "%7E")
}

// FakaAdapter implements the Faka product, trade and query API.
type FakaAdapter struct {
	baseURL string
	appID   string
	appKey  string
	client  *http.Client
}

func NewFakaAdapter(conn *siteconnectiondomain.Connection) *FakaAdapter {
	return &FakaAdapter{
		baseURL: strings.TrimRight(conn.BaseURL, "/"),
		appID:   conn.ApiKey,
		appKey:  conn.ApiSecret,
		client: &http.Client{
			Timeout: fakaRequestTimeout,
			CheckRedirect: func(_ *http.Request, _ []*http.Request) error {
				return http.ErrUseLastResponse
			},
		},
	}
}

func (a *FakaAdapter) Ping(ctx context.Context) (*PingResult, error) {
	var categories []fakaCategory
	if err := a.doRequest(ctx, "/shared/commodity/items", nil, &categories); err != nil {
		return nil, err
	}
	return &PingResult{ProtocolVersion: "faka-api"}, nil
}

func (a *FakaAdapter) ListCategories(ctx context.Context) (*CategoryListResult, error) {
	var remote []fakaCategory
	if err := a.doRequest(ctx, "/shared/commodity/items", nil, &remote); err != nil {
		return nil, err
	}
	categories := make([]UpstreamCategory, 0, len(remote))
	for _, category := range remote {
		categories = append(categories, UpstreamCategory{
			ID:        category.ID,
			Slug:      categorySlug(category.Name, category.ID),
			Name:      jsonmap.JSON{"zh-CN": category.Name},
			SortOrder: category.Sort,
		})
	}
	return &CategoryListResult{Supported: true, Categories: categories}, nil
}

func (a *FakaAdapter) ListProducts(ctx context.Context, opts ListProductsOpts) (*ProductListResult, error) {
	var remote []fakaCategory
	if err := a.doRequest(ctx, "/shared/commodity/items", nil, &remote); err != nil {
		return nil, err
	}
	var all []UpstreamProduct
	for _, category := range remote {
		for _, item := range category.Children {
			all = append(all, item.toProduct(category.ID))
		}
	}
	if err := validateFakaProductIdentities(all); err != nil {
		return nil, err
	}
	total := len(all)
	pageSize := opts.PageSize
	if pageSize <= 0 {
		pageSize = total
	}
	page := opts.Page
	if page <= 0 {
		page = 1
	}
	if total == 0 {
		return &ProductListResult{Total: 0, Items: []UpstreamProduct{}}, nil
	}
	if page > 1 && page-1 > total/pageSize {
		return &ProductListResult{Total: total, Items: []UpstreamProduct{}}, nil
	}
	start := (page - 1) * pageSize
	if start >= total {
		return &ProductListResult{Total: total, Items: []UpstreamProduct{}}, nil
	}
	end := start + pageSize
	if end > total {
		end = total
	}
	return &ProductListResult{Total: total, Items: all[start:end]}, nil
}

func (a *FakaAdapter) GetProduct(ctx context.Context, productID uint) (*UpstreamProduct, error) {
	// The Dujiao-Next adapter contract uses a numeric ID while Faka requires
	// product code. Resolve the code through the official list endpoint.
	products, err := a.ListProducts(ctx, ListProductsOpts{Page: 1, PageSize: int(^uint(0) >> 1)})
	if err != nil {
		return nil, err
	}
	var code string
	for _, product := range products.Items {
		if product.ID == productID {
			code = product.Code
			break
		}
	}
	if code == "" {
		return nil, ErrUpstreamProductDeleted
	}
	product, err := a.GetProductByCode(ctx, code)
	if err != nil {
		return nil, err
	}
	if product.ID != productID {
		return nil, &FakaIdentityError{
			Message:      fmt.Sprintf("faka product ID mismatch: requested %d for code %q, received %d", productID, code, product.ID),
			ProductIDs:   []uint{productID, product.ID},
			ProductCodes: []string{code, product.Code},
		}
	}
	return product, nil
}

func (a *FakaAdapter) GetProductByCode(ctx context.Context, code string) (*UpstreamProduct, error) {
	if code == "" {
		return nil, ErrFakaProductCodeMissing
	}
	products, err := a.ListProducts(ctx, ListProductsOpts{Page: 1, PageSize: int(^uint(0) >> 1)})
	if err != nil {
		return nil, err
	}
	var categoryID uint
	found := false
	for _, product := range products.Items {
		if product.Code == code {
			categoryID = product.CategoryID
			found = true
			break
		}
	}
	if !found {
		return nil, ErrUpstreamProductDeleted
	}
	var remote fakaItem
	if err := a.doRequest(ctx, "/shared/commodity/item", map[string]any{"code": code}, &remote); err != nil {
		return nil, err
	}
	product := remote.toProduct(categoryID)
	if product.Code != code {
		return nil, &FakaIdentityError{
			Message:      fmt.Sprintf("faka product code mismatch: requested %q, received %q (id %d)", code, product.Code, product.ID),
			ProductIDs:   []uint{product.ID},
			ProductCodes: []string{code, product.Code},
		}
	}
	return &product, nil
}

func (a *FakaAdapter) VerifyFakaProcurementCapability(ctx context.Context, code string, expectedID uint, quantity int) (*FakaProductProcurementProfile, error) {
	product, err := a.GetProductByCode(ctx, code)
	if err != nil {
		return nil, err
	}
	if product.ID != expectedID || product.Code != code {
		return nil, &FakaIdentityError{
			Message:    fmt.Sprintf("faka live capability identity mismatch: expected id/code=%d/%q, received=%d/%q", expectedID, code, product.ID, product.Code),
			ProductIDs: []uint{expectedID, product.ID}, ProductCodes: []string{code, product.Code},
		}
	}
	if product.FakaProcurement == nil || !product.FakaProcurement.AllowsAutomaticProcurement(quantity) {
		return product.FakaProcurement, ErrFakaProcurementCapabilityUnavailable
	}
	return product.FakaProcurement, nil
}

func validateFakaProductIdentities(products []UpstreamProduct) error {
	ids := make(map[uint]string, len(products))
	codes := make(map[string]uint, len(products))
	conflictingIDs := make(map[uint]struct{})
	conflictingCodes := make(map[string]struct{})
	for _, product := range products {
		if product.Code == "" {
			return &FakaIdentityError{Message: fmt.Sprintf("faka product id %d has an empty code", product.ID), ProductIDs: []uint{product.ID}}
		}
		if priorCode, ok := ids[product.ID]; ok {
			conflictingIDs[product.ID] = struct{}{}
			conflictingCodes[priorCode] = struct{}{}
			conflictingCodes[product.Code] = struct{}{}
		}
		if priorID, ok := codes[product.Code]; ok {
			conflictingIDs[priorID] = struct{}{}
			conflictingIDs[product.ID] = struct{}{}
			conflictingCodes[product.Code] = struct{}{}
		}
		ids[product.ID] = product.Code
		codes[product.Code] = product.ID
	}
	if len(conflictingIDs) == 0 && len(conflictingCodes) == 0 {
		return nil
	}
	identityErr := &FakaIdentityError{}
	for id := range conflictingIDs {
		identityErr.ProductIDs = append(identityErr.ProductIDs, id)
	}
	for code := range conflictingCodes {
		identityErr.ProductCodes = append(identityErr.ProductCodes, code)
	}
	sort.Slice(identityErr.ProductIDs, func(i, j int) bool { return identityErr.ProductIDs[i] < identityErr.ProductIDs[j] })
	sort.Strings(identityErr.ProductCodes)
	identityErr.Message = fmt.Sprintf("faka product list contains duplicate IDs %v and duplicate codes %q", identityErr.ProductIDs, identityErr.ProductCodes)
	return identityErr
}

func (a *FakaAdapter) CreateOrder(ctx context.Context, req CreateUpstreamOrderReq) (*CreateUpstreamOrderResp, error) {
	if strings.TrimSpace(req.SharedCode) == "" {
		return nil, errors.Join(ErrFakaTradeNotSent, ErrFakaProductCodeMissing)
	}
	if req.Quantity <= 0 || strings.TrimSpace(req.TraceID) == "" {
		return nil, fmt.Errorf("%w: invalid quantity or missing persistent request number", ErrFakaTradeNotSent)
	}
	var data struct {
		TradeNo string          `json:"tradeNo"`
		Amount  json.RawMessage `json:"amount"`
		Secret  json.RawMessage `json:"secret"`
		Stock   json.RawMessage `json:"stock"`
	}
	err := a.doRequest(ctx, "/shared/commodity/trade", map[string]any{
		"shared_code": req.SharedCode,
		"num":         req.Quantity,
		"request_no":  req.TraceID,
	}, &data)
	if err != nil {
		// A dial error establishes that the request was never written. All other transport
		// and API errors remain uncertain, including code=0 and a timeout during setup.
		var opErr *net.OpError
		if errors.As(err, &opErr) && opErr.Op == "dial" && !errors.Is(err, context.DeadlineExceeded) && !errors.Is(err, context.Canceled) {
			return nil, fmt.Errorf("%w: %v", ErrFakaTradeNotSent, err)
		}
		return nil, err
	}
	amount := strings.TrimSpace(string(data.Amount))
	if len(amount) > 0 && amount[0] == '"' {
		if unmarshalErr := json.Unmarshal(data.Amount, &amount); unmarshalErr != nil {
			amount = ""
		}
	}
	parsedAmount, parseErr := decimal.NewFromString(amount)
	if strings.TrimSpace(data.TradeNo) == "" || parseErr != nil || parsedAmount.IsNegative() {
		if strings.TrimSpace(data.TradeNo) != "" {
			return &CreateUpstreamOrderResp{OrderNo: data.TradeNo}, fmt.Errorf("faka trade success response has invalid amount; tradeNo must be verified")
		}
		return nil, fmt.Errorf("faka trade success response has missing tradeNo or invalid amount")
	}
	if secret := bytes.TrimSpace(data.Secret); len(secret) > 0 && !bytes.Equal(secret, []byte("null")) {
		var value string
		if json.Unmarshal(secret, &value) != nil {
			return &CreateUpstreamOrderResp{OrderNo: data.TradeNo}, fmt.Errorf("faka trade success response has invalid secret type; tradeNo must be verified")
		}
	}
	if stock := bytes.TrimSpace(data.Stock); len(stock) > 0 && !bytes.Equal(stock, []byte("null")) {
		var stockErr error
		if stock[0] == '"' {
			var value string
			stockErr = json.Unmarshal(stock, &value)
		} else {
			var value int
			stockErr = json.Unmarshal(stock, &value)
		}
		if stockErr != nil {
			return &CreateUpstreamOrderResp{OrderNo: data.TradeNo}, fmt.Errorf("faka trade success response has invalid stock type; tradeNo must be verified")
		}
	}
	return &CreateUpstreamOrderResp{OK: true, OrderNo: data.TradeNo, Status: "accepted", Amount: amount}, nil
}

type FakaQueryResult struct {
	PaymentStatus int
	Secret        string
	Widget        json.RawMessage
}

// QueryOrderByTradeNo uses the original string identifier. A paid response
// does not establish that the secret is a deliverable card code.
func (a *FakaAdapter) QueryOrderByTradeNo(ctx context.Context, tradeNo string) (*FakaQueryResult, error) {
	if strings.TrimSpace(tradeNo) == "" {
		return nil, fmt.Errorf("faka query requires a persisted tradeNo")
	}
	var data struct {
		Status json.RawMessage `json:"status"`
		Secret json.RawMessage `json:"secret"`
		Widget json.RawMessage `json:"widget"`
	}
	if err := a.doRequest(ctx, "/shared/commodity/query", map[string]any{"tradeNo": tradeNo}, &data); err != nil {
		return nil, err
	}
	statusRaw := bytes.TrimSpace(data.Status)
	if !bytes.Equal(statusRaw, []byte("0")) && !bytes.Equal(statusRaw, []byte("1")) {
		return nil, fmt.Errorf("faka query returned an unsupported payment status")
	}
	status := int(statusRaw[0] - '0')
	var secret string
	if len(data.Secret) > 0 && string(data.Secret) != "null" {
		if err := json.Unmarshal(data.Secret, &secret); err != nil {
			return nil, fmt.Errorf("faka query returned an invalid secret type: %w", err)
		}
	}
	widget := json.RawMessage(nil)
	widgetValue := bytes.TrimSpace(data.Widget)
	if len(widgetValue) > 0 && !bytes.Equal(widgetValue, []byte("null")) {
		if widgetValue[0] != '{' {
			return nil, fmt.Errorf("faka query returned an invalid widget type")
		}
		widget = widgetValue
	}
	return &FakaQueryResult{PaymentStatus: status, Secret: secret, Widget: widget}, nil
}

func (a *FakaAdapter) GetOrder(context.Context, uint) (*UpstreamOrderDetail, error) {
	return nil, ErrFakaProcurementNotImplemented
}

func (a *FakaAdapter) CancelOrder(context.Context, uint) error {
	return ErrFakaProcurementNotImplemented
}

func (a *FakaAdapter) DownloadImage(_ context.Context, _ string) (string, error) {
	return "", errors.New("faka image download is not implemented")
}

func (a *FakaAdapter) doRequest(ctx context.Context, endpoint string, params map[string]any, result any) error {
	values := make(url.Values)
	formParams := make(map[string]any, len(params)+1)
	for key, value := range params {
		if key == "app_key" {
			continue
		}
		formParams[key] = value
	}
	formParams["app_id"] = a.appID
	for key, value := range formParams {
		if key == "sign" || key == "app_key" {
			continue
		}
		var pairs []string
		appendFakaQueryPairs(&pairs, key, value)
		for _, pair := range pairs {
			parts := strings.SplitN(pair, "=", 2)
			decodedKey, _ := url.QueryUnescape(parts[0])
			decodedValue, _ := url.QueryUnescape(parts[1])
			values.Add(decodedKey, decodedValue)
		}
	}
	formParams["sign"] = FakaSign(a.appKey, formParams)

	body := values.Encode()
	body += "&sign=" + phpFormEscape(formParams["sign"].(string))
	req, err := http.NewRequestWithContext(ctx, http.MethodPost, a.baseURL+endpoint, strings.NewReader(body))
	if err != nil {
		return fmt.Errorf("create faka request: %w", err)
	}
	req.Header.Set("Content-Type", "application/x-www-form-urlencoded")
	resp, err := a.client.Do(req)
	if err != nil {
		return fmt.Errorf("send faka request: %w", err)
	}
	defer resp.Body.Close()

	respBody, err := io.ReadAll(io.LimitReader(resp.Body, fakaMaxResponse+1))
	if err != nil {
		return fmt.Errorf("read faka response: %w", err)
	}
	if len(respBody) > fakaMaxResponse {
		return fmt.Errorf("faka response exceeds %d bytes", fakaMaxResponse)
	}

	var envelope struct {
		Code json.RawMessage `json:"code"`
		Msg  string          `json:"msg"`
		Data json.RawMessage `json:"data"`
	}
	if err := json.Unmarshal(respBody, &envelope); err != nil {
		return &FakaAPIError{HTTPStatus: resp.StatusCode, Body: truncateFakaBody(respBody), Message: "decode response JSON: " + err.Error()}
	}
	code := strings.Trim(string(envelope.Code), `"`)
	if resp.StatusCode < http.StatusOK || resp.StatusCode >= http.StatusMultipleChoices || code != "200" {
		return &FakaAPIError{HTTPStatus: resp.StatusCode, Code: code, Message: envelope.Msg, Body: truncateFakaBody(respBody)}
	}
	if result == nil || len(envelope.Data) == 0 || string(envelope.Data) == "null" {
		return nil
	}
	if err := json.Unmarshal(envelope.Data, result); err != nil {
		return fmt.Errorf("decode faka response data: %w", err)
	}
	return nil
}

func truncateFakaBody(body []byte) string {
	const maxErrorBody = 512
	if len(body) > maxErrorBody {
		return string(body[:maxErrorBody])
	}
	return string(body)
}

type fakaCategory struct {
	ID       uint       `json:"id"`
	Name     string     `json:"name"`
	Sort     int        `json:"sort"`
	Children []fakaItem `json:"children"`
}

type fakaItem struct {
	ID                uint            `json:"id"`
	Name              string          `json:"name"`
	Description       string          `json:"description"`
	Code              string          `json:"code"`
	Price             json.RawMessage `json:"price"`
	UserPrice         json.RawMessage `json:"user_price"`
	Stock             json.RawMessage `json:"stock"`
	DeliveryWay       *int            `json:"delivery_way"`
	Minimum           *int            `json:"minimum"`
	Maximum           *int            `json:"maximum"`
	Config            json.RawMessage `json:"config"`
	Widget            json.RawMessage `json:"widget"`
	RawDraftStatus    json.RawMessage `json:"draft_status"`
	RawPasswordStatus json.RawMessage `json:"password_status"`
	DraftStatus       *int            `json:"-"`
	PasswordStatus    *int            `json:"-"`
	IsActive          *bool           `json:"is_active"`
	Images            []string        `json:"images"`
}

func (item fakaItem) toProduct(categoryID uint) UpstreamProduct {
	active := true
	if item.IsActive != nil {
		active = *item.IsActive
	}
	widget := fakaWidgetSchema(item.Widget)
	return UpstreamProduct{
		ID:               item.ID,
		Code:             item.Code,
		Slug:             item.Code,
		Title:            jsonmap.JSON{"zh-CN": item.Name},
		Description:      jsonmap.JSON{"zh-CN": item.Description},
		PriceAmount:      fakaJSONNumber(item.Price),
		MemberPrice:      fakaJSONNumber(item.UserPrice),
		FulfillmentType:  "upstream",
		ManualFormSchema: widget,
		FakaProcurement:  assessFakaProcurementCapability(item),
		IsActive:         active,
		CategoryID:       categoryID,
		Images:           item.Images,
	}
}

func assessFakaProcurementCapability(item fakaItem) *FakaProductProcurementProfile {
	item.DraftStatus = parseFakaStatus(item.RawDraftStatus)
	item.PasswordStatus = parseFakaStatus(item.RawPasswordStatus)
	assessmentComplete := true
	capability := FakaProcurementCapability{
		Status: FakaCapabilityEligible, CanAutoProcure: true,
	}
	markUnknown := func(reason string) {
		capability.Status = FakaCapabilityUnknown
		capability.CanAutoProcure = false
		capability.Reasons = append(capability.Reasons, reason)
	}
	markIncomplete := func(reason string) {
		assessmentComplete = false
		markUnknown(reason)
	}
	markUnsupported := func(reason string) {
		if capability.Status != FakaCapabilityUnknown {
			capability.Status = FakaCapabilityUnsupported
		}
		capability.CanAutoProcure = false
		capability.Reasons = append(capability.Reasons, reason)
	}
	if item.Minimum == nil {
		markIncomplete("minimum quantity is missing")
	} else if *item.Minimum < 0 {
		markIncomplete("minimum quantity is negative")
	} else {
		capability.MinimumQuantity = *item.Minimum
	}
	if item.Maximum == nil {
		markIncomplete("maximum quantity is missing")
	} else if *item.Maximum < 0 {
		markIncomplete("maximum quantity is negative")
	} else {
		capability.MaximumQuantity = *item.Maximum
	}
	if item.Minimum != nil && item.Maximum != nil && *item.Minimum > 0 && *item.Maximum > 0 && *item.Minimum > *item.Maximum {
		markIncomplete("minimum quantity exceeds maximum quantity")
	}
	if item.DeliveryWay == nil {
		markIncomplete("delivery_way is missing")
	} else {
		capability.DeliveryWay = *item.DeliveryWay
	}

	if len(item.Config) == 0 || string(item.Config) == "null" {
		markIncomplete("product config is missing")
	} else {
		var config map[string]json.RawMessage
		if err := json.Unmarshal(item.Config, &config); err != nil || config == nil {
			markIncomplete("product config is not a JSON object")
		} else {
			for key, value := range config {
				switch strings.ToLower(key) {
				case "sku":
					capability.RequiresSKU = true
					markUnsupported("product config requires SKU selection")
				case "category", "race":
					capability.RequiresRace = true
					markUnsupported("product config requires race selection")
				case "card_id", "draft", "draft_status":
					capability.RequiresCardID = true
					markUnsupported("product config requires preselected card data")
				case "password", "password_status":
					capability.RequiresPassword = true
					markUnsupported("product config requires order password")
				case "coupon":
					capability.RequiresCoupon = true
					markUnsupported("product config requires upstream coupon")
				default:
					markUnknown("product config contains unrecognized key: " + key)
				}
				_ = value
			}
		}
	}

	if item.DraftStatus == nil {
		markIncomplete("draft_status is missing")
	} else if *item.DraftStatus == 1 {
		capability.RequiresCardID = true
		markUnsupported("product requires a preselected card")
	} else if *item.DraftStatus != 0 {
		markIncomplete("draft_status has an unrecognized value")
	}
	if item.PasswordStatus == nil {
		markIncomplete("password_status is missing")
	} else if *item.PasswordStatus == 1 {
		capability.RequiresPassword = true
		markUnsupported("product requires an order password")
	} else if *item.PasswordStatus != 0 {
		markIncomplete("password_status has an unrecognized value")
	}
	if len(item.Widget) == 0 {
		markIncomplete("widget field is missing")
	} else if string(item.Widget) != "null" && string(item.Widget) != "[]" {
		var widgets []map[string]json.RawMessage
		if err := json.Unmarshal(item.Widget, &widgets); err != nil {
			markIncomplete("widget config is not a JSON array")
		} else if len(widgets) > 0 {
			capability.RequiresWidget = true
			markUnsupported("product requires widget values")
			for _, widget := range widgets {
				var name string
				if err := json.Unmarshal(widget["name"], &name); err != nil || strings.TrimSpace(name) == "" {
					markIncomplete("widget contains a field without a valid name")
					continue
				}
				field := FakaWidgetField{Name: name}
				if required, ok := widget["required"]; ok {
					if err := json.Unmarshal(required, &field.Required); err != nil {
						markIncomplete("widget required flag is invalid")
					}
				}
				capability.WidgetFields = append(capability.WidgetFields, field)
			}
		}
	}
	if capability.Status == FakaCapabilityUnknown || capability.Status == FakaCapabilityUnsupported {
		capability.CanAutoProcure = false
	}
	return &FakaProductProcurementProfile{
		RawConfig:          append(json.RawMessage(nil), item.Config...),
		RawWidget:          append(json.RawMessage(nil), item.Widget...),
		RawDraftStatus:     append(json.RawMessage(nil), item.RawDraftStatus...),
		RawPasswordStatus:  append(json.RawMessage(nil), item.RawPasswordStatus...),
		AssessmentComplete: assessmentComplete,
		SnapshotValid:      assessmentComplete,
		AssessedAt:         time.Now().UTC(),
		Capability:         capability,
	}
}

func parseFakaStatus(raw json.RawMessage) *int {
	if len(raw) == 0 || string(raw) == "null" {
		return nil
	}
	var value int
	if json.Unmarshal(raw, &value) != nil {
		return nil
	}
	return &value
}

// MappingSnapshot returns the serializable capability snapshot stored on the mapping.
func (profile *FakaProductProcurementProfile) MappingSnapshot() jsonmap.JSON {
	if profile == nil {
		return nil
	}
	return jsonmap.JSON{
		"raw_config": string(profile.RawConfig), "raw_widget": string(profile.RawWidget),
		"raw_draft_status": string(profile.RawDraftStatus), "raw_password_status": string(profile.RawPasswordStatus),
		"assessment_complete": profile.AssessmentComplete, "snapshot_valid": profile.SnapshotValid,
		"assessed_at": profile.AssessedAt, "capability": profile.Capability,
	}
}

func isEmptyFakaConfigValue(value json.RawMessage) bool {
	trimmed := strings.TrimSpace(string(value))
	return trimmed == "" || trimmed == "null" || trimmed == `""` || trimmed == "[]" || trimmed == "{}"
}

func fakaWidgetSchema(raw json.RawMessage) jsonmap.JSON {
	if len(raw) == 0 || string(raw) == "null" {
		return nil
	}
	var object jsonmap.JSON
	if err := json.Unmarshal(raw, &object); err == nil {
		return object
	}
	var fields []any
	if err := json.Unmarshal(raw, &fields); err == nil {
		return jsonmap.JSON{"fields": fields}
	}
	return nil
}

func fakaJSONNumber(raw json.RawMessage) string {
	if len(raw) == 0 || string(raw) == "null" {
		return "0"
	}
	var value string
	if err := json.Unmarshal(raw, &value); err == nil {
		return value
	}
	return string(raw)
}

func categorySlug(name string, id uint) string {
	name = strings.TrimSpace(name)
	if name == "" {
		return fmt.Sprintf("faka-category-%d", id)
	}
	return fmt.Sprintf("%s-%d", name, id)
}

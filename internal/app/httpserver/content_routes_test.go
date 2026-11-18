package httpserver

import (
	"encoding/json"
	"fmt"
	"net/http"
	"net/http/httptest"
	"os"
	"path/filepath"
	"regexp"
	"runtime"
	"sort"
	"strings"
	"testing"
	"time"

	"github.com/dujiao-next/internal/app/httpserver/middleware"
	"github.com/dujiao-next/internal/authz"
	contenttransport "github.com/dujiao-next/internal/modules/content/transport/http"
	admincontract "github.com/dujiao-next/internal/modules/identity/admin/contract"
	admindomain "github.com/dujiao-next/internal/modules/identity/admin/domain"
	adminstore "github.com/dujiao-next/internal/modules/identity/admin/infrastructure/gormstore"
	adminauthapp "github.com/dujiao-next/internal/modules/identity/adminauth/application"
	"github.com/dujiao-next/internal/modules/identity/jwttoken"

	procurementcontract "github.com/dujiao-next/internal/modules/procurement/contract"
	procurementdomain "github.com/dujiao-next/internal/modules/procurement/domain"
	procurementhttp "github.com/dujiao-next/internal/modules/procurement/transport/http"
	"github.com/dujiao-next/internal/platform/http/response"
	"github.com/gin-gonic/gin"
	"github.com/glebarez/sqlite"
	"github.com/golang-jwt/jwt/v5"
	"gorm.io/gorm"
)

const contentAdminJWTSecret = "content-admin-route-contract-secret"

var contentAdminRouteContract = []adminRoute{
	{method: http.MethodGet, object: "/admin/posts"},
	{method: http.MethodPost, object: "/admin/posts"},
	{method: http.MethodPut, object: "/admin/posts/:id"},
	{method: http.MethodDelete, object: "/admin/posts/:id"},
	{method: http.MethodGet, object: "/admin/posts/:id/products"},
	{method: http.MethodGet, object: "/admin/post-categories"},
	{method: http.MethodPost, object: "/admin/post-categories"},
	{method: http.MethodPut, object: "/admin/post-categories/:id"},
	{method: http.MethodDelete, object: "/admin/post-categories/:id"},
	{method: http.MethodPatch, object: "/admin/post-categories/:id/status"},
	{method: http.MethodGet, object: "/admin/banners"},
	{method: http.MethodGet, object: "/admin/banners/:id"},
	{method: http.MethodPost, object: "/admin/banners"},
	{method: http.MethodPut, object: "/admin/banners/:id"},
	{method: http.MethodDelete, object: "/admin/banners/:id"},
	{method: http.MethodGet, object: "/admin/media"},
	{method: http.MethodPost, object: "/admin/media/batch-delete"},
	{method: http.MethodPut, object: "/admin/media/:id"},
	{method: http.MethodDelete, object: "/admin/media/:id"},
}

var contentPublicRouteContract = []adminRoute{
	{method: http.MethodGet, object: "/api/v1/public/posts"},
	{method: http.MethodGet, object: "/api/v1/public/posts/:slug"},
	{method: http.MethodGet, object: "/api/v1/public/banners"},
	{method: http.MethodGet, object: "/api/v1/public/post-categories"},
}

func TestContentRouteContractMatchesRouterSource(t *testing.T) {
	adminRoutes, err := extractAdminRoutesFromSource()
	if err != nil {
		t.Fatalf("extract admin routes: %v", err)
	}
	actualAdmin := make([]adminRoute, 0, len(contentAdminRouteContract))
	for _, route := range adminRoutes {
		if isContentAdminRoute(route.object) {
			actualAdmin = append(actualAdmin, route)
		}
	}
	assertRouteSetsEqual(t, contentAdminRouteContract, actualAdmin)

	publicRoutes, err := extractPublicContentRoutesFromSource()
	if err != nil {
		t.Fatalf("extract public content routes: %v", err)
	}
	assertRouteSetsEqual(t, contentPublicRouteContract, publicRoutes)
}

func TestContentRoutesStayInExpectedRouterTrustGroups(t *testing.T) {
	_, thisFile, _, ok := runtime.Caller(0)
	if !ok {
		t.Fatal("resolve content route test filename")
	}
	routerDirectory := filepath.Dir(thisFile)
	registrations := []struct {
		file      string
		statement string
	}{
		{file: "routes_storefront.go", statement: "contenttransport.RegisterPublicRoutes(public, publicContentHandler)"},
		{file: "routes_admin.go", statement: "contenttransport.RegisterAdminRoutes(authorized, adminContentHandler)"},
	}

	for _, registration := range registrations {
		raw, err := os.ReadFile(filepath.Join(routerDirectory, registration.file))
		if err != nil {
			t.Fatalf("read production route file %s: %v", registration.file, err)
		}
		source := string(raw)
		if !strings.Contains(source, registration.statement) {
			t.Errorf("%s must contain %q; changing the route group can bypass the intended authentication or public boundary", registration.file, registration.statement)
		}
	}
}

func TestAdminContentRoutesRequireAuthenticationAndOperationsPermission(t *testing.T) {
	adminRepo, authzService, noPermissionToken, operationsToken := setupContentRouteAccessTest(t)

	tests := []struct {
		name       string
		token      string
		wantStatus int
	}{
		{name: "anonymous", wantStatus: response.CodeUnauthorized},
		{name: "invalid token", token: "not-a-jwt", wantStatus: response.CodeUnauthorized},
		{name: "no permission", token: noPermissionToken, wantStatus: response.CodeForbidden},
		{name: "operations", token: operationsToken, wantStatus: response.CodeOK},
	}

	for _, route := range contentAdminRouteContract {
		route := route
		t.Run(route.method+" "+route.object, func(t *testing.T) {
			for _, test := range tests {
				t.Run(test.name, func(t *testing.T) {
					router := newContentRouteAccessRouter(adminRepo, authzService)
					path := strings.ReplaceAll("/api/v1"+route.object, ":id", "42")
					request := httptest.NewRequest(route.method, path, nil)
					if test.token != "" {
						request.Header.Set("Authorization", "Bearer "+test.token)
					}
					recorder := httptest.NewRecorder()
					router.ServeHTTP(recorder, request)

					var got struct {
						StatusCode int `json:"status_code"`
					}
					if err := json.Unmarshal(recorder.Body.Bytes(), &got); err != nil {
						t.Fatalf("decode response: %v body=%s", err, recorder.Body.String())
					}
					if got.StatusCode != test.wantStatus {
						t.Fatalf("business status want %d got %d body=%s", test.wantStatus, got.StatusCode, recorder.Body.String())
					}
				})
			}
		})
	}
}

func extractPublicContentRoutesFromSource() ([]adminRoute, error) {
	_, thisFile, _, ok := runtime.Caller(0)
	if !ok {
		return nil, fmt.Errorf("resolve content route test filename")
	}
	routerSource := filepath.Join(filepath.Dir(thisFile), "..", "..", "modules", "content", "transport", "http", "routes.go")
	raw, err := os.ReadFile(routerSource)
	if err != nil {
		return nil, err
	}

	re := regexp.MustCompile(`public\.(GET|POST|PUT|PATCH|DELETE)\("([^"]+)"`)
	matches := re.FindAllStringSubmatch(string(raw), -1)
	routes := make([]adminRoute, 0, len(contentPublicRouteContract))
	for _, match := range matches {
		path := "/api/v1/public" + match[2]
		if isContentPublicRoute(path) {
			routes = append(routes, adminRoute{method: match[1], object: path})
		}
	}
	return routes, nil
}

func isContentAdminRoute(object string) bool {
	return object == "/admin/posts" || strings.HasPrefix(object, "/admin/posts/") ||
		object == "/admin/post-categories" || strings.HasPrefix(object, "/admin/post-categories/") ||
		object == "/admin/banners" || strings.HasPrefix(object, "/admin/banners/") ||
		object == "/admin/media" || strings.HasPrefix(object, "/admin/media/")
}

func isContentPublicRoute(path string) bool {
	return path == "/api/v1/public/posts" || path == "/api/v1/public/posts/:slug" ||
		path == "/api/v1/public/banners" || path == "/api/v1/public/post-categories"
}

func assertRouteSetsEqual(t *testing.T, want, got []adminRoute) {
	t.Helper()
	normalize := func(routes []adminRoute) []string {
		result := make([]string, 0, len(routes))
		for _, route := range routes {
			result = append(result, route.method+" "+route.object)
		}
		sort.Strings(result)
		return result
	}
	wantRoutes := normalize(want)
	gotRoutes := normalize(got)
	if strings.Join(wantRoutes, "\n") != strings.Join(gotRoutes, "\n") {
		t.Fatalf("content route contract mismatch\nwant:\n%s\ngot:\n%s", strings.Join(wantRoutes, "\n"), strings.Join(gotRoutes, "\n"))
	}
}

func setupContentRouteAccessTest(t *testing.T) (admincontract.Store, *authz.Service, string, string) {
	t.Helper()
	gin.SetMode(gin.TestMode)

	dsn := fmt.Sprintf("file:content_route_access_%d?mode=memory&cache=shared", time.Now().UnixNano())
	db, err := gorm.Open(sqlite.Open(dsn), &gorm.Config{})
	if err != nil {
		t.Fatalf("open sqlite: %v", err)
	}
	if err := db.AutoMigrate(&admindomain.Admin{}); err != nil {
		t.Fatalf("auto migrate admins: %v", err)
	}

	adminRepo := adminstore.New(db)
	noPermission := &admindomain.Admin{Username: "content-none", PasswordHash: "hash"}
	operations := &admindomain.Admin{Username: "content-operations", PasswordHash: "hash"}
	if err := adminRepo.Create(noPermission); err != nil {
		t.Fatalf("create no-permission admin: %v", err)
	}
	if err := adminRepo.Create(operations); err != nil {
		t.Fatalf("create operations admin: %v", err)
	}

	authzService, err := authz.NewService(db)
	if err != nil {
		t.Fatalf("create authz service: %v", err)
	}
	if err := authzService.BootstrapBuiltinRoles(); err != nil {
		t.Fatalf("bootstrap builtin roles: %v", err)
	}
	if err := authzService.SetAdminRoles(operations.ID, []string{"operations"}); err != nil {
		t.Fatalf("assign operations role: %v", err)
	}

	return adminRepo, authzService,
		signContentAdminToken(t, noPermission),
		signContentAdminToken(t, operations)
}

func signContentAdminToken(t *testing.T, admin *admindomain.Admin) string {
	t.Helper()
	now := time.Now()
	claims := adminauthapp.JWTClaims{
		AdminID:      admin.ID,
		Username:     admin.Username,
		TokenVersion: admin.TokenVersion,
		Typ:          jwttoken.TypeAccess,
		RegisteredClaims: jwt.RegisteredClaims{
			ExpiresAt: jwt.NewNumericDate(now.Add(time.Hour)),
			IssuedAt:  jwt.NewNumericDate(now),
			NotBefore: jwt.NewNumericDate(now.Add(-time.Second)),
		},
	}
	token := jwt.NewWithClaims(jwt.SigningMethodHS256, claims)
	signed, err := token.SignedString([]byte(contentAdminJWTSecret))
	if err != nil {
		t.Fatalf("sign admin token: %v", err)
	}
	return signed
}

func newContentRouteAccessRouter(adminRepo admincontract.Store, authzService *authz.Service) *gin.Engine {
	router := gin.New()
	admin := router.Group("/api/v1/admin")
	admin.Use(middleware.JWTAuthMiddleware(contentAdminJWTSecret, adminRepo), middleware.AdminRBACMiddleware(authzService))
	admin.Use(func(c *gin.Context) {
		response.Success(c, nil)
		c.Abort()
	})
	contenttransport.RegisterAdminRoutes(admin, contenttransport.NewAdminHandler(nil, nil, nil, nil))
	return router
}

const procurementConflictJWTSecret = "procurement-conflict-route-test-secret"

func TestProcurementTradeEvidenceConflictRoutesEnforceAdminAccessAndIdentity(t *testing.T) {
	adminRepo, authzService, noPermissionToken, integrationToken, integrationAdminID := setupProcurementConflictRouteAccessTest(t)
	service := &procurementConflictHTTPService{
		conflicts: []procurementdomain.TradeEvidenceConflict{{
			ID: 17, ConflictKey: "conflict-key", EventID: "event-17", ProcurementOrderID: 9,
			TraceID: "trace-9", ConflictType: "event_content_mismatch", ExistingEventHash: "old-hash",
			IncomingEventHash: "new-hash", Status: "pending",
		}},
		journal: []procurementdomain.TradeResponseJournalEntry{{
			ID: 1, ProcurementOrderID: 9, TraceID: "trace-9", UpstreamTradeNo: "TRADE-9", Amount: "12.00", EventType: "trade_response",
		}},
	}
	router := newProcurementConflictAccessRouter(adminRepo, authzService, service)

	queryPath := "/api/v1/admin/procurement-trade-evidence-conflicts?event_id=event-17"
	resolvePath := "/api/v1/admin/procurement-trade-evidence-conflicts/17/resolve"
	denied := []struct {
		name   string
		method string
		path   string
		token  string
		body   string
		status int
	}{
		{name: "anonymous query", method: http.MethodGet, path: queryPath, status: response.CodeUnauthorized},
		{name: "anonymous resolve", method: http.MethodPost, path: resolvePath, body: `{"note":"reviewed by operator"}`, status: response.CodeUnauthorized},
		{name: "ordinary user query", method: http.MethodGet, path: queryPath, token: signProcurementConflictUserToken(t), status: response.CodeUnauthorized},
		{name: "ordinary user resolve with forged admin id", method: http.MethodPost, path: resolvePath, token: signProcurementConflictUserToken(t), body: `{"admin_id":999,"note":"reviewed by operator"}`, status: response.CodeUnauthorized},
		{name: "admin without permission query", method: http.MethodGet, path: queryPath, token: noPermissionToken, status: response.CodeForbidden},
		{name: "admin without permission resolve", method: http.MethodPost, path: resolvePath, token: noPermissionToken, body: `{"note":"reviewed by operator"}`, status: response.CodeForbidden},
	}
	for _, tc := range denied {
		t.Run(tc.name, func(t *testing.T) {
			before := service.conflicts[0]
			request := httptest.NewRequest(tc.method, tc.path, strings.NewReader(tc.body))
			if tc.body != "" {
				request.Header.Set("Content-Type", "application/json")
			}
			if tc.token != "" {
				request.Header.Set("Authorization", "Bearer "+tc.token)
			}
			recorder := httptest.NewRecorder()
			router.ServeHTTP(recorder, request)
			assertProcurementConflictBusinessStatus(t, recorder, tc.status)
			if service.queryCalls != 0 || service.resolveCalls != 0 {
				t.Fatalf("unauthorized request reached service: queries=%d resolves=%d", service.queryCalls, service.resolveCalls)
			}
			if service.conflicts[0] != before {
				t.Fatalf("unauthorized request changed conflict: before=%#v after=%#v", before, service.conflicts[0])
			}
		})
	}

	queryRequest := httptest.NewRequest(http.MethodGet, queryPath, nil)
	queryRequest.Header.Set("Authorization", "Bearer "+integrationToken)
	queryRecorder := httptest.NewRecorder()
	router.ServeHTTP(queryRecorder, queryRequest)
	assertProcurementConflictBusinessStatus(t, queryRecorder, response.CodeOK)
	if service.queryCalls != 1 || service.lastEventID != "event-17" {
		t.Fatalf("authorized query not dispatched correctly: calls=%d eventID=%q", service.queryCalls, service.lastEventID)
	}

	journalBefore, err := json.Marshal(service.journal)
	if err != nil {
		t.Fatal(err)
	}
	resolveRequest := httptest.NewRequest(http.MethodPost, resolvePath, strings.NewReader(`{"admin_id":999,"note":"confirmed against source evidence"}`))
	resolveRequest.Header.Set("Content-Type", "application/json")
	resolveRequest.Header.Set("Authorization", "Bearer "+integrationToken)
	resolveRecorder := httptest.NewRecorder()
	router.ServeHTTP(resolveRecorder, resolveRequest)
	assertProcurementConflictBusinessStatus(t, resolveRecorder, response.CodeOK)
	if service.resolveCalls != 1 || service.lastResolvedAdminID != integrationAdminID {
		t.Fatalf("resolution identity must come from authenticated context: calls=%d adminID=%d want=%d", service.resolveCalls, service.lastResolvedAdminID, integrationAdminID)
	}
	if got := service.conflicts[0]; got.Status != "resolved" || got.ResolvedByAdminID == nil || *got.ResolvedByAdminID != integrationAdminID || got.ResolutionNote != "confirmed against source evidence" {
		t.Fatalf("authorized resolution not recorded as expected: %#v", got)
	}
	journalAfter, err := json.Marshal(service.journal)
	if err != nil {
		t.Fatal(err)
	}
	if string(journalAfter) != string(journalBefore) {
		t.Fatalf("conflict resolution changed original Journal: before=%s after=%s", journalBefore, journalAfter)
	}

	procurementRequest := httptest.NewRequest(http.MethodGet, "/api/v1/admin/procurement-orders", nil)
	procurementRequest.Header.Set("Authorization", "Bearer "+integrationToken)
	procurementRecorder := httptest.NewRecorder()
	router.ServeHTTP(procurementRecorder, procurementRequest)
	assertProcurementConflictBusinessStatus(t, procurementRecorder, response.CodeOK)
	if service.listCalls != 1 {
		t.Fatalf("existing procurement admin route was not preserved: List calls=%d", service.listCalls)
	}
}

func setupProcurementConflictRouteAccessTest(t *testing.T) (admincontract.Store, *authz.Service, string, string, uint) {
	t.Helper()
	gin.SetMode(gin.TestMode)
	dsn := fmt.Sprintf("file:procurement_conflict_route_%d?mode=memory&cache=shared", time.Now().UnixNano())
	db, err := gorm.Open(sqlite.Open(dsn), &gorm.Config{})
	if err != nil {
		t.Fatalf("open SQLite: %v", err)
	}
	sqlDB, err := db.DB()
	if err != nil {
		t.Fatalf("get SQLite connection: %v", err)
	}
	sqlDB.SetMaxOpenConns(1)
	t.Cleanup(func() { _ = sqlDB.Close() })
	if err := db.AutoMigrate(&admindomain.Admin{}); err != nil {
		t.Fatalf("migrate admins: %v", err)
	}
	adminRepo := adminstore.New(db)
	noPermission := &admindomain.Admin{Username: "procurement-conflict-none", PasswordHash: "test-hash"}
	integration := &admindomain.Admin{Username: "procurement-conflict-integration", PasswordHash: "test-hash"}
	for _, admin := range []*admindomain.Admin{noPermission, integration} {
		if err := adminRepo.Create(admin); err != nil {
			t.Fatalf("create admin: %v", err)
		}
	}
	authzService, err := authz.NewService(db)
	if err != nil {
		t.Fatalf("create authz service: %v", err)
	}
	if err := authzService.BootstrapBuiltinRoles(); err != nil {
		t.Fatalf("bootstrap builtin roles: %v", err)
	}
	if err := authzService.SetAdminRoles(integration.ID, []string{"integration"}); err != nil {
		t.Fatalf("assign integration role: %v", err)
	}
	return adminRepo, authzService, signProcurementConflictAdminToken(t, noPermission), signProcurementConflictAdminToken(t, integration), integration.ID
}

func signProcurementConflictAdminToken(t *testing.T, admin *admindomain.Admin) string {
	t.Helper()
	now := time.Now()
	claims := adminauthapp.JWTClaims{
		AdminID: admin.ID, Username: admin.Username, TokenVersion: admin.TokenVersion, Typ: jwttoken.TypeAccess,
		RegisteredClaims: jwt.RegisteredClaims{ExpiresAt: jwt.NewNumericDate(now.Add(time.Hour)), IssuedAt: jwt.NewNumericDate(now), NotBefore: jwt.NewNumericDate(now.Add(-time.Second))},
	}
	token := jwt.NewWithClaims(jwt.SigningMethodHS256, claims)
	signed, err := token.SignedString([]byte(procurementConflictJWTSecret))
	if err != nil {
		t.Fatalf("sign admin token: %v", err)
	}
	return signed
}

func signProcurementConflictUserToken(t *testing.T) string {
	t.Helper()
	claims := jwt.MapClaims{"user_id": 71, "typ": jwttoken.TypeAccess, "exp": time.Now().Add(time.Hour).Unix()}
	token := jwt.NewWithClaims(jwt.SigningMethodHS256, claims)
	signed, err := token.SignedString([]byte(procurementConflictJWTSecret))
	if err != nil {
		t.Fatalf("sign user token: %v", err)
	}
	return signed
}

func newProcurementConflictAccessRouter(adminRepo admincontract.Store, authzService *authz.Service, service procurementhttp.Service) *gin.Engine {
	router := gin.New()
	admin := router.Group("/api/v1/admin")
	admin.Use(middleware.JWTAuthMiddleware(procurementConflictJWTSecret, adminRepo), middleware.AdminRBACMiddleware(authzService))
	procurementhttp.RegisterAdminRoutes(admin, procurementhttp.NewAdminHandler(service))
	return router
}

func assertProcurementConflictBusinessStatus(t *testing.T, recorder *httptest.ResponseRecorder, want int) {
	t.Helper()
	var got struct {
		StatusCode int `json:"status_code"`
	}
	if err := json.Unmarshal(recorder.Body.Bytes(), &got); err != nil {
		t.Fatalf("decode response: %v body=%s", err, recorder.Body.String())
	}
	if got.StatusCode != want {
		t.Fatalf("business status want %d got %d body=%s", want, got.StatusCode, recorder.Body.String())
	}
}

type procurementConflictHTTPService struct {
	conflicts           []procurementdomain.TradeEvidenceConflict
	journal             []procurementdomain.TradeResponseJournalEntry
	queryCalls          int
	resolveCalls        int
	listCalls           int
	lastEventID         string
	lastResolvedAdminID uint
}

func (s *procurementConflictHTTPService) List(procurementcontract.ListFilter) ([]procurementdomain.Order, int64, error) {
	s.listCalls++
	return nil, 0, nil
}

func (*procurementConflictHTTPService) StatsByStatus(procurementcontract.ListFilter) (map[string]int64, error) {
	return map[string]int64{}, nil
}

func (*procurementConflictHTTPService) GetByID(uint) (*procurementdomain.Order, error) {
	return nil, nil
}
func (*procurementConflictHTTPService) FillParentOrderNo(*procurementdomain.Order) {}
func (*procurementConflictHTTPService) RetryManual(uint, ...procurementcontract.ManualResolution) error {
	return nil
}

func (s *procurementConflictHTTPService) ListTradeResponseJournal(string) ([]procurementdomain.TradeResponseJournalEntry, error) {
	return append([]procurementdomain.TradeResponseJournalEntry(nil), s.journal...), nil
}

func (*procurementConflictHTTPService) RecordTradeResponseReconciliation(uint, string, string, string, string, uint) error {
	return nil
}

func (s *procurementConflictHTTPService) ListTradeEvidenceConflicts(eventID string) ([]procurementdomain.TradeEvidenceConflict, error) {
	s.queryCalls++
	s.lastEventID = eventID
	return append([]procurementdomain.TradeEvidenceConflict(nil), s.conflicts...), nil
}

func (s *procurementConflictHTTPService) ResolveTradeEvidenceConflict(id, adminID uint, note string) error {
	s.resolveCalls++
	s.lastResolvedAdminID = adminID
	for i := range s.conflicts {
		if s.conflicts[i].ID == id {
			resolvedBy := adminID
			now := time.Now().UTC()
			s.conflicts[i].Status = "resolved"
			s.conflicts[i].ResolvedByAdminID = &resolvedBy
			s.conflicts[i].ResolvedAt = &now
			s.conflicts[i].ResolutionNote = note
		}
	}
	return nil
}

func (*procurementConflictHTTPService) CancelManual(uint) error { return nil }

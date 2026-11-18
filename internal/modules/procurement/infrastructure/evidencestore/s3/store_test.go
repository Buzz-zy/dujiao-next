package s3

import (
	"bytes"
	"context"
	"encoding/json"
	"encoding/xml"
	"errors"
	"fmt"
	"io"
	"net/http"
	"net/http/httptest"
	"strings"
	"sync"
	"testing"
	"time"

	"github.com/dujiao-next/internal/modules/procurement/contract"
)

type mockS3 struct {
	mu              sync.Mutex
	objects         map[string][]byte
	conditional     bool
	putCount        int
	getCount        int
	listPageSize    int
	corruptGet      bool
	failGet         bool
	putResponseLost bool
	unavailable     bool
	putDelay        time.Duration
}

func newMockS3() *mockS3 {
	return &mockS3{objects: make(map[string][]byte), conditional: true, listPageSize: 1000}
}

func (m *mockS3) ServeHTTP(w http.ResponseWriter, r *http.Request) {
	m.mu.Lock()
	defer m.mu.Unlock()
	if m.unavailable {
		http.Error(w, "unavailable", http.StatusServiceUnavailable)
		return
	}
	key := strings.TrimPrefix(r.URL.Path, "/bucket/")
	if r.Method == http.MethodGet && r.URL.Query().Get("list-type") == "2" {
		keys := make([]string, 0, len(m.objects))
		for k := range m.objects {
			if strings.HasPrefix(k, "events/v1/") {
				keys = append(keys, k)
			}
		}
		for i := 0; i < len(keys); i++ {
			for j := i + 1; j < len(keys); j++ {
				if keys[j] < keys[i] {
					keys[i], keys[j] = keys[j], keys[i]
				}
			}
		}
		start := 0
		if token := r.URL.Query().Get("continuation-token"); token != "" {
			_, _ = fmt.Sscanf(token, "%d", &start)
		}
		end := start + m.listPageSize
		if end > len(keys) {
			end = len(keys)
		}
		type item struct {
			Key string `xml:"Key"`
		}
		result := struct {
			XMLName     xml.Name `xml:"ListBucketResult"`
			IsTruncated bool     `xml:"IsTruncated"`
			Next        string   `xml:"NextContinuationToken,omitempty"`
			Contents    []item   `xml:"Contents"`
		}{IsTruncated: end < len(keys)}
		for _, k := range keys[start:end] {
			result.Contents = append(result.Contents, item{Key: k})
		}
		if result.IsTruncated {
			result.Next = fmt.Sprint(end)
		}
		w.Header().Set("Content-Type", "application/xml")
		_ = xml.NewEncoder(w).Encode(result)
		return
	}
	switch r.Method {
	case http.MethodPut:
		m.putCount++
		body, _ := io.ReadAll(r.Body)
		if r.Header.Get("If-None-Match") == "*" && m.conditional {
			if _, ok := m.objects[key]; ok {
				w.WriteHeader(http.StatusPreconditionFailed)
				return
			}
		} else if r.Header.Get("If-None-Match") == "*" && !m.conditional {
			// Simulate a service that silently ignores the required condition.
		}
		m.objects[key] = body
		if m.putDelay > 0 {
			time.Sleep(m.putDelay)
		}
		if m.putResponseLost {
			hijacker, ok := w.(http.Hijacker)
			if ok {
				conn, _, err := hijacker.Hijack()
				if err == nil {
					_ = conn.Close()
					return
				}
			}
		}
		w.WriteHeader(http.StatusOK)
	case http.MethodGet:
		m.getCount++
		if m.failGet {
			http.Error(w, "read failed", http.StatusInternalServerError)
			return
		}
		body, ok := m.objects[key]
		if !ok {
			http.NotFound(w, r)
			return
		}
		body = append([]byte(nil), body...)
		if m.corruptGet && len(body) > 0 {
			body[len(body)-1] ^= 1
		}
		w.Write(body)
	default:
		if r.URL.Query().Get("list-type") == "2" {
			keys := make([]string, 0, len(m.objects))
			for k := range m.objects {
				if strings.HasPrefix(k, "events/v1/") {
					keys = append(keys, k)
				}
			}
			// Tests use stable pages; keys are sorted by the handler.
			for i := 0; i < len(keys); i++ {
				for j := i + 1; j < len(keys); j++ {
					if keys[j] < keys[i] {
						keys[i], keys[j] = keys[j], keys[i]
					}
				}
			}
			start := 0
			if token := r.URL.Query().Get("continuation-token"); token != "" {
				_, _ = fmt.Sscanf(token, "%d", &start)
			}
			end := start + m.listPageSize
			if end > len(keys) {
				end = len(keys)
			}
			type item struct {
				Key string `xml:"Key"`
			}
			result := struct {
				XMLName     xml.Name `xml:"ListBucketResult"`
				IsTruncated bool     `xml:"IsTruncated"`
				Next        string   `xml:"NextContinuationToken,omitempty"`
				Contents    []item   `xml:"Contents"`
			}{IsTruncated: end < len(keys)}
			for _, k := range keys[start:end] {
				result.Contents = append(result.Contents, item{Key: k})
			}
			if result.IsTruncated {
				result.Next = fmt.Sprint(end)
			}
			w.Header().Set("Content-Type", "application/xml")
			_ = xml.NewEncoder(w).Encode(result)
			return
		}
		http.NotFound(w, r)
	}
}

func s3TestConfig(endpoint string) Config {
	return Config{Endpoint: endpoint, Bucket: "bucket", Region: "us-east-1", AddressingStyle: "path", Timeout: 2 * time.Second}
}

func s3TestEvent(id, trace string) contract.EvidenceEvent {
	return contract.EvidenceEvent{EventID: id, ProcurementOrderID: 42, TraceID: trace, EventType: "trade_response", TradeNo: "trade-42", Amount: "12.30", ResponseAt: time.Date(2026, 11, 17, 1, 2, 3, 0, time.UTC), SchemaVersion: 1, Supplier: "faka", RelatedEventID: "intent-42"}
}

func newTestStore(t *testing.T, mock *mockS3) (*Store, *httptest.Server) {
	t.Helper()
	t.Setenv("AWS_ACCESS_KEY_ID", "test-access")
	t.Setenv("AWS_SECRET_ACCESS_KEY", "test-secret")
	server := httptest.NewServer(mock)
	store, err := New(context.Background(), s3TestConfig(server.URL))
	if err != nil {
		server.Close()
		t.Fatal(err)
	}
	t.Cleanup(func() { _ = store.Close(); server.Close() })
	return store, server
}

func TestConditionalCreateIsProbedAndRepeatedEventIsIdempotent(t *testing.T) {
	mock := newMockS3()
	store, _ := newTestStore(t, mock)
	event := s3TestEvent("event/one", "trace-1")
	if err := store.AppendResponse(event); err != nil {
		t.Fatal(err)
	}
	if err := store.AppendResponse(event); err != nil {
		t.Fatalf("identical retry: %v", err)
	}
	if mock.putCount < 3 {
		t.Fatalf("put count=%d; want probe twice plus event", mock.putCount)
	}
	changed := event
	changed.Amount = "99.00"
	if err := store.AppendResponse(changed); !errors.Is(err, ErrEventConflict) {
		t.Fatalf("conflict error=%v", err)
	}
	original, _ := json.Marshal(event)
	mock.mu.Lock()
	stored := append([]byte(nil), mock.objects[eventKey(event.EventID)]...)
	mock.mu.Unlock()
	if !bytes.Equal(stored, original) {
		t.Fatalf("conflict overwrote original evidence: %s", stored)
	}
}

func TestConstructorRejectsIgnoredConditionalCreate(t *testing.T) {
	t.Setenv("AWS_ACCESS_KEY_ID", "test-access")
	t.Setenv("AWS_SECRET_ACCESS_KEY", "test-secret")
	mock := newMockS3()
	mock.conditional = false
	server := httptest.NewServer(mock)
	defer server.Close()
	if _, err := New(context.Background(), s3TestConfig(server.URL)); !errors.Is(err, ErrConditionalWriteUnsupported) {
		t.Fatalf("New error=%v", err)
	}
}

func TestAppendReadsBackAndVerifiesPayload(t *testing.T) {
	mock := newMockS3()
	store, _ := newTestStore(t, mock)
	event := s3TestEvent("event-1", "trace-1")
	mock.mu.Lock()
	mock.corruptGet = true
	mock.mu.Unlock()
	if err := store.AppendResponse(event); !errors.Is(err, ErrIntegrity) {
		t.Fatalf("append error=%v", err)
	}
}

func TestAppendFailsWhenReadBackIsUnavailable(t *testing.T) {
	mock := newMockS3()
	store, _ := newTestStore(t, mock)
	mock.mu.Lock()
	mock.failGet = true
	mock.mu.Unlock()
	if err := store.AppendResponse(s3TestEvent("event-no-readback", "trace-no-readback")); err == nil {
		t.Fatal("successful Put without a readable verification response must fail")
	}
}

func TestTraceLookupFollowsPagination(t *testing.T) {
	mock := newMockS3()
	mock.listPageSize = 1
	store, _ := newTestStore(t, mock)
	for _, event := range []contract.EvidenceEvent{s3TestEvent("a", "same"), s3TestEvent("b", "other"), s3TestEvent("c", "same")} {
		if err := store.AppendResponse(event); err != nil {
			t.Fatal(err)
		}
	}
	got, err := store.GetByTraceID("same")
	if err != nil || len(got) != 2 {
		t.Fatalf("events=%d err=%v", len(got), err)
	}
}

func TestAppendResolvesLostPutResponseBySameEventID(t *testing.T) {
	mock := newMockS3()
	store, _ := newTestStore(t, mock)
	event := s3TestEvent("event-lost", "trace-lost")
	mock.mu.Lock()
	mock.putResponseLost = true
	mock.mu.Unlock()
	if err := store.AppendResponse(event); err != nil {
		t.Fatalf("reconcile stored event after lost response: %v", err)
	}
}

func TestGetByEventIDReturnsPersistedEvent(t *testing.T) {
	mock := newMockS3()
	store, _ := newTestStore(t, mock)
	event := s3TestEvent("event-read", "trace-read")
	if err := store.AppendResponse(event); err != nil {
		t.Fatal(err)
	}
	got, err := store.GetByEventID(event.EventID)
	if err != nil || got == nil || got.EventID != event.EventID || got.Amount != event.Amount {
		t.Fatalf("event=%#v err=%v", got, err)
	}
	missing, err := store.GetByEventID("not-found")
	if err != nil || missing != nil {
		t.Fatalf("missing=%#v err=%v", missing, err)
	}
}

func TestTwoStoresConcurrentlyAppendSameEvent(t *testing.T) {
	mock := newMockS3()
	t.Setenv("AWS_ACCESS_KEY_ID", "test-access")
	t.Setenv("AWS_SECRET_ACCESS_KEY", "test-secret")
	server := httptest.NewServer(mock)
	defer server.Close()
	first, err := New(context.Background(), s3TestConfig(server.URL))
	if err != nil {
		t.Fatal(err)
	}
	defer first.Close()
	second, err := New(context.Background(), s3TestConfig(server.URL))
	if err != nil {
		t.Fatal(err)
	}
	defer second.Close()
	event := s3TestEvent("shared-event", "shared-trace")
	var wg sync.WaitGroup
	errs := make(chan error, 2)
	for _, store := range []*Store{first, second} {
		wg.Add(1)
		go func(store *Store) { defer wg.Done(); errs <- store.AppendResponse(event) }(store)
	}
	wg.Wait()
	close(errs)
	for err := range errs {
		if err != nil {
			t.Fatalf("concurrent append: %v", err)
		}
	}
}

func TestTimeoutOutcomeCanBeReconciledWithSameEventID(t *testing.T) {
	t.Setenv("AWS_ACCESS_KEY_ID", "test-access")
	t.Setenv("AWS_SECRET_ACCESS_KEY", "test-secret")
	mock := newMockS3()
	server := httptest.NewServer(mock)
	defer server.Close()
	cfg := s3TestConfig(server.URL)
	cfg.Timeout = 40 * time.Millisecond
	store, err := New(context.Background(), cfg)
	if err != nil {
		t.Fatal(err)
	}
	defer store.Close()
	event := s3TestEvent("timeout-event", "timeout-trace")
	mock.mu.Lock()
	mock.putDelay = 100 * time.Millisecond
	mock.mu.Unlock()
	err = store.AppendResponse(event)
	if !errors.Is(err, ErrOutcomeUnknown) {
		t.Fatalf("slow S3 write error=%v, want uncertain outcome", err)
	}
	mock.mu.Lock()
	mock.putDelay = 0
	mock.mu.Unlock()
	if err := store.AppendResponse(event); err != nil {
		t.Fatalf("same EventID reconciliation failed: %v", err)
	}
}

func TestConfirmerCopiesOnlyExistingLocalEvent(t *testing.T) {
	mock := newMockS3()
	remote, _ := newTestStore(t, mock)
	local := &oneEventStore{event: s3TestEvent("local-event", "trace-local")}
	confirmer, err := NewConfirmer(local, remote)
	if err != nil {
		t.Fatal(err)
	}
	if err := confirmer.ConfirmIndependent(local.event.EventID); err != nil {
		t.Fatal(err)
	}
	got, err := remote.GetByEventID(local.event.EventID)
	if err != nil || got == nil || got.TradeNo != local.event.TradeNo {
		t.Fatalf("event=%#v err=%v", got, err)
	}
	if err := confirmer.ConfirmIndependent("missing-event"); err == nil {
		t.Fatal("missing local event must not be generated or uploaded")
	}
}

type oneEventStore struct{ event contract.EvidenceEvent }

func (s *oneEventStore) AppendIntent(contract.EvidenceEvent) error {
	return errors.New("not implemented")
}
func (s *oneEventStore) AppendResponse(contract.EvidenceEvent) error {
	return errors.New("not implemented")
}
func (s *oneEventStore) GetByEventID(id string) (*contract.EvidenceEvent, error) {
	if id != s.event.EventID {
		return nil, nil
	}
	e := s.event
	return &e, nil
}
func (s *oneEventStore) GetByTraceID(string) ([]contract.EvidenceEvent, error) {
	return []contract.EvidenceEvent{s.event}, nil
}
func (s *oneEventStore) ListPending() ([]contract.EvidenceEvent, error) {
	return []contract.EvidenceEvent{s.event}, nil
}

func TestAppendRejectsRemoteOutage(t *testing.T) {
	mock := newMockS3()
	store, _ := newTestStore(t, mock)
	mock.mu.Lock()
	mock.unavailable = true
	mock.mu.Unlock()
	if err := store.AppendResponse(s3TestEvent("event-down", "trace-down")); err == nil {
		t.Fatal("expected S3 outage error")
	}
}

func TestEventJSONShapeIsStable(t *testing.T) {
	b, err := json.Marshal(s3TestEvent("event-1", "trace-1"))
	if err != nil || !strings.Contains(string(b), `"event_id":"event-1"`) {
		t.Fatalf("json=%s err=%v", b, err)
	}
}

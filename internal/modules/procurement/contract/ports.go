package contract

import (
	"crypto/sha256"
	"encoding/hex"
	"encoding/json"
	"fmt"
	"strings"
	"time"

	procurementdomain "github.com/dujiao-next/internal/modules/procurement/domain"
)

type Repository interface {
	GetByID(id uint) (*procurementdomain.Order, error)
	GetByLocalOrderID(localOrderID uint) (*procurementdomain.Order, error)
	GetByLocalOrderNo(localOrderNo string) (*procurementdomain.Order, error)
	Create(order *procurementdomain.Order) error
	// UpdateStatus 仅当当前状态仍为 fromStatus 时写入（比较后再写），返回是否写入；
	// 回调、轮询、提交可能并发处理同一采购单，未写入说明已被其它路径推进。
	UpdateStatus(id uint, fromStatus, toStatus string, updates map[string]interface{}) (bool, error)
	List(filter ListFilter) ([]procurementdomain.Order, int64, error)
	StatsByStatus(filter ListFilter) (map[string]int64, error)
	ListByConnectionAndTimeRange(connectionID uint, start, end time.Time) ([]procurementdomain.Order, error)
}

type OrderRepository interface {
	GetByID(id uint) (*procurementdomain.LocalOrder, error)
	GetByIDs(ids []uint) ([]procurementdomain.LocalOrder, error)
	UpdateStatus(id uint, status string, updates map[string]interface{}) error
}

type ProcurementMapping struct {
	ID                  uint
	ConnectionID        uint
	UpstreamProductID   uint
	UpstreamProductCode string
	IsActive            bool
	UpstreamStatus      string
}

type ProcurementSKUMapping struct {
	ProductMappingID uint
	UpstreamSKUID    uint
	IsActive         bool
}

func ValidateProcurementMapping(mapping *ProcurementMapping, sku *ProcurementSKUMapping, productID, skuID, expectedConnectionID uint, requireProductCode bool) error {
	if mapping == nil {
		return fmt.Errorf("%w: product %d has no mapping", ErrMappingInvalid, productID)
	}
	if !mapping.IsActive {
		return fmt.Errorf("%w: product mapping %d is paused", ErrMappingInvalid, mapping.ID)
	}
	if mapping.UpstreamStatus != "active" {
		return fmt.Errorf("%w: product mapping %d upstream status is %q", ErrMappingInvalid, mapping.ID, mapping.UpstreamStatus)
	}
	if mapping.ID == 0 || mapping.ConnectionID == 0 || mapping.UpstreamProductID == 0 {
		return fmt.Errorf("%w: product mapping %d has incomplete numeric identity", ErrMappingInvalid, mapping.ID)
	}
	if expectedConnectionID != 0 && mapping.ConnectionID != expectedConnectionID {
		return fmt.Errorf("%w: procurement connection %d differs from mapped connection %d", ErrMappingInvalid, expectedConnectionID, mapping.ConnectionID)
	}
	if requireProductCode && strings.TrimSpace(mapping.UpstreamProductCode) == "" {
		return fmt.Errorf("%w: product mapping %d has no persisted upstream code", ErrMappingInvalid, mapping.ID)
	}
	if sku == nil {
		return fmt.Errorf("%w: local SKU %d has no upstream mapping", ErrMappingInvalid, skuID)
	}
	if sku.ProductMappingID != mapping.ID {
		return fmt.Errorf("%w: SKU mapping for local SKU %d belongs to product mapping %d, expected %d", ErrMappingInvalid, skuID, sku.ProductMappingID, mapping.ID)
	}
	if !sku.IsActive || sku.UpstreamSKUID == 0 {
		return fmt.Errorf("%w: SKU mapping for local SKU %d is inactive or incomplete", ErrMappingInvalid, skuID)
	}
	return nil
}

type ProductMappingReader interface {
	FindMappingForProduct(productID uint) (*ProcurementMapping, error)
}

type SKUMappingReader interface {
	FindMappingForSKU(skuID uint) (*ProcurementSKUMapping, error)
	FindUpstreamSKUID(skuID uint) (upstreamSKUID uint, found bool, err error)
}

type ConnectionProvider interface {
	Open(connectionID uint) (UpstreamConnection, error)
}

type Enqueuer interface {
	EnqueueSubmit(procurementOrderID uint, delay time.Duration) error
	EnqueuePoll(procurementOrderID uint, delay time.Duration) error
}

type OrderLifecycle interface {
	CreateUpstreamFulfillment(orderID uint, fulfillment *Fulfillment, now time.Time) error
	SyncParentStatus(parentID uint, now time.Time) (string, error)
	EnqueueStatusEmail(orderID uint, status string) (skipped bool, err error)
}

type DownstreamCallbackEnqueuer interface {
	EnqueueCallback(orderID uint)
}

type BotFulfillmentNotifier interface {
	NotifyBotOrderFulfilled(userID, orderID uint)
}

type FailureNotifier interface {
	NotifyFailure(order *procurementdomain.Order, message string) error
}

type ManualDecisionAudit interface {
	RecordProcurementDecision(operatorAdminID, procurementOrderID uint, traceID, action, reason, tradeNo string) error
	ResolveProcurementDecision(orderID uint, traceID, action, reason, tradeNo string, operatorAdminID uint, toStatus string, updates map[string]interface{}) (bool, error)
	AppendTradeResponseJournal(entry procurementdomain.TradeResponseJournalEntry) error
	ListTradeResponseJournalByTraceID(traceID string) ([]procurementdomain.TradeResponseJournalEntry, error)
	RecordTradeResponseReconciliation(orderID uint, traceID, confirmedTradeNo, confirmedAmount, note string, operatorAdminID uint, respondedAt time.Time) error
	TradeEvidenceConflictStore
}

// EvidenceEvent is a minimal durable record for one Trade intent or response.
type EvidenceEvent struct {
	EventID            string    `json:"event_id"`
	ProcurementOrderID uint      `json:"procurement_order_id"`
	TraceID            string    `json:"trace_id"`
	EventType          string    `json:"event_type"`
	TradeNo            string    `json:"trade_no,omitempty"`
	Amount             string    `json:"amount,omitempty"`
	ResponseAt         time.Time `json:"response_at,omitempty"`
	SchemaVersion      int       `json:"schema_version"`
	Supplier           string    `json:"supplier"`
	RelatedEventID     string    `json:"related_event_id,omitempty"`
}

// EvidenceStore only exposes append and read operations; it has no mutation or delete API.
type EvidenceStore interface {
	AppendIntent(event EvidenceEvent) error
	AppendResponse(event EvidenceEvent) error
	GetByEventID(eventID string) (*EvidenceEvent, error)
	GetByTraceID(traceID string) ([]EvidenceEvent, error)
	ListPending() ([]EvidenceEvent, error)
}

// IndependentEvidenceSource identifies a store whose events have been confirmed outside the SQL database.
type IndependentEvidenceSource interface {
	EvidenceStore
	IndependentlyConfirmed() bool
}

// TradeResponseJournalEvidenceImporter imports complete response evidence by stable EventID.
type TradeResponseJournalEvidenceImporter interface {
	ImportTradeResponseEvidence(event EvidenceEvent) error
}

type TradeEvidenceConflictStore interface {
	RecordTradeEvidenceConflict(conflict procurementdomain.TradeEvidenceConflict) error
	GetTradeEvidenceConflictsByEventID(eventID string) ([]procurementdomain.TradeEvidenceConflict, error)
	ResolveTradeEvidenceConflict(id uint, adminID uint, note string) error
}

// TradeResponseJournalEntryFromEvidence validates and maps a complete immutable response event.
func TradeResponseJournalEntryFromEvidence(event EvidenceEvent) (procurementdomain.TradeResponseJournalEntry, error) {
	if event.EventType != "trade_response" || event.EventID == "" || len(event.EventID) > 64 || event.ProcurementOrderID == 0 || strings.TrimSpace(event.TraceID) == "" || strings.TrimSpace(event.TradeNo) == "" || strings.TrimSpace(event.Amount) == "" || event.ResponseAt.IsZero() || event.SchemaVersion != 1 || strings.TrimSpace(event.Supplier) == "" || strings.TrimSpace(event.RelatedEventID) == "" {
		return procurementdomain.TradeResponseJournalEntry{}, ErrInvalidTradeResponseEvidence
	}
	payload, err := json.Marshal(event)
	if err != nil {
		return procurementdomain.TradeResponseJournalEntry{}, err
	}
	hash := sha256.Sum256(payload)
	eventID, fingerprint := event.EventID, hex.EncodeToString(hash[:])
	return procurementdomain.TradeResponseJournalEntry{
		SourceEventID: &eventID, SourceEventHash: &fingerprint,
		ProcurementOrderID: event.ProcurementOrderID, TraceID: event.TraceID,
		UpstreamTradeNo: event.TradeNo, Amount: event.Amount,
		ResponseTime: event.ResponseAt, CreatedAt: event.ResponseAt,
		EventType: "trade_success_response",
	}, nil
}

// IndependentEvidenceConfirmer proves that an event is durable outside the main SQL database.
// A local spool intentionally does not implement this capability.
type IndependentEvidenceConfirmer interface {
	ConfirmIndependent(eventID string) error
}

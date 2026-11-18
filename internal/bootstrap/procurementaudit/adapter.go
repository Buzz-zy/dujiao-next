package procurementaudit

import (
	"crypto/sha256"
	"encoding/hex"
	"fmt"
	"time"

	"github.com/dujiao-next/internal/constants"
	auditlogapp "github.com/dujiao-next/internal/modules/auditlog/application"
	auditlogstore "github.com/dujiao-next/internal/modules/auditlog/infrastructure/gormstore"
	procurementcontract "github.com/dujiao-next/internal/modules/procurement/contract"
	procurementdomain "github.com/dujiao-next/internal/modules/procurement/domain"
	"github.com/dujiao-next/internal/shared/jsonmap"
	"gorm.io/gorm"
	"gorm.io/gorm/clause"
)

type Adapter struct {
	db      *gorm.DB
	service *auditlogapp.AuthzService
}

var _ procurementcontract.ManualDecisionAudit = (*Adapter)(nil)
var _ procurementcontract.TradeResponseJournalEvidenceImporter = (*Adapter)(nil)
var _ procurementcontract.TradeEvidenceConflictStore = (*Adapter)(nil)

func New(db *gorm.DB) *Adapter {
	return &Adapter{db: db, service: auditlogapp.NewAuthzService(auditlogstore.NewAuthzStore(db))}
}

func (a *Adapter) RecordProcurementDecision(operatorAdminID, procurementOrderID uint, traceID, action, reason, tradeNo string) error {
	if a == nil || a.service == nil {
		return fmt.Errorf("procurement decision audit service is unavailable")
	}
	return a.service.Record(auditlogapp.AuthzRecord{
		OperatorAdminID: operatorAdminID,
		Action:          action,
		Object:          "procurement_orders/" + fmt.Sprint(procurementOrderID),
		Method:          "POST",
		RequestID:       traceID,
		Detail:          jsonmap.JSON{"reason": reason, "trade_no": tradeNo, "decision": action},
	})
}

func (a *Adapter) AppendTradeResponseJournal(entry procurementdomain.TradeResponseJournalEntry) error {
	if a == nil || a.db == nil || entry.ProcurementOrderID == 0 || entry.TraceID == "" || entry.EventType == "" {
		return fmt.Errorf("procurement Trade response journal is unavailable or invalid")
	}
	if entry.CreatedAt.IsZero() {
		entry.CreatedAt = time.Now()
	}
	if entry.ResponseTime.IsZero() {
		entry.ResponseTime = entry.CreatedAt
	}
	if entry.SourceEventID != nil || entry.SourceEventHash != nil {
		return a.appendSourcedTradeResponse(entry)
	}
	if err := a.db.Create(&entry).Error; err != nil {
		return fmt.Errorf("append procurement Trade response journal: %w", err)
	}
	return nil
}

// ImportTradeResponseEvidence appends one confirmed S3 response event without updating an existing journal row.
func (a *Adapter) ImportTradeResponseEvidence(event procurementcontract.EvidenceEvent) error {
	if a == nil || a.db == nil {
		return fmt.Errorf("procurement Trade response journal is unavailable")
	}
	entry, err := procurementcontract.TradeResponseJournalEntryFromEvidence(event)
	if err != nil {
		return err
	}
	return a.appendSourcedTradeResponse(entry)
}

func (a *Adapter) RecordTradeEvidenceConflict(conflict procurementdomain.TradeEvidenceConflict) error {
	if a == nil || a.db == nil || conflict.EventID == "" || conflict.TraceID == "" || conflict.ConflictType == "" || conflict.IncomingEventHash == "" {
		return fmt.Errorf("trade evidence conflict is unavailable or invalid")
	}
	keyInput := conflict.EventID + "\x00" + conflict.ConflictType + "\x00" + conflict.ExistingEventHash + "\x00" + conflict.IncomingEventHash
	keyHash := sha256.Sum256([]byte(keyInput))
	conflict.ConflictKey = hex.EncodeToString(keyHash[:])
	now := time.Now().UTC()
	if conflict.FirstSeenAt.IsZero() {
		conflict.FirstSeenAt = now
	}
	conflict.LastSeenAt = now
	if conflict.Status == "" {
		conflict.Status = "open"
	}
	result := a.db.Clauses(clause.OnConflict{Columns: []clause.Column{{Name: "conflict_key"}}, DoUpdates: clause.Assignments(map[string]interface{}{"last_seen_at": now})}).Create(&conflict)
	if result.Error != nil {
		return fmt.Errorf("persist trade evidence conflict %q: %w", conflict.EventID, result.Error)
	}
	return nil
}

func (a *Adapter) GetTradeEvidenceConflictsByEventID(eventID string) ([]procurementdomain.TradeEvidenceConflict, error) {
	if a == nil || a.db == nil || eventID == "" {
		return nil, fmt.Errorf("trade evidence conflict query is unavailable or invalid")
	}
	var rows []procurementdomain.TradeEvidenceConflict
	err := a.db.Where("event_id = ?", eventID).Order("first_seen_at ASC").Find(&rows).Error
	return rows, err
}

func (a *Adapter) ResolveTradeEvidenceConflict(id uint, adminID uint, note string) error {
	if a == nil || a.db == nil || id == 0 || adminID == 0 || note == "" {
		return fmt.Errorf("trade evidence conflict resolution is invalid")
	}
	now := time.Now().UTC()
	result := a.db.Model(&procurementdomain.TradeEvidenceConflict{}).Where("id = ? AND status = ?", id, "open").Updates(map[string]interface{}{"status": "resolved", "resolution_note": note, "resolved_by_admin_id": adminID, "resolved_at": now})
	if result.Error != nil {
		return result.Error
	}
	if result.RowsAffected == 0 {
		return fmt.Errorf("trade evidence conflict not found or already resolved")
	}
	return nil
}

func (a *Adapter) appendSourcedTradeResponse(entry procurementdomain.TradeResponseJournalEntry) error {
	if entry.SourceEventID == nil || entry.SourceEventHash == nil || *entry.SourceEventID == "" || *entry.SourceEventHash == "" {
		return procurementcontract.ErrInvalidTradeResponseEvidence
	}
	result := a.db.Clauses(clause.OnConflict{
		Columns:   []clause.Column{{Name: "source_event_id"}},
		DoNothing: true,
	}).Create(&entry)
	if result.Error != nil {
		return fmt.Errorf("import procurement Trade response journal event %q: %w", *entry.SourceEventID, result.Error)
	}
	if result.RowsAffected == 1 {
		return nil
	}
	var existing procurementdomain.TradeResponseJournalEntry
	if err := a.db.Where("source_event_id = ?", *entry.SourceEventID).First(&existing).Error; err != nil {
		return fmt.Errorf("read existing procurement Trade response journal event %q after duplicate insert: %w", *entry.SourceEventID, err)
	}
	if existing.SourceEventHash != nil && *existing.SourceEventHash == *entry.SourceEventHash && existing.ProcurementOrderID == entry.ProcurementOrderID && existing.TraceID == entry.TraceID && existing.UpstreamTradeNo == entry.UpstreamTradeNo && existing.Amount == entry.Amount && existing.ResponseTime.Equal(entry.ResponseTime) && existing.EventType == entry.EventType {
		return nil
	}
	conflict := procurementdomain.TradeEvidenceConflict{EventID: *entry.SourceEventID, ProcurementOrderID: entry.ProcurementOrderID, TraceID: entry.TraceID, ConflictType: "source_event_payload_mismatch", ExistingEventHash: valueOrEmpty(existing.SourceEventHash), IncomingEventHash: *entry.SourceEventHash, Status: "open"}
	if err := a.RecordTradeEvidenceConflict(conflict); err != nil {
		return fmt.Errorf("source EventID %q: %w; persist isolation record: %v", *entry.SourceEventID, procurementcontract.ErrEvidenceJournalConflict, err)
	}
	return fmt.Errorf("source EventID %q: %w", *entry.SourceEventID, procurementcontract.ErrEvidenceJournalConflict)
}

func valueOrEmpty(value *string) string {
	if value == nil {
		return ""
	}
	return *value
}

func (a *Adapter) ListTradeResponseJournalByTraceID(traceID string) ([]procurementdomain.TradeResponseJournalEntry, error) {
	if a == nil || a.db == nil || traceID == "" {
		return nil, fmt.Errorf("procurement Trade response journal is unavailable or invalid")
	}
	var entries []procurementdomain.TradeResponseJournalEntry
	if err := a.db.Where("trace_id = ?", traceID).Order("id ASC").Find(&entries).Error; err != nil {
		return nil, fmt.Errorf("query procurement Trade response journal: %w", err)
	}
	return entries, nil
}

func (a *Adapter) RecordTradeResponseReconciliation(orderID uint, traceID, confirmedTradeNo, confirmedAmount, note string, operatorAdminID uint, respondedAt time.Time) error {
	if operatorAdminID == 0 {
		return fmt.Errorf("procurement Trade response reconciliation operator is required")
	}
	return a.AppendTradeResponseJournal(procurementdomain.TradeResponseJournalEntry{
		ProcurementOrderID: orderID, TraceID: traceID, UpstreamTradeNo: confirmedTradeNo,
		Amount: confirmedAmount, ConfirmedTradeNo: confirmedTradeNo, ConfirmedAmount: confirmedAmount,
		OperatorAdminID: &operatorAdminID, ReconciliationNote: note, EventType: "manual_reconciliation_recorded", ResponseTime: respondedAt,
	})
}

// ResolveProcurementDecision commits the state CAS and audit entry together.
func (a *Adapter) ResolveProcurementDecision(orderID uint, traceID, action, reason, tradeNo string, operatorAdminID uint, toStatus string, updates map[string]interface{}) (bool, error) {
	if a == nil || a.db == nil || operatorAdminID == 0 {
		return false, fmt.Errorf("procurement decision audit database is unavailable")
	}
	updated := false
	err := a.db.Transaction(func(tx *gorm.DB) error {
		values := make(map[string]interface{}, len(updates)+1)
		for key, value := range updates {
			values[key] = value
		}
		values["status"] = toStatus
		query := tx.Model(&procurementdomain.Order{}).
			Where("id = ? AND deleted_at IS NULL AND status = ?", orderID, constants.ProcurementStatusAwaitingVerification)
		if toStatus == constants.ProcurementStatusPending {
			query = query.Where("(upstream_order_no IS NULL OR upstream_order_no = '')")
		}
		result := query.Updates(values)
		if result.Error != nil {
			return result.Error
		}
		if result.RowsAffected != 1 {
			return nil
		}
		detail := jsonmap.JSON{"reason": reason, "trade_no": tradeNo, "decision": action}
		if toStatus == constants.ProcurementStatusPending {
			detail["confirmed_no_charge"] = true
			detail["confirmed_no_order"] = true
		}
		audit := auditlogapp.NewAuthzService(auditlogstore.NewAuthzStore(tx))
		if err := audit.Record(auditlogapp.AuthzRecord{
			OperatorAdminID: operatorAdminID,
			Action:          action,
			Object:          "procurement_orders/" + fmt.Sprint(orderID),
			Method:          "POST",
			RequestID:       traceID,
			Detail:          detail,
		}); err != nil {
			return err
		}
		updated = true
		return nil
	})
	if err != nil {
		return false, err
	}
	return updated, nil
}

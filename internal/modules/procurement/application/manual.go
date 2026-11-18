package application

import (
	"context"
	"errors"
	"fmt"
	"strings"
	"time"

	"github.com/dujiao-next/internal/constants"
	"github.com/dujiao-next/internal/logger"
	procurementcontract "github.com/dujiao-next/internal/modules/procurement/contract"
	procurementdomain "github.com/dujiao-next/internal/modules/procurement/domain"
)

type TradeResponseJournalRecovery struct {
	source   procurementcontract.IndependentEvidenceSource
	importer procurementcontract.TradeResponseJournalEvidenceImporter
}

type JournalImportFailure struct {
	EventID string
	Err     error
}

type JournalImportReport struct {
	Scanned  int
	Imported int
	Skipped  int
	Failures []JournalImportFailure
}

func NewTradeResponseJournalRecovery(source procurementcontract.IndependentEvidenceSource, importer procurementcontract.TradeResponseJournalEvidenceImporter) (*TradeResponseJournalRecovery, error) {
	if source == nil || !source.IndependentlyConfirmed() || importer == nil {
		return nil, fmt.Errorf("independent evidence source and Journal importer are required")
	}
	return &TradeResponseJournalRecovery{source: source, importer: importer}, nil
}

func (r *TradeResponseJournalRecovery) ImportByEventID(eventID string) error {
	if strings.TrimSpace(eventID) == "" {
		return procurementcontract.ErrInvalidTradeResponseEvidence
	}
	event, err := r.source.GetByEventID(eventID)
	if err != nil {
		return fmt.Errorf("read independent evidence event %q: %w", eventID, err)
	}
	if event == nil {
		return fmt.Errorf("independent evidence event %q not found", eventID)
	}
	if event.EventType != "trade_response" {
		return procurementcontract.ErrInvalidTradeResponseEvidence
	}
	return r.importer.ImportTradeResponseEvidence(*event)
}

func (r *TradeResponseJournalRecovery) ScanPending() (JournalImportReport, error) {
	var report JournalImportReport
	events, err := r.source.ListPending()
	if err != nil {
		return report, fmt.Errorf("list independently confirmed Trade evidence: %w", err)
	}
	report.Scanned = len(events)
	failures := make([]error, 0)
	for _, event := range events {
		if event.EventType != "trade_response" {
			report.Skipped++
			continue
		}
		if err := r.importer.ImportTradeResponseEvidence(event); err != nil {
			failure := JournalImportFailure{EventID: event.EventID, Err: err}
			report.Failures = append(report.Failures, failure)
			failures = append(failures, fmt.Errorf("event %q: %w", event.EventID, err))
			continue
		}
		report.Imported++
	}
	return report, errors.Join(failures...)
}

func (s *Service) ListTradeResponseJournal(traceID string) ([]procurementdomain.TradeResponseJournalEntry, error) {
	if s.decisionAudit == nil {
		return nil, fmt.Errorf("procurement Trade response journal is unavailable")
	}
	return s.decisionAudit.ListTradeResponseJournalByTraceID(traceID)
}

func (s *Service) ListTradeEvidenceConflicts(eventID string) ([]procurementdomain.TradeEvidenceConflict, error) {
	if s.decisionAudit == nil {
		return nil, fmt.Errorf("trade evidence conflict store is unavailable")
	}
	return s.decisionAudit.GetTradeEvidenceConflictsByEventID(eventID)
}

func (s *Service) ResolveTradeEvidenceConflict(id, adminID uint, note string) error {
	if s.decisionAudit == nil {
		return fmt.Errorf("trade evidence conflict store is unavailable")
	}
	return s.decisionAudit.ResolveTradeEvidenceConflict(id, adminID, note)
}

func (s *Service) RecordTradeResponseReconciliation(orderID uint, traceID, confirmedTradeNo, confirmedAmount, note string, operatorAdminID uint) error {
	if orderID == 0 || strings.TrimSpace(traceID) == "" || strings.TrimSpace(note) == "" || operatorAdminID == 0 {
		return procurementcontract.ErrStatusInvalid
	}
	if s.decisionAudit == nil {
		return fmt.Errorf("procurement Trade response journal is unavailable")
	}
	order, err := s.procRepo.GetByID(orderID)
	if err != nil {
		return err
	}
	if order == nil {
		return procurementcontract.ErrNotFound
	}
	if order.TraceID != traceID || order.Status != constants.ProcurementStatusAwaitingVerification {
		return procurementcontract.ErrStatusInvalid
	}
	return s.decisionAudit.RecordTradeResponseReconciliation(orderID, traceID, confirmedTradeNo, confirmedAmount, note, operatorAdminID, time.Now())
}

func (s *Service) RetryManual(id uint, resolution ...procurementcontract.ManualResolution) error {
	procOrder, err := s.procRepo.GetByID(id)
	if err != nil {
		return fmt.Errorf("load procurement order: %w", err)
	}
	if procOrder == nil {
		return procurementcontract.ErrNotFound
	}
	if len(resolution) > 1 {
		return procurementcontract.ErrStatusInvalid
	}
	if len(resolution) == 1 {
		decision := resolution[0]
		decision.Reason, decision.TradeNo = strings.TrimSpace(decision.Reason), strings.TrimSpace(decision.TradeNo)
		if decision.OperatorAdminID == 0 || decision.Reason == "" || (decision.Mode != "order_found" && decision.Mode != "no_charge") || (decision.Mode == "order_found" && decision.TradeNo == "") {
			return procurementcontract.ErrStatusInvalid
		}
		if procOrder.Status != constants.ProcurementStatusAwaitingVerification {
			return procurementcontract.ErrStatusInvalid
		}
		if decision.Mode == "no_charge" && (!decision.ConfirmedNoCharge || !decision.ConfirmedNoOrder || strings.TrimSpace(procOrder.UpstreamOrderNo) != "") {
			return procurementcontract.ErrStatusInvalid
		}
		connection, openErr := s.connections.Open(procOrder.ConnectionID)
		if openErr != nil {
			return openErr
		}
		if connection == nil || connection.Protocol() != constants.ConnectionProtocolFaka {
			return procurementcontract.ErrStatusInvalid
		}
		if s.decisionAudit == nil {
			return fmt.Errorf("procurement decision audit is unavailable")
		}
		toStatus, action := constants.ProcurementStatusAccepted, "upstream_order_confirmed"
		updates := map[string]interface{}{"error_message": "", "retry_count": 0, "updated_at": time.Now()}
		if decision.Mode == "order_found" {
			updates["upstream_order_no"] = decision.TradeNo
		} else {
			toStatus, action = constants.ProcurementStatusAwaitingVerification, "no_charge_review_recorded_frozen"
			updates["error_message"] = "administrator recorded no-charge review; upstream result remains unprovable and procurement stays frozen"
		}
		updated, updateErr := s.decisionAudit.ResolveProcurementDecision(id, procOrder.TraceID, action, decision.Reason, decision.TradeNo, decision.OperatorAdminID, toStatus, updates)
		if updateErr != nil {
			return updateErr
		}
		if !updated {
			return procurementcontract.ErrStatusInvalid
		}
		return nil
	}

	if procOrder.Status != "failed" && procOrder.Status != "rejected" {
		return procurementcontract.ErrStatusInvalid
	}
	localOrder, err := s.orderRepo.GetByID(procOrder.LocalOrderID)
	if err != nil {
		return fmt.Errorf("load local order: %w", err)
	}
	if localOrder == nil || len(localOrder.Items) != 1 {
		err := fmt.Errorf("%w: local order %d is missing or has unsupported item count", procurementcontract.ErrMappingInvalid, procOrder.LocalOrderID)
		s.rejectProcurement(procOrder, err.Error())
		return err
	}
	item := localOrder.Items[0]
	mapping, err := s.mappingRepo.FindMappingForProduct(item.ProductID)
	if err != nil {
		return fmt.Errorf("read product mapping: %w", err)
	}
	skuMapping, err := s.skuMapRepo.FindMappingForSKU(item.SKUID)
	if err != nil {
		return fmt.Errorf("read SKU mapping: %w", err)
	}
	connection, err := s.connections.Open(procOrder.ConnectionID)
	if err != nil {
		return fmt.Errorf("open connection: %w", err)
	}
	if connection == nil {
		err := fmt.Errorf("%w: connection %d not found", procurementcontract.ErrMappingInvalid, procOrder.ConnectionID)
		s.rejectProcurement(procOrder, err.Error())
		return err
	}
	if err := procurementcontract.ValidateProcurementMapping(mapping, skuMapping, item.ProductID, item.SKUID, procOrder.ConnectionID, connection.Protocol() == constants.ConnectionProtocolFaka); err != nil {
		s.rejectProcurement(procOrder, err.Error())
		return err
	}

	now := time.Now()
	updates := map[string]interface{}{
		"retry_count":   0,
		"next_retry_at": nil,
		"error_message": "",
		"updated_at":    now,
	}
	reset, err := s.procRepo.UpdateStatus(procOrder.ID, procOrder.Status, constants.ProcurementStatusPending, updates)
	if err != nil {
		return fmt.Errorf("reset procurement status: %w", err)
	}
	if !reset {
		return procurementcontract.ErrStatusInvalid
	}

	logger.Infow("procurement_manual_retry",
		"procurement_order_id", procOrder.ID,
	)

	if s.queue != nil {
		return s.queue.EnqueueSubmit(procOrder.ID, 0)
	}
	return nil
}

// CancelManual 手动取消采购单
func (s *Service) CancelManual(id uint) error {
	procOrder, err := s.procRepo.GetByID(id)
	if err != nil {
		return fmt.Errorf("load procurement order: %w", err)
	}
	if procOrder == nil {
		return procurementcontract.ErrNotFound
	}

	switch procOrder.Status {
	case constants.ProcurementStatusPending, constants.ProcurementStatusFailed, constants.ProcurementStatusRejected:
		// 尚未在上游成单，可直接本地取消
	case constants.ProcurementStatusAccepted:
		// 上游已成单（通常已从上游钱包扣款）：必须上游取消成功才能标记取消，
		// 否则上游照常发货而本地已是终态，回调被丢弃、本地订单永久卡在 fulfilling。
		connection, err := s.connections.Open(procOrder.ConnectionID)
		if err != nil {
			return fmt.Errorf("open connection: %w", err)
		}
		if connection == nil {
			return procurementcontract.ErrConnectionNotFound
		}
		ctx, cancel := context.WithTimeout(context.Background(), 15*time.Second)
		defer cancel()
		if err := connection.CancelOrder(ctx, procOrder.UpstreamOrderID); err != nil {
			logger.Warnw("procurement_cancel_upstream_failed",
				"procurement_order_id", procOrder.ID,
				"upstream_order_id", procOrder.UpstreamOrderID,
				"error", err,
			)
			return fmt.Errorf("cancel upstream order: %w", err)
		}
	default:
		return procurementcontract.ErrStatusInvalid
	}

	now := time.Now()
	updates := map[string]interface{}{
		"error_message": "manually canceled",
		"updated_at":    now,
	}
	canceled, err := s.procRepo.UpdateStatus(procOrder.ID, procOrder.Status, constants.ProcurementStatusCanceled, updates)
	if err != nil {
		return fmt.Errorf("update procurement status: %w", err)
	}
	if !canceled {
		return procurementcontract.ErrStatusInvalid
	}

	logger.Infow("procurement_manual_cancel",
		"procurement_order_id", procOrder.ID,
	)
	return nil
}

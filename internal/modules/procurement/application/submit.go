package application

import (
	"context"
	"crypto/rand"
	"encoding/hex"
	"errors"
	"fmt"
	"strconv"
	"strings"
	"time"

	"github.com/dujiao-next/internal/constants"
	"github.com/dujiao-next/internal/logger"
	procurementcontract "github.com/dujiao-next/internal/modules/procurement/contract"
	procurementdomain "github.com/dujiao-next/internal/modules/procurement/domain"
	"github.com/shopspring/decimal"
)

// SubmitToUpstream Worker 调用：向上游站点提交采购单
func (s *Service) SubmitToUpstream(procurementOrderID uint) error {
	procOrder, err := s.procRepo.GetByID(procurementOrderID)
	if err != nil {
		return fmt.Errorf("load procurement order: %w", err)
	}
	if procOrder == nil {
		return procurementcontract.ErrNotFound
	}
	rejectFakaBeforeTrade := func(cause error) error {
		updated, updateErr := s.procRepo.UpdateStatus(procOrder.ID, constants.ProcurementStatusSubmitting, constants.ProcurementStatusRejected, map[string]interface{}{
			"error_message": cause.Error(),
			"updated_at":    time.Now(),
		})
		if updateErr != nil {
			return fmt.Errorf("persist Faka pre-trade rejection (submission remains frozen): %w", updateErr)
		}
		if !updated {
			return fmt.Errorf("persist Faka pre-trade rejection: %w", procurementcontract.ErrStatusInvalid)
		}
		logger.Warnw("faka_procurement_rejected_before_trade", "procurement_order_id", procOrder.ID, "trace_id", procOrder.TraceID, "error", cause)
		return nil
	}

	// 校验状态
	if procOrder.Status != "pending" && procOrder.Status != "failed" {
		return procurementcontract.ErrStatusInvalid
	}

	// 获取连接和适配器
	localOrder, err := s.orderRepo.GetByID(procOrder.LocalOrderID)
	if err != nil {
		s.markProcurementError(procOrder, fmt.Sprintf("load local order failed: %v", err))
		return fmt.Errorf("load local order: %w", err)
	}
	if localOrder == nil {
		s.rejectProcurement(procOrder, fmt.Sprintf("local order %d not found", procOrder.LocalOrderID))
		return nil
	}
	if len(localOrder.Items) != 1 {
		s.rejectProcurement(procOrder, fmt.Sprintf("local order %d has %d items; procurement supports one item per child order", localOrder.ID, len(localOrder.Items)))
		return nil
	}
	productMapping, err := s.mappingRepo.FindMappingForProduct(localOrder.Items[0].ProductID)
	if err != nil {
		s.markProcurementError(procOrder, fmt.Sprintf("read product mapping failed: %v", err))
		return fmt.Errorf("read product mapping: %w", err)
	}
	skuMapping, err := s.skuMapRepo.FindMappingForSKU(localOrder.Items[0].SKUID)
	if err != nil {
		s.markProcurementError(procOrder, fmt.Sprintf("read SKU mapping failed: %v", err))
		return fmt.Errorf("read SKU mapping: %w", err)
	}
	connection, err := s.connections.Open(procOrder.ConnectionID)
	if err != nil {
		if errors.Is(err, procurementcontract.ErrConnectionNotFound) {
			s.rejectProcurement(procOrder, err.Error())
			return nil
		}
		s.markProcurementError(procOrder, fmt.Sprintf("load connection failed: %v", err))
		return fmt.Errorf("load connection: %w", err)
	}
	if connection == nil {
		s.rejectProcurement(procOrder, fmt.Sprintf("connection %d not found", procOrder.ConnectionID))
		return nil // 永久性错误，不重试
	}
	if err := procurementcontract.ValidateProcurementMapping(productMapping, skuMapping, localOrder.Items[0].ProductID, localOrder.Items[0].SKUID, procOrder.ConnectionID, connection.Protocol() == constants.ConnectionProtocolFaka); err != nil {
		s.rejectProcurement(procOrder, err.Error())
		return nil
	}
	fakaSubmission := connection.Protocol() == constants.ConnectionProtocolFaka
	if connection == nil {
		s.rejectProcurement(procOrder, fmt.Sprintf("connection %d not found", procOrder.ConnectionID))
		return nil // 永久性错误，不重试
	}

	// 加载本地订单获取 SKU 信息
	if localOrder == nil {
		s.rejectProcurement(procOrder, fmt.Sprintf("local order %d not found", procOrder.LocalOrderID))
		return nil // 永久性错误，不重试
	}
	// 本地订单已退款/取消等不再需要采购（如巡检补投的历史积压单），不能再去上游扣款
	if localOrder.Status != constants.OrderStatusPaid && localOrder.Status != constants.OrderStatusFulfilling {
		s.rejectProcurement(procOrder, fmt.Sprintf("local order %d status %s no longer needs procurement", localOrder.ID, localOrder.Status))
		return nil // 永久性错误，不重试
	}
	if len(localOrder.Items) == 0 {
		s.rejectProcurement(procOrder, fmt.Sprintf("local order %d has no items", localOrder.ID))
		return nil // 永久性错误，不重试
	}
	item := localOrder.Items[0]

	// 查找 SKU 映射
	var upstreamSKUID uint
	if !fakaSubmission {
		var found bool
		upstreamSKUID, found, err = s.skuMapRepo.FindUpstreamSKUID(item.SKUID)
		if err != nil {
			s.markProcurementError(procOrder, fmt.Sprintf("lookup sku mapping failed: %v", err))
			return fmt.Errorf("lookup sku mapping: %w", err)
		}
		if !found {
			s.rejectProcurement(procOrder, fmt.Sprintf("no sku mapping for local sku %d", item.SKUID))
			return nil // 永久性错误，不重试
		}
	}

	// 构建上游请求
	req := procurementcontract.CreateOrderRequest{
		SKUID:               upstreamSKUID,
		UpstreamProductCode: productMapping.UpstreamProductCode,
		Quantity:            item.Quantity,
		DownstreamOrderNo:   localOrder.OrderNo,
		TraceID:             procOrder.TraceID,
		CallbackURL:         connection.CallbackURL(),
	}

	// 传递人工表单数据（如有）
	if len(item.ManualFormSubmissionJSON) > 0 {
		req.ManualFormData = item.ManualFormSubmissionJSON
	}

	ctx, cancel := context.WithTimeout(context.Background(), 30*time.Second)
	defer cancel()

	var fakaIntentEventID string
	if fakaSubmission {
		claimed, claimErr := s.procRepo.UpdateStatus(procOrder.ID, procOrder.Status, constants.ProcurementStatusSubmitting, map[string]interface{}{
			"updated_at": time.Now(),
		})
		if claimErr != nil {
			return fmt.Errorf("claim faka procurement submission: %w", claimErr)
		}
		if !claimed {
			return procurementcontract.ErrStatusInvalid
		}
		procOrder.Status = constants.ProcurementStatusSubmitting
		if s.evidenceStore == nil {
			return s.freezeFakaForEvidenceFailure(procOrder, errors.New("local Trade evidence store is unavailable"))
		}
		existingEvents, evidenceErr := s.evidenceStore.GetByTraceID(procOrder.TraceID)
		if evidenceErr != nil {
			return s.freezeFakaForEvidenceFailure(procOrder, fmt.Errorf("read existing Trade evidence: %w", evidenceErr))
		}
		for _, event := range existingEvents {
			if event.ProcurementOrderID == procOrder.ID && event.EventType == "trade_intent" {
				return s.freezeFakaForEvidenceFailure(procOrder, errors.New("prior Trade intent exists without a safe new submission authorization"))
			}
		}
		intentID, idErr := newEvidenceEventID()
		if idErr != nil {
			return s.freezeFakaForEvidenceFailure(procOrder, idErr)
		}
		intent := procurementcontract.EvidenceEvent{EventID: intentID, ProcurementOrderID: procOrder.ID, TraceID: procOrder.TraceID, EventType: "trade_intent", SchemaVersion: 1, Supplier: "faka"}
		if err := s.evidenceStore.AppendIntent(intent); err != nil {
			return s.freezeFakaForEvidenceFailure(procOrder, fmt.Errorf("persist local Trade intent: %w", err))
		}
		fakaIntentEventID = intentID
		if s.evidenceConfirmer == nil {
			return s.freezeFakaForEvidenceFailure(procOrder, errors.New("independent S3 evidence confirmation is not configured; Trade is disabled"))
		}
		if err := s.evidenceConfirmer.ConfirmIndependent(intentID); err != nil {
			return s.freezeFakaForEvidenceFailure(procOrder, fmt.Errorf("confirm independent Trade intent persistence: %w", err))
		}

		// Re-read persistent mapping identity after acquiring the unique submission right.
		latestMapping, mappingErr := s.mappingRepo.FindMappingForProduct(item.ProductID)
		if mappingErr != nil {
			return rejectFakaBeforeTrade(fmt.Errorf("reload Faka product mapping: %w", mappingErr))
		}
		latestSKU, skuErr := s.skuMapRepo.FindMappingForSKU(item.SKUID)
		if skuErr != nil {
			return rejectFakaBeforeTrade(fmt.Errorf("reload Faka SKU mapping: %w", skuErr))
		}
		if err := procurementcontract.ValidateProcurementMapping(latestMapping, latestSKU, item.ProductID, item.SKUID, procOrder.ConnectionID, true); err != nil {
			return rejectFakaBeforeTrade(err)
		}
		if latestMapping.ID != productMapping.ID || latestMapping.UpstreamProductID != productMapping.UpstreamProductID || latestMapping.UpstreamProductCode != productMapping.UpstreamProductCode {
			return rejectFakaBeforeTrade(fmt.Errorf("Faka persisted product identity changed before live verification"))
		}
		verifier, ok := connection.(procurementcontract.FakaProcurementCapabilityVerifier)
		if !ok {
			return rejectFakaBeforeTrade(fmt.Errorf("Faka live procurement capability verifier is unavailable"))
		}
		if err := verifier.VerifyFakaProcurementCapability(ctx, latestMapping.UpstreamProductCode, latestMapping.UpstreamProductID, item.Quantity); err != nil {
			return rejectFakaBeforeTrade(fmt.Errorf("Faka live procurement capability check failed: %w", err))
		}
	}

	resp, err := connection.CreateOrder(ctx, req)
	if err != nil {
		if fakaSubmission {
			if errors.Is(err, procurementcontract.ErrSubmissionNotSent) {
				return rejectFakaBeforeTrade(err)
			}
			freezeUpdates := map[string]interface{}{
				"error_message": fmt.Sprintf("faka submission result is unknown: %v", err),
				"updated_at":    time.Now(),
			}
			if resp != nil && strings.TrimSpace(resp.OrderNo) != "" {
				freezeUpdates["upstream_order_no"] = resp.OrderNo
			}
			frozen, freezeErr := s.procRepo.UpdateStatus(procOrder.ID, constants.ProcurementStatusSubmitting, constants.ProcurementStatusAwaitingVerification, freezeUpdates)
			if freezeErr != nil {
				return fmt.Errorf("freeze procurement for manual verification: %w", freezeErr)
			}
			if !frozen {
				return fmt.Errorf("freeze procurement for manual verification: %w", procurementcontract.ErrStatusInvalid)
			}
			logger.Warnw("procurement_awaiting_verification", "procurement_order_id", procOrder.ID, "trace_id", procOrder.TraceID, "error", err)
			return nil
		}
		return s.handleSubmitFailure(procOrder, connection, fmt.Sprintf("upstream request error: %v", err), true)
	}

	if !resp.OK {
		if fakaSubmission {
			errMsg := strings.TrimSpace(resp.ErrorMessage)
			if errMsg == "" {
				errMsg = strings.TrimSpace(resp.ErrorCode)
			}
			if errMsg == "" {
				errMsg = "upstream rejected procurement"
			}
			return s.handleSubmitFailure(procOrder, connection, fmt.Sprintf("faka rejected procurement: %s", errMsg), false)
		}
		retryable := isRetryableErrorCode(resp.ErrorCode)
		errMsg := resp.ErrorMessage
		if errMsg == "" {
			errMsg = resp.ErrorCode
		}
		return s.handleSubmitFailure(procOrder, connection, errMsg, retryable)
	}

	// 成功：更新状态，重置 retry_count 用于轮询阶段
	now := time.Now()
	upstreamFields := map[string]interface{}{
		"upstream_order_id": resp.OrderID,
		"upstream_order_no": resp.OrderNo,
		"upstream_amount":   resp.Amount,
		"upstream_currency": resp.Currency,
		"updated_at":        now,
	}
	if fakaSubmission {
		responseID, idErr := newEvidenceEventID()
		if idErr != nil {
			return s.freezeFakaForEvidenceFailure(procOrder, idErr)
		}
		responseEvent := procurementcontract.EvidenceEvent{EventID: responseID, ProcurementOrderID: procOrder.ID, TraceID: procOrder.TraceID, EventType: "trade_response", TradeNo: resp.OrderNo, Amount: resp.Amount, ResponseAt: now.UTC(), SchemaVersion: 1, Supplier: "faka", RelatedEventID: fakaIntentEventID}
		if err := s.evidenceStore.AppendResponse(responseEvent); err != nil {
			return s.freezeFakaForEvidenceFailure(procOrder, fmt.Errorf("persist local Trade response: %w", err))
		}
		if s.evidenceConfirmer == nil {
			return s.freezeFakaForEvidenceFailure(procOrder, errors.New("independent S3 response confirmation is not configured; normal acceptance is disabled"))
		}
		if err := s.evidenceConfirmer.ConfirmIndependent(responseID); err != nil {
			return s.freezeFakaForEvidenceFailure(procOrder, fmt.Errorf("confirm independent Trade response persistence: %w", err))
		}
		entry, journalErr := procurementcontract.TradeResponseJournalEntryFromEvidence(responseEvent)
		if s.decisionAudit == nil {
			journalErr = errors.New("journal is unavailable")
		} else if journalErr == nil {
			journalErr = s.decisionAudit.AppendTradeResponseJournal(entry)
		}
		if journalErr != nil {
			logger.Errorw("faka_trade_response_journal_failed", "procurement_order_id", procOrder.ID, "trace_id", procOrder.TraceID, "error", journalErr)
			frozen, freezeErr := s.procRepo.UpdateStatus(procOrder.ID, constants.ProcurementStatusSubmitting, constants.ProcurementStatusAwaitingVerification, map[string]interface{}{
				"error_message": "Faka Trade succeeded but response journal persistence failed; verify upstream result before recovery",
				"updated_at":    time.Now(),
			})
			if freezeErr != nil || !frozen {
				logger.Errorw("faka_trade_journal_failure_freeze_failed", "procurement_order_id", procOrder.ID, "trace_id", procOrder.TraceID, "error", freezeErr, "updated", frozen)
			}
			s.notifyProcurementFailure(procOrder, fmt.Sprintf("CRITICAL: Faka Trade succeeded but response journal persistence failed for trace %s", procOrder.TraceID))
			if freezeErr != nil {
				return fmt.Errorf("persist Faka Trade response evidence: %w; freeze procurement for manual verification: %v", journalErr, freezeErr)
			}
			if !frozen {
				return fmt.Errorf("persist Faka Trade response evidence: %w; procurement state changed before it could be frozen", journalErr)
			}
			return fmt.Errorf("persist Faka Trade response evidence: %w", journalErr)
		}
	}
	updates := make(map[string]interface{}, len(upstreamFields)+2)
	for key, value := range upstreamFields {
		updates[key] = value
	}
	updates["error_message"] = ""
	updates["retry_count"] = 0
	fromStatus := procOrder.Status
	if fakaSubmission {
		fromStatus = constants.ProcurementStatusSubmitting
	}
	accepted, err := s.procRepo.UpdateStatus(procOrder.ID, fromStatus, constants.ProcurementStatusAccepted, updates)
	if err != nil {
		if fakaSubmission {
			logger.Errorw("faka_trade_succeeded_but_local_write_failed", "procurement_order_id", procOrder.ID, "trace_id", procOrder.TraceID, "trade_no", resp.OrderNo, "error", err)
			// The trade already succeeded. Preserve its number and freeze even if the
			// accepted-state write failed; if this also fails, submitting stays frozen.
			frozen, freezeErr := s.procRepo.UpdateStatus(procOrder.ID, constants.ProcurementStatusSubmitting, constants.ProcurementStatusAwaitingVerification, map[string]interface{}{
				"error_message": "Faka Trade succeeded but local acceptance write failed; verify upstream result before recovery",
				"updated_at":    time.Now(),
			})
			if freezeErr != nil || !frozen {
				logger.Errorw("faka_trade_freeze_failed", "procurement_order_id", procOrder.ID, "trace_id", procOrder.TraceID, "trade_no", resp.OrderNo, "error", freezeErr, "updated", frozen)
			}
			return s.reportFakaTradeCASRecovery(procOrder, resp, err, frozen, freezeErr)
		}
		return fmt.Errorf("update procurement status: %w", err)
	}
	if !accepted {
		if fakaSubmission {
			current, readErr := s.procRepo.GetByID(procOrder.ID)
			if readErr != nil {
				return fmt.Errorf("reload procurement after lost Faka submission CAS: %w", readErr)
			}
			if current == nil {
				return procurementcontract.ErrNotFound
			}
			if current.Status == constants.ProcurementStatusAccepted {
				conflictType := "late_response_same_trade_no"
				if current.UpstreamOrderNo != resp.OrderNo {
					conflictType = "late_response_trade_no_mismatch"
				}
				if s.decisionAudit == nil {
					return fmt.Errorf("persist late Faka trade response: audit unavailable")
				}
				if auditErr := s.decisionAudit.AppendTradeResponseJournal(procurementdomain.TradeResponseJournalEntry{
					ProcurementOrderID: procOrder.ID, TraceID: procOrder.TraceID, UpstreamTradeNo: resp.OrderNo,
					Amount: resp.Amount, ConfirmedTradeNo: current.UpstreamOrderNo, ConfirmedAmount: current.UpstreamAmount.String(),
					ResponseTime: time.Now(), EventType: conflictType,
				}); auditErr != nil {
					return fmt.Errorf("persist late Faka trade journal event: %w", auditErr)
				}
				if current.UpstreamOrderNo != resp.OrderNo {
					return fmt.Errorf("Faka late Trade conflict: administrator confirmed trade %q, worker returned %q", current.UpstreamOrderNo, resp.OrderNo)
				}
				return fmt.Errorf("Faka Trade response arrived after manual confirmation; matching trade %q recorded", resp.OrderNo)
			}
			logger.Errorw("faka_late_trade_state_conflict", "procurement_order_id", procOrder.ID, "trace_id", procOrder.TraceID, "status", current.Status, "worker_trade_no", resp.OrderNo)
			return fmt.Errorf("Faka late Trade returned after procurement state changed to %q", current.Status)
		}
		return s.recordUpstreamOrderAfterStatusChanged(procOrder.ID, resp.OrderNo, upstreamFields)
	}

	logger.Infow("procurement_order_accepted",
		"procurement_order_id", procOrder.ID,
		"upstream_order_id", resp.OrderID,
		"upstream_order_no", resp.OrderNo,
	)

	// 更新本地订单状态为 fulfilling。
	// shortcut: 本地订单写入不做比较后再写，回调恰好落在上一步与这一步之间时仍可能回退为 fulfilling；
	// 窗口仅两条语句，需要时给订单仓储加带前置状态的更新。
	_ = s.orderRepo.UpdateStatus(localOrder.ID, constants.OrderStatusFulfilling, map[string]interface{}{
		"updated_at": now,
	})

	// 入队轮询任务（30s 延迟，作为回调的 fallback）
	if s.queue != nil {
		_ = s.queue.EnqueuePoll(procOrder.ID, 30*time.Second)
	}

	return nil
}

func newEvidenceEventID() (string, error) {
	var id [16]byte
	if _, err := rand.Read(id[:]); err != nil {
		return "", fmt.Errorf("generate evidence event ID: %w", err)
	}
	return hex.EncodeToString(id[:]), nil
}

func (s *Service) freezeFakaForEvidenceFailure(order *procurementdomain.Order, cause error) error {
	if order == nil {
		return fmt.Errorf("Faka Trade blocked by evidence failure: %w", cause)
	}
	frozen, freezeErr := s.procRepo.UpdateStatus(order.ID, constants.ProcurementStatusSubmitting, constants.ProcurementStatusAwaitingVerification, map[string]interface{}{"error_message": "Faka Trade blocked because durable independent evidence confirmation is unavailable; manual verification required", "updated_at": time.Now()})
	if freezeErr != nil {
		return fmt.Errorf("Faka Trade blocked by evidence failure: %w; freeze failed: %v; order remains submitting and must not retry", cause, freezeErr)
	}
	if !frozen {
		return fmt.Errorf("Faka Trade blocked by evidence failure: %w; freeze CAS did not apply; order must not retry", cause)
	}
	return fmt.Errorf("Faka Trade blocked by evidence failure: %w; order frozen for manual verification", cause)
}

// reportFakaTradeCASRecovery only reads durable state after an ambiguous local
// write. It never retries Trade, changes procurement state, or enqueues work.
func (s *Service) reportFakaTradeCASRecovery(procOrder *procurementdomain.Order, response *procurementcontract.CreateOrderResult, casErr error, freezeSucceeded bool, freezeErr error) error {
	if procOrder == nil || response == nil {
		return fmt.Errorf("Faka Trade response recovery failed: missing procurement order or response; Trade CAS error: %w", casErr)
	}
	current, readErr := s.procRepo.GetByID(procOrder.ID)
	if readErr != nil {
		return fmt.Errorf("Faka Trade response recovery failed: reload procurement order %d: %v (freeze_succeeded=%t, freeze_error=%v); Trade CAS result is uncertain: %w", procOrder.ID, freezeSucceeded, freezeErr, readErr, casErr)
	}
	if current == nil {
		return fmt.Errorf("Faka Trade response recovery failed: procurement order %d is missing after CAS error (freeze_succeeded=%t, freeze_error=%v); Trade CAS result is uncertain: %w", procOrder.ID, freezeSucceeded, freezeErr, casErr)
	}
	if current.TraceID != procOrder.TraceID {
		return fmt.Errorf("Faka Trade response recovery failed: procurement order %d TraceID changed from %q to %q; state left untouched; Trade CAS result is uncertain: %w", procOrder.ID, procOrder.TraceID, current.TraceID, casErr)
	}
	if s.decisionAudit == nil {
		return fmt.Errorf("Faka Trade response recovery failed: journal reader unavailable for procurement order %d trace %q (status=%q, freeze_succeeded=%t, freeze_error=%v); Trade CAS result is uncertain: %w", procOrder.ID, procOrder.TraceID, current.Status, freezeSucceeded, freezeErr, casErr)
	}
	events, journalErr := s.decisionAudit.ListTradeResponseJournalByTraceID(procOrder.TraceID)
	if journalErr != nil {
		return fmt.Errorf("Faka Trade response recovery failed: read journal for procurement order %d trace %q: %v (status=%q, freeze_succeeded=%t, freeze_error=%v); Trade CAS result is uncertain: %w", procOrder.ID, procOrder.TraceID, journalErr, current.Status, freezeSucceeded, freezeErr, casErr)
	}
	sameAmount := func(left, right string) bool {
		leftAmount, leftErr := decimal.NewFromString(strings.TrimSpace(left))
		rightAmount, rightErr := decimal.NewFromString(strings.TrimSpace(right))
		return leftErr == nil && rightErr == nil && leftAmount.Equal(rightAmount)
	}
	sameStoredAmount := func(left, right string) bool {
		leftAmount, leftErr := decimal.NewFromString(strings.TrimSpace(left))
		rightAmount, rightErr := decimal.NewFromString(strings.TrimSpace(right))
		return leftErr == nil && rightErr == nil && leftAmount.Round(2).Equal(rightAmount.Round(2))
	}
	matching := 0
	for _, event := range events {
		if event.TraceID != procOrder.TraceID {
			continue
		}
		if event.ProcurementOrderID != procOrder.ID {
			return fmt.Errorf("Faka Trade response recovery conflict: TraceID %q is also associated with procurement order %d, expected %d; current status=%q; state preserved; CAS result uncertain: %w", procOrder.TraceID, event.ProcurementOrderID, procOrder.ID, current.Status, casErr)
		}
		if event.EventType != "trade_success_response" {
			continue
		}
		if event.UpstreamTradeNo != response.OrderNo || !sameAmount(event.Amount, response.Amount) {
			return fmt.Errorf("Faka Trade response recovery conflict: journal evidence for procurement order %d trace %q has trade=%q amount=%q, response has trade=%q amount=%q; current status=%q; no state changed; CAS result uncertain: %w", procOrder.ID, procOrder.TraceID, event.UpstreamTradeNo, event.Amount, response.OrderNo, response.Amount, current.Status, casErr)
		}
		matching++
	}
	if matching != 1 {
		return fmt.Errorf("Faka Trade response recovery failed: found %d matching durable success events for procurement order %d trace %q trade=%q amount=%q; current status=%q, freeze_succeeded=%t, freeze_error=%v; CAS result uncertain: %w", matching, procOrder.ID, procOrder.TraceID, response.OrderNo, response.Amount, current.Status, freezeSucceeded, freezeErr, casErr)
	}
	if current.UpstreamOrderNo != "" && current.UpstreamOrderNo != response.OrderNo {
		return fmt.Errorf("Faka Trade response recovery conflict: procurement order %d trace %q currently has trade=%q amount=%s, journal has trade=%q amount=%q; status=%q; administrator/current data preserved; CAS result uncertain: %w", procOrder.ID, procOrder.TraceID, current.UpstreamOrderNo, current.UpstreamAmount.String(), response.OrderNo, response.Amount, current.Status, casErr)
	}
	if current.UpstreamOrderNo == "" && current.UpstreamAmount.String() != "0.00" && !sameStoredAmount(current.UpstreamAmount.String(), response.Amount) {
		return fmt.Errorf("Faka Trade response recovery conflict: procurement order %d trace %q has no trade number but current amount=%s differs from journal amount=%q; status=%q; data preserved; CAS result uncertain: %w", procOrder.ID, procOrder.TraceID, current.UpstreamAmount.String(), response.Amount, current.Status, casErr)
	}
	if current.UpstreamOrderNo == response.OrderNo && !sameStoredAmount(current.UpstreamAmount.String(), response.Amount) {
		return fmt.Errorf("Faka Trade response recovery conflict: procurement order %d trace %q has matching trade=%q but current amount=%s differs from journal amount=%q; status=%q; data preserved; CAS result uncertain: %w", procOrder.ID, procOrder.TraceID, response.OrderNo, current.UpstreamAmount.String(), response.Amount, current.Status, casErr)
	}
	if current.Status == constants.ProcurementStatusAccepted && current.UpstreamOrderNo == response.OrderNo && sameStoredAmount(current.UpstreamAmount.String(), response.Amount) {
		return fmt.Errorf("Faka Trade response recovery verified: procurement order %d trace %q already contains matching accepted state trade=%q amount=%q; no change made; original database result was uncertain: %w", procOrder.ID, procOrder.TraceID, response.OrderNo, response.Amount, casErr)
	}
	return fmt.Errorf("Faka Trade response recovery evidence ready for administrator review: procurement order %d trace %q status=%q trade=%q amount=%q; freeze_succeeded=%t, freeze_error=%v; no state change or retry performed; original database result was uncertain: %w", procOrder.ID, procOrder.TraceID, current.Status, response.OrderNo, response.Amount, freezeSucceeded, freezeErr, casErr)
}

// markProcurementError 记录错误信息但不改变状态（用于瞬态错误，asynq 可重试）
func (s *Service) markProcurementError(procOrder *procurementdomain.Order, errMsg string) {
	now := time.Now()
	_, _ = s.procRepo.UpdateStatus(procOrder.ID, procOrder.Status, procOrder.Status, map[string]interface{}{
		"error_message": errMsg,
		"updated_at":    now,
	})
	logger.Warnw("procurement_prepare_error",
		"procurement_order_id", procOrder.ID,
		"error", errMsg,
	)
}

// rejectProcurement 将采购单标记为 rejected（用于永久性配置错误，不值得重试）
// 同时回退本地订单状态并通知管理员
func (s *Service) rejectProcurement(procOrder *procurementdomain.Order, errMsg string) {
	now := time.Now()
	rejected, _ := s.procRepo.UpdateStatus(procOrder.ID, procOrder.Status, constants.ProcurementStatusRejected, map[string]interface{}{
		"error_message": errMsg,
		"updated_at":    now,
	})
	if !rejected {
		return
	}
	logger.Warnw("procurement_rejected_config_error",
		"procurement_order_id", procOrder.ID,
		"error", errMsg,
	)
	s.rollbackLocalOrderOnProcurementFailure(procOrder, errMsg)
}

// recordUpstreamOrderAfterStatusChanged 上游已成单，但采购单在请求途中被回调推进（如秒发卡密）或被管理员取消：
// 不回退状态，只补记上游订单号，否则对账与上游退款同步会跳过这单；被取消的需告警人工处理。
func (s *Service) recordUpstreamOrderAfterStatusChanged(id uint, upstreamOrderNo string, fields map[string]interface{}) error {
	current, err := s.procRepo.GetByID(id)
	if err != nil {
		return fmt.Errorf("reload procurement order: %w", err)
	}
	if current == nil {
		return procurementcontract.ErrNotFound
	}
	if _, err := s.procRepo.UpdateStatus(id, current.Status, current.Status, fields); err != nil {
		return fmt.Errorf("record upstream order: %w", err)
	}
	if current.Status == constants.ProcurementStatusCanceled {
		logger.Errorw("procurement_canceled_but_upstream_created",
			"procurement_order_id", id,
			"upstream_order_no", upstreamOrderNo,
		)
		s.notifyProcurementFailure(current, fmt.Sprintf("procurement canceled locally but upstream order %s was created and paid", upstreamOrderNo))
		return nil
	}
	logger.Infow("procurement_accept_skipped_status_changed", "procurement_order_id", id, "status", current.Status)
	return nil
}

// rollbackLocalOrderOnProcurementFailure 采购单终态失败时回退本地订单状态并通知管理员
func (s *Service) rollbackLocalOrderOnProcurementFailure(procOrder *procurementdomain.Order, errMsg string) {
	localOrder, err := s.orderRepo.GetByID(procOrder.LocalOrderID)
	if err != nil || localOrder == nil {
		return
	}
	if localOrder.Status == constants.OrderStatusFulfilling {
		now := time.Now()
		_ = s.orderRepo.UpdateStatus(localOrder.ID, constants.OrderStatusPaid, map[string]interface{}{
			"updated_at": now,
		})
		// 如果是子订单，同步父订单状态

		if localOrder.ParentID != nil && s.orderLifecycle != nil {
			_, _ = s.orderLifecycle.SyncParentStatus(*localOrder.ParentID, now)
		}
		logger.Infow("procurement_failure_order_rolled_back",
			"procurement_order_id", procOrder.ID,
			"local_order_id", localOrder.ID,
			"from_status", constants.OrderStatusFulfilling,
			"to_status", constants.OrderStatusPaid,
		)
	}
	s.notifyProcurementFailure(procOrder, errMsg)
}

// notifyProcurementFailure 发送采购失败异常告警
func (s *Service) notifyProcurementFailure(procOrder *procurementdomain.Order, errMsg string) {
	if s.notifications == nil {
		return
	}
	if err := s.notifications.NotifyFailure(procOrder, errMsg); err != nil {
		logger.Errorw("procurement_failure_alert_enqueue_failed", "procurement_order_id", procOrder.ID, "error", err)
	}
}

// handleSubmitFailure 处理提交失败
func (s *Service) handleSubmitFailure(procOrder *procurementdomain.Order, connection procurementcontract.UpstreamConnection, errMsg string, retryable bool) error {
	now := time.Now()

	if retryable && procOrder.RetryCount < connection.RetryMax() {
		intervals := parseRetryIntervals(connection.RetryIntervals())
		idx := procOrder.RetryCount
		if idx >= len(intervals) {
			idx = len(intervals) - 1
		}
		delay := intervals[idx]
		nextRetry := now.Add(delay)

		updates := map[string]interface{}{
			"retry_count":   procOrder.RetryCount + 1,
			"next_retry_at": &nextRetry,
			"error_message": errMsg,
			"updated_at":    now,
		}
		failed, err := s.procRepo.UpdateStatus(procOrder.ID, procOrder.Status, constants.ProcurementStatusFailed, updates)
		if err != nil {
			return fmt.Errorf("update procurement status (failed): %w", err)
		}
		if !failed {
			return nil
		}

		logger.Warnw("procurement_submit_failed_retryable",
			"procurement_order_id", procOrder.ID,
			"retry_count", procOrder.RetryCount+1,
			"next_retry_at", nextRetry,
			"error", errMsg,
		)

		// 按配置间隔延迟重试：上游超时可能仍在处理首个请求，立即重试会与之并发
		if s.queue != nil {
			if err := s.queue.EnqueueSubmit(procOrder.ID, delay); err != nil {
				logger.Warnw("procurement_enqueue_retry_failed", "procurement_order_id", procOrder.ID, "error", err)
			}
		}

		return nil
	}

	// 不可重试或已达上限：拒绝
	updates := map[string]interface{}{
		"error_message": errMsg,
		"updated_at":    now,
	}
	rejected, err := s.procRepo.UpdateStatus(procOrder.ID, procOrder.Status, constants.ProcurementStatusRejected, updates)
	if err != nil {
		return fmt.Errorf("update procurement status (rejected): %w", err)
	}
	if !rejected {
		return nil
	}

	logger.Warnw("procurement_submit_rejected",
		"procurement_order_id", procOrder.ID,
		"error", errMsg,
	)

	// 回退本地订单状态并通知管理员
	s.rollbackLocalOrderOnProcurementFailure(procOrder, errMsg)

	return fmt.Errorf("procurement rejected: %s", errMsg)
}

// isRetryableErrorCode 判断上游错误码是否可重试
func isRetryableErrorCode(code string) bool {
	nonRetryable := map[string]bool{
		"insufficient_balance": true,
		"payment_failed":       true,
		"product_unavailable":  true,
		"sku_unavailable":      true,
		"invalid_request":      true,
		"unauthorized":         true,
		"forbidden":            true,
		"duplicate_order":      true,
		"product_out_of_stock": true,
	}
	return !nonRetryable[strings.ToLower(strings.TrimSpace(code))]
}

// parseRetryIntervals 解析重试间隔配置（JSON 数组格式如 "[30,60,300]"）
func parseRetryIntervals(raw string) []time.Duration {
	raw = strings.TrimSpace(raw)
	// 移除方括号
	raw = strings.TrimPrefix(raw, "[")
	raw = strings.TrimSuffix(raw, "]")

	if raw == "" {
		return []time.Duration{30 * time.Second, 60 * time.Second, 300 * time.Second}
	}

	parts := strings.Split(raw, ",")
	intervals := make([]time.Duration, 0, len(parts))
	for _, part := range parts {
		part = strings.TrimSpace(part)
		if part == "" {
			continue
		}
		seconds, err := strconv.Atoi(part)
		if err != nil || seconds <= 0 {
			continue
		}
		intervals = append(intervals, time.Duration(seconds)*time.Second)
	}

	if len(intervals) == 0 {
		return []time.Duration{30 * time.Second, 60 * time.Second, 300 * time.Second}
	}
	return intervals
}

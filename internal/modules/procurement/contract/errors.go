package contract

import "errors"

var (
	ErrNotFound                     = errors.New("procurement order not found")
	ErrExists                       = errors.New("procurement order already exists")
	ErrStatusInvalid                = errors.New("procurement order status invalid")
	ErrMappingInvalid               = errors.New("procurement mapping identity invalid")
	ErrSubmissionNotSent            = errors.New("upstream procurement request was not sent")
	ErrOrderNotFound                = errors.New("order not found")
	ErrInvalidTradeResponseEvidence = errors.New("invalid or incomplete Trade response evidence")
	ErrEvidenceJournalConflict      = errors.New("Journal source EventID payload conflict; source evidence isolated")
	ErrConnectionNotFound           = errors.New("site connection not found")
	// ErrUpstreamCancelRefused 上游拒绝取消（订单已在上游付款），需到上游申请退款
	ErrUpstreamCancelRefused = errors.New("upstream refused to cancel order")
)

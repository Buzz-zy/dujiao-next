package upstreamgateway

import (
	"context"
	"errors"
	"fmt"

	"github.com/dujiao-next/internal/constants"
	procurementcontract "github.com/dujiao-next/internal/modules/procurement/contract"
	siteconnectiondomain "github.com/dujiao-next/internal/modules/siteconnection/domain"
	"github.com/dujiao-next/internal/upstream"
)

type Provider interface {
	GetByID(id uint) (*siteconnectiondomain.Connection, error)
	GetAdapter(connection *siteconnectiondomain.Connection) (upstream.Adapter, error)
}

type Gateway struct {
	provider Provider
}

var _ procurementcontract.ConnectionProvider = (*Gateway)(nil)

func New(provider Provider) *Gateway {
	if provider == nil {
		panic("procurement upstream gateway: provider is nil")
	}
	return &Gateway{provider: provider}
}

func (g *Gateway) Open(connectionID uint) (procurementcontract.UpstreamConnection, error) {
	connection, err := g.provider.GetByID(connectionID)
	if err != nil || connection == nil {
		return nil, err
	}
	adapter, err := g.provider.GetAdapter(connection)
	if err != nil {
		return nil, err
	}
	return &session{connection: connection, adapter: adapter}, nil
}

type session struct {
	connection *siteconnectiondomain.Connection
	adapter    upstream.Adapter
}

var _ procurementcontract.UpstreamConnection = (*session)(nil)

func (s *session) CallbackURL() string    { return s.connection.CallbackURL }
func (s *session) Protocol() string       { return s.connection.Protocol }
func (s *session) RetryMax() int          { return s.connection.RetryMax }
func (s *session) RetryIntervals() string { return s.connection.RetryIntervals }

func (s *session) VerifyFakaProcurementCapability(ctx context.Context, code string, expectedProductID uint, quantity int) error {
	verifier, ok := s.adapter.(upstream.FakaCapabilityVerifier)
	if !ok {
		return fmt.Errorf("faka adapter does not support live procurement capability verification")
	}
	_, err := verifier.VerifyFakaProcurementCapability(ctx, code, expectedProductID, quantity)
	return err
}

func (s *session) CreateOrder(ctx context.Context, request procurementcontract.CreateOrderRequest) (*procurementcontract.CreateOrderResult, error) {
	upstreamRequest := upstream.CreateUpstreamOrderReq{
		SKUID:    request.SKUID,
		Quantity: request.Quantity, ManualFormData: request.ManualFormData,
		DownstreamOrderNo: request.DownstreamOrderNo, TraceID: request.TraceID, CallbackURL: request.CallbackURL,
	}
	if s.connection.Protocol == constants.ConnectionProtocolFaka {
		upstreamRequest.SharedCode = request.UpstreamProductCode
	}
	result, err := s.adapter.CreateOrder(ctx, upstreamRequest)
	if err != nil {
		if s.connection.Protocol == constants.ConnectionProtocolFaka && errors.Is(err, upstream.ErrFakaTradeNotSent) {
			return nil, fmt.Errorf("%w: %v", procurementcontract.ErrSubmissionNotSent, err)
		}
		if result == nil {
			return nil, err
		}
	}
	response := &procurementcontract.CreateOrderResult{
		OK: result.OK, OrderID: result.OrderID, OrderNo: result.OrderNo,
		Status: result.Status, Amount: result.Amount, Currency: result.Currency,
		ErrorCode: result.ErrorCode, ErrorMessage: result.ErrorMessage,
	}
	return response, err
}

func (s *session) GetOrder(ctx context.Context, orderID uint) (*procurementcontract.UpstreamOrder, error) {
	detail, err := s.adapter.GetOrder(ctx, orderID)
	if err != nil {
		return nil, err
	}
	result := &procurementcontract.UpstreamOrder{
		OrderID: detail.OrderID, OrderNo: detail.OrderNo, Status: detail.Status,
		Amount: detail.Amount, RefundedAmount: detail.RefundedAmount,
		Currency: detail.Currency, RefundRecords: detail.RefundRecords,
	}
	if detail.Fulfillment != nil {
		result.Fulfillment = fromUpstreamFulfillment(detail.Fulfillment)
	}
	return result, nil
}

func (s *session) QueryFakaTrade(ctx context.Context, tradeNo string) (*procurementcontract.UpstreamOrder, error) {
	if s.connection.Protocol != constants.ConnectionProtocolFaka {
		return nil, fmt.Errorf("string trade query requires Faka protocol")
	}
	querier, ok := s.adapter.(interface {
		QueryOrderByTradeNo(context.Context, string) (*upstream.FakaQueryResult, error)
	})
	if !ok {
		return nil, fmt.Errorf("Faka adapter does not support string trade queries")
	}
	result, err := querier.QueryOrderByTradeNo(ctx, tradeNo)
	if err != nil {
		return nil, err
	}
	if result == nil {
		return nil, fmt.Errorf("Faka query returned no order data")
	}
	status := "unpaid"
	switch result.PaymentStatus {
	case 0:
	case 1:
		status = "paid"
	default:
		return nil, fmt.Errorf("Faka query returned an unsupported payment status")
	}
	// Query's secret is free-form text. It must not enter Fulfillment until a
	// supplier-backed delivery discriminator is available.
	return &procurementcontract.UpstreamOrder{OrderNo: tradeNo, Status: status}, nil
}

func (s *session) CancelOrder(ctx context.Context, orderID uint) error {
	err := s.adapter.CancelOrder(ctx, orderID)
	if errors.Is(err, upstream.ErrUpstreamCancelNotAllowed) {
		return fmt.Errorf("%w: %v", procurementcontract.ErrUpstreamCancelRefused, err)
	}
	return err
}

func fromUpstreamFulfillment(value *upstream.UpstreamFulfillment) *procurementcontract.Fulfillment {
	if value == nil {
		return nil
	}
	return &procurementcontract.Fulfillment{
		Type: value.Type, Status: value.Status, Payload: value.Payload,
		DeliveryData: value.DeliveryData, DeliveredAt: value.DeliveredAt,
	}
}

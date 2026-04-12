package fincode

import (
	"context"
	"encoding/json"
	"fmt"
	"strconv"

	"github.com/contract-to-cash/core/domain/payment"
)

// Gateway implements payment.Gateway using the fincode API.
type Gateway struct {
	client *Client
}

var _ payment.Gateway = (*Gateway)(nil)

// NewGateway creates a new fincode payment gateway.
func NewGateway(client *Client) *Gateway {
	return &Gateway{client: client}
}

// Authorize registers a payment with fincode and executes it.
// The fincode flow requires two steps:
//  1. POST /v1/payments — register (creates order with access_id)
//  2. PUT /v1/payments/{id} — execute (charges the card)
func (g *Gateway) Authorize(ctx context.Context, req payment.AuthorizeRequest) (*payment.AuthorizeResponse, error) {
	jobCode := JobCodeAuth
	if req.Capture {
		jobCode = JobCodeCapture
	}

	// Step 1: Register the payment.
	createReq := &CreatePaymentRequest{
		PayType: PayTypeCard,
		JobCode: jobCode,
		Amount:  strconv.FormatInt(req.Amount.Int64(), 10),
	}
	if req.OrderID != "" {
		createReq.ID = req.OrderID
	}

	createResp, err := g.client.CreatePayment(ctx, createReq)
	if err != nil {
		return nil, fmt.Errorf("fincode authorize: create payment: %w", err)
	}

	// Step 2: Execute the payment.
	execReq := &ExecutePaymentRequest{
		PayType:  PayTypeCard,
		AccessID: createResp.AccessID,
		Method:   string(PayMethodLumpSum),
	}
	if req.Token != "" {
		execReq.Token = req.Token
	}
	if req.CustomerID != "" {
		execReq.CustomerID = req.CustomerID
	}
	if req.CardID != "" {
		execReq.CardID = req.CardID
	}

	execResp, err := g.client.ExecutePayment(ctx, createResp.ID, execReq)
	if err != nil {
		return nil, fmt.Errorf("fincode authorize: execute payment: %w", err)
	}

	return &payment.AuthorizeResponse{
		TransactionID: execResp.TransactionID,
		AccessID:      execResp.AccessID,
		OrderID:       execResp.ID,
		Status:        toGatewayStatus(execResp.Status),
		RawResponse:   toRawMap(execResp),
	}, nil
}

// Capture confirms a previously authorized payment.
func (g *Gateway) Capture(ctx context.Context, req payment.CaptureRequest) (*payment.CaptureResponse, error) {
	captureReq := &CapturePaymentRequest{
		PayType:  PayTypeCard,
		AccessID: req.AccessID,
	}

	resp, err := g.client.CapturePayment(ctx, req.OrderID, captureReq)
	if err != nil {
		return nil, fmt.Errorf("fincode capture: %w", err)
	}

	return &payment.CaptureResponse{
		TransactionID: resp.TransactionID,
		OrderID:       resp.ID,
		Status:        toGatewayStatus(resp.Status),
		RawResponse:   toRawMap(resp),
	}, nil
}

// Cancel voids a payment.
func (g *Gateway) Cancel(ctx context.Context, req payment.CancelRequest) (*payment.CancelResponse, error) {
	cancelReq := &CancelPaymentRequest{
		PayType:  PayTypeCard,
		AccessID: req.AccessID,
	}

	resp, err := g.client.CancelPayment(ctx, req.OrderID, cancelReq)
	if err != nil {
		return nil, fmt.Errorf("fincode cancel: %w", err)
	}

	return &payment.CancelResponse{
		OrderID:     resp.ID,
		Status:      toGatewayStatus(resp.Status),
		RawResponse: toRawMap(resp),
	}, nil
}

// Refund cancels a captured payment. In fincode, refund is performed via the cancel endpoint.
func (g *Gateway) Refund(ctx context.Context, req payment.RefundRequest) (*payment.RefundResponse, error) {
	cancelReq := &CancelPaymentRequest{
		PayType:  PayTypeCard,
		AccessID: req.AccessID,
	}

	resp, err := g.client.CancelPayment(ctx, req.OrderID, cancelReq)
	if err != nil {
		return nil, fmt.Errorf("fincode refund: %w", err)
	}

	return &payment.RefundResponse{
		OrderID:     resp.ID,
		Status:      toGatewayStatus(resp.Status),
		RawResponse: toRawMap(resp),
	}, nil
}

func toGatewayStatus(s PaymentStatus) payment.GatewayStatus {
	switch s {
	case StatusUnprocessed:
		return payment.GatewayStatusUnprocessed
	case StatusChecked:
		return payment.GatewayStatusChecked
	case StatusAuthorized:
		return payment.GatewayStatusAuthorized
	case StatusCaptured:
		return payment.GatewayStatusCaptured
	case StatusCanceled:
		return payment.GatewayStatusCanceled
	case StatusAuthenticated:
		return payment.GatewayStatusAuthenticated
	default:
		return payment.GatewayStatus(s)
	}
}

func toRawMap(v any) map[string]any {
	data, _ := json.Marshal(v)
	var m map[string]any
	_ = json.Unmarshal(data, &m)
	return m
}

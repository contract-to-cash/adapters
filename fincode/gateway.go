package fincode

import (
	"context"
	"encoding/json"
	"fmt"
	"net/url"
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
//
// The fincode flow requires two steps:
//  1. POST /v1/payments       — register (creates order with access_id)
//  2. PUT  /v1/payments/{id}  — execute (charges the card)
//
// The two steps are not atomic at the HTTP layer. If step 2 fails after
// step 1 has succeeded, Authorize returns a *PartialAuthorizeError wrapping
// the cause and carrying the registered OrderID and AccessID. Callers
// should persist those IDs and retry by calling ExecuteAuthorize with the
// same inputs; fincode does not bill until step 2 completes.
//
// Idempotency: if req.IdempotencyKey is non-empty, it is forwarded as the
// fincode `idempotent_key` header on step 1. Retries of step 1 within
// fincode's 30-minute TTL return the same registered order rather than
// creating a duplicate. If req.IdempotencyKey is empty, callers should
// supply req.OrderID to get the same effect (fincode rejects duplicate
// IDs, making retries detectable).
//
// Validation: at least one of req.Token or req.CustomerID must be set.
//
// Note: Method is hardcoded to lump-sum ("1"). Installment/revolving
// support requires adding a field to payment.AuthorizeRequest in core.
func (g *Gateway) Authorize(ctx context.Context, req payment.AuthorizeRequest) (*payment.AuthorizeResponse, error) {
	if err := validateAuthorizeRequest(req); err != nil {
		return nil, err
	}

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

	createResp, err := g.client.CreatePayment(ctx, createReq, req.IdempotencyKey)
	if err != nil {
		return nil, fmt.Errorf("fincode authorize: create payment: %w", err)
	}

	// Step 2: Execute the payment. If this fails, the order is registered
	// at fincode but not charged. Wrap the error so the caller can retry.
	execResp, rawBody, err := g.executeAndReturn(ctx, createResp.ID, createResp.AccessID, req)
	if err != nil {
		return nil, &PartialAuthorizeError{
			OrderID:  createResp.ID,
			AccessID: createResp.AccessID,
			Cause:    err,
		}
	}

	return &payment.AuthorizeResponse{
		TransactionID: execResp.TransactionID,
		AccessID:      execResp.AccessID,
		OrderID:       execResp.ID,
		Status:        toGatewayStatus(execResp.Status),
		RawResponse:   rawBody,
	}, nil
}

// ExecuteAuthorize completes a previously registered-but-not-executed
// authorization. Use this to recover from a PartialAuthorizeError returned
// by Authorize: the PartialAuthorizeError carries the OrderID and AccessID
// that should be passed back in here.
//
// The AuthorizeRequest is reused for its Token/CustomerID/CardID; OrderID
// and IdempotencyKey are ignored (the order is already registered).
func (g *Gateway) ExecuteAuthorize(ctx context.Context, orderID, accessID string, req payment.AuthorizeRequest) (*payment.AuthorizeResponse, error) {
	if err := validateAuthorizeRequest(req); err != nil {
		return nil, err
	}
	if orderID == "" {
		return nil, &ValidationError{Field: "orderID", Message: "must not be empty"}
	}
	if accessID == "" {
		return nil, &ValidationError{Field: "accessID", Message: "must not be empty"}
	}

	execResp, rawBody, err := g.executeAndReturn(ctx, orderID, accessID, req)
	if err != nil {
		return nil, fmt.Errorf("fincode execute authorize: %w", err)
	}

	return &payment.AuthorizeResponse{
		TransactionID: execResp.TransactionID,
		AccessID:      execResp.AccessID,
		OrderID:       execResp.ID,
		Status:        toGatewayStatus(execResp.Status),
		RawResponse:   rawBody,
	}, nil
}

// executeAndReturn calls PUT /v1/payments/{id} and returns both the parsed
// response and the raw body bytes (for RawResponse preservation).
func (g *Gateway) executeAndReturn(ctx context.Context, orderID, accessID string, req payment.AuthorizeRequest) (*PaymentResponse, json.RawMessage, error) {
	execReq := &ExecutePaymentRequest{
		PayType:  PayTypeCard,
		AccessID: accessID,
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
	return doJSONRaw[PaymentResponse](g.client, ctx, "PUT", "/v1/payments/"+pathEscape(orderID), execReq)
}

// Capture confirms a previously authorized payment.
func (g *Gateway) Capture(ctx context.Context, req payment.CaptureRequest) (*payment.CaptureResponse, error) {
	captureReq := &CapturePaymentRequest{
		PayType:  PayTypeCard,
		AccessID: req.AccessID,
	}

	resp, rawBody, err := doJSONRaw[PaymentResponse](g.client, ctx, "PUT",
		"/v1/payments/"+pathEscape(req.OrderID)+"/capture", captureReq)
	if err != nil {
		return nil, fmt.Errorf("fincode capture: %w", err)
	}

	return &payment.CaptureResponse{
		TransactionID: resp.TransactionID,
		OrderID:       resp.ID,
		Status:        toGatewayStatus(resp.Status),
		RawResponse:   rawBody,
	}, nil
}

// Cancel voids a payment via PUT /v1/payments/{id}/cancel.
//
// Semantics vary by prior status:
//   - AUTHORIZED → CANCELED (authorization voided, credit line released)
//   - CAPTURED   → CANCELED (full reversal attempted; may not complete
//     after the acquirer's settlement cutoff depending on card scheme)
//
// Use Refund (which routes to /change for partial amounts) when you want
// to refund less than the full captured amount.
func (g *Gateway) Cancel(ctx context.Context, req payment.CancelRequest) (*payment.CancelResponse, error) {
	cancelReq := &CancelPaymentRequest{
		PayType:  PayTypeCard,
		AccessID: req.AccessID,
	}

	resp, rawBody, err := doJSONRaw[PaymentResponse](g.client, ctx, "PUT",
		"/v1/payments/"+pathEscape(req.OrderID)+"/cancel", cancelReq)
	if err != nil {
		return nil, fmt.Errorf("fincode cancel: %w", err)
	}

	return &payment.CancelResponse{
		OrderID:     resp.ID,
		Status:      toGatewayStatus(resp.Status),
		RawResponse: rawBody,
	}, nil
}

// Refund refunds a captured payment.
//
// fincode does not have a dedicated refund endpoint; the behavior depends
// on the amount being refunded:
//
//   - Full refund  (req.Amount == current total) → PUT /v1/payments/{id}/cancel
//   - Partial refund (req.Amount < current total)  → PUT /v1/payments/{id}/change
//     with a new amount = (current total - req.Amount)
//
// To determine the current total, Refund first issues GET /v1/payments/{id}.
// req.Amount must be positive and must not exceed the current total.
//
// Callers wanting the previous "always cancel" behavior should call Cancel
// directly.
func (g *Gateway) Refund(ctx context.Context, req payment.RefundRequest) (*payment.RefundResponse, error) {
	if req.OrderID == "" {
		return nil, &ValidationError{Field: "OrderID", Message: "must not be empty"}
	}
	if req.AccessID == "" {
		return nil, &ValidationError{Field: "AccessID", Message: "must not be empty"}
	}
	refundAmount := req.Amount.Int64()
	if refundAmount <= 0 {
		return nil, &ValidationError{Field: "Amount", Message: "must be positive"}
	}

	current, err := g.client.RetrievePayment(ctx, req.OrderID, PayTypeCard)
	if err != nil {
		return nil, fmt.Errorf("fincode refund: retrieve current payment: %w", err)
	}
	currentTotal := current.TotalAmount
	if currentTotal == 0 {
		// Fall back to Amount field if TotalAmount isn't populated.
		currentTotal = current.Amount
	}

	if refundAmount > currentTotal {
		return nil, &ValidationError{
			Field:   "Amount",
			Message: fmt.Sprintf("refund amount %d exceeds current total %d", refundAmount, currentTotal),
		}
	}

	// Full refund: use /cancel.
	if refundAmount == currentTotal {
		cancelReq := &CancelPaymentRequest{
			PayType:  PayTypeCard,
			AccessID: req.AccessID,
		}
		resp, rawBody, err := doJSONRaw[PaymentResponse](g.client, ctx, "PUT",
			"/v1/payments/"+pathEscape(req.OrderID)+"/cancel", cancelReq)
		if err != nil {
			return nil, fmt.Errorf("fincode refund (full): %w", err)
		}
		return &payment.RefundResponse{
			OrderID:     resp.ID,
			Status:      toGatewayStatus(resp.Status),
			RawResponse: rawBody,
		}, nil
	}

	// Partial refund: use /change to lower the amount.
	newAmount := currentTotal - refundAmount
	changeReq := &ChangeAmountRequest{
		PayType:  PayTypeCard,
		AccessID: req.AccessID,
		JobCode:  JobCodeCapture,
		Amount:   strconv.FormatInt(newAmount, 10),
	}
	resp, rawBody, err := doJSONRaw[PaymentResponse](g.client, ctx, "PUT",
		"/v1/payments/"+pathEscape(req.OrderID)+"/change", changeReq)
	if err != nil {
		return nil, fmt.Errorf("fincode refund (partial): %w", err)
	}
	return &payment.RefundResponse{
		OrderID:     resp.ID,
		Status:      toGatewayStatus(resp.Status),
		RawResponse: rawBody,
	}, nil
}

func validateAuthorizeRequest(req payment.AuthorizeRequest) error {
	if req.Token == "" && req.CustomerID == "" {
		return &ValidationError{
			Field:   "Token/CustomerID",
			Message: "at least one of Token or CustomerID must be provided",
		}
	}
	return nil
}

// pathEscape escapes an orderID (or other path segment) using net/url
// semantics so that slashes, question marks, etc., in caller-provided IDs
// don't re-route the URL.
func pathEscape(s string) string {
	return url.PathEscape(s)
}

// toGatewayStatus maps a fincode PaymentStatus to the core GatewayStatus.
// Unknown statuses are passed through as-is so that callers can observe
// future fincode additions without the adapter blocking them.
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

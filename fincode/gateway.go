package fincode

import (
	"context"
	"crypto/sha256"
	"encoding/base32"
	"errors"
	"fmt"
	"math/big"
	"net/http"
	"strconv"
	"strings"
	"time"

	"github.com/contract-to-cash/core/application/port"
	"github.com/contract-to-cash/core/domain/shared"
)

// GatewayID is the identifier returned by Gateway.ID().
const GatewayID = "fincode"

// Gateway implements port.PaymentGateway using the fincode API.
//
// ID mapping conventions:
//
//   - port TransactionID / AuthorizationID == the fincode payment (order) ID.
//     fincode mutating endpoints additionally require the payment's access_id;
//     the gateway re-fetches it with GET /v1/payments/{id} before each
//     mutating call, so callers only ever store the single order ID.
//   - port payment method IDs are composite: "<customer_id>/<card_id>".
//     fincode scopes cards under customers, but the port API passes a single
//     flat ID to Get/DeletePaymentMethod. The card ID is fincode-generated and
//     never contains "/", so the composite is split at the LAST "/".
//
// Currency: fincode processes JPY only. Requests in any other currency fail
// with a *port.GatewayError (ErrorCodeCurrencyNotSupported).
type Gateway struct {
	client *Client
	clock  shared.Clock
}

var _ port.PaymentGateway = (*Gateway)(nil)

// GatewayOption configures the Gateway.
type GatewayOption func(*Gateway)

// WithClock sets the clock used for fallback timestamps when a fincode
// response carries no parseable time. Defaults to shared.SystemClock.
func WithClock(clock shared.Clock) GatewayOption {
	return func(g *Gateway) { g.clock = clock }
}

// NewGateway creates a new fincode payment gateway.
func NewGateway(client *Client, opts ...GatewayOption) *Gateway {
	g := &Gateway{
		client: client,
		clock:  shared.SystemClock{},
	}
	for _, opt := range opts {
		opt(g)
	}
	return g
}

// ID returns the gateway identifier.
func (g *Gateway) ID() string { return GatewayID }

// SupportedMethods returns the payment method types this gateway supports.
// Only card payments are implemented by this adapter.
func (g *Gateway) SupportedMethods() []port.PaymentMethodType {
	return []port.PaymentMethodType{port.PaymentMethodTypeCreditCard}
}

// --- Charge / Authorize ---

// Charge performs a one-step charge (fincode job_code=CAPTURE).
//
// The fincode flow requires two HTTP steps:
//  1. POST /v1/payments       — register (creates order with access_id)
//  2. PUT  /v1/payments/{id}  — execute (charges the card)
//
// The two steps are not atomic. If step 2 fails after step 1 succeeded,
// Charge returns a *PartialAuthorizeError carrying the registered OrderID and
// AccessID; retry via CompleteCharge with the same values. fincode does not
// bill until step 2 completes.
//
// Idempotency: when req.IdempotencyKey is set, the fincode order ID for
// step 1 is derived deterministically from it (see deriveOrderID), so a
// retried Charge re-registers the SAME order and fincode rejects the
// duplicate instead of double-charging — this holds permanently, satisfying
// the core PaymentService contract that the gateway deduplicates on
// IdempotencyKey. The key is additionally forwarded as the fincode
// `idempotent_key` header (30-minute TTL per fincode docs) so short-window
// retries get the original response back rather than an error. An empty
// IdempotencyKey lets fincode assign the order ID.
//
// Payment source: exactly one of req.Token (fincode.js token) or a stored
// card is required. A stored card is addressed either by req.PaymentMethodID
// (composite "<customer_id>/<card_id>") or by req.CustomerID alone (fincode
// then uses the customer's default card).
//
// Not supported: req.ThreeDSecure (returns method_not_supported) and
// req.Metadata / StatementDescriptor (fincode has no equivalent; Description
// is forwarded as client_field_1).
func (g *Gateway) Charge(ctx context.Context, req *port.ChargeRequest) (*port.ChargeResponse, error) {
	if req == nil {
		return nil, &ValidationError{Field: "req", Message: "must not be nil"}
	}
	if req.ThreeDSecure != nil && req.ThreeDSecure.Required {
		return nil, unsupported3DSError()
	}
	src, err := resolvePaymentSource(req.Token, req.PaymentMethodID, req.CustomerID)
	if err != nil {
		return nil, err
	}
	amount, err := jpyAmount("Amount", req.Amount)
	if err != nil {
		return nil, err
	}

	resp, err := g.registerAndExecute(ctx, JobCodeCapture, amount, req.Description, req.IdempotencyKey, src)
	if err != nil {
		return nil, err
	}
	return g.toChargeResponse(resp), nil
}

// CompleteCharge retries the execute step of a Charge that failed with
// *PartialAuthorizeError. orderID and accessID come from that error; req is
// reused for its payment source (Token / PaymentMethodID / CustomerID).
//
// Before re-executing, the payment's current state is fetched: if the earlier
// execute actually succeeded but its response was lost (timeout, connection
// reset), the payment is already CAPTURED and re-executing would fail —
// turning a successful charge into a reported failure. In that case the
// current state is converted into a success response instead.
func (g *Gateway) CompleteCharge(ctx context.Context, orderID, accessID string, req *port.ChargeRequest) (*port.ChargeResponse, error) {
	if req == nil {
		return nil, &ValidationError{Field: "req", Message: "must not be nil"}
	}
	src, err := resolvePaymentSource(req.Token, req.PaymentMethodID, req.CustomerID)
	if err != nil {
		return nil, err
	}
	current, err := g.retrieve(ctx, orderID)
	if err != nil {
		return nil, err
	}
	if current.Status == StatusCaptured {
		return g.toChargeResponse(current), nil
	}
	resp, err := g.executeRegistered(ctx, orderID, accessID, src)
	if err != nil {
		return nil, err
	}
	return g.toChargeResponse(resp), nil
}

// Authorize places a hold on funds without capturing (fincode job_code=AUTH).
// Same two-step flow, partial-failure recovery (CompleteAuthorize), and
// idempotency semantics as Charge.
//
// req.ExpiresIn is not supported: the authorization window is fixed by
// fincode (auth_max_date is reported back in ExpiresAt when parseable).
func (g *Gateway) Authorize(ctx context.Context, req *port.AuthorizeRequest) (*port.AuthorizeResponse, error) {
	if req == nil {
		return nil, &ValidationError{Field: "req", Message: "must not be nil"}
	}
	if req.ThreeDSecure != nil && req.ThreeDSecure.Required {
		return nil, unsupported3DSError()
	}
	src, err := resolvePaymentSource(req.Token, req.PaymentMethodID, req.CustomerID)
	if err != nil {
		return nil, err
	}
	amount, err := jpyAmount("Amount", req.Amount)
	if err != nil {
		return nil, err
	}

	resp, err := g.registerAndExecute(ctx, JobCodeAuth, amount, "", req.IdempotencyKey, src)
	if err != nil {
		return nil, err
	}
	return g.toAuthorizeResponse(resp), nil
}

// CompleteAuthorize retries the execute step of an Authorize that failed with
// *PartialAuthorizeError. See CompleteCharge — the same lost-response
// recovery applies: an already-AUTHORIZED payment is converted into a success
// response without re-executing.
func (g *Gateway) CompleteAuthorize(ctx context.Context, orderID, accessID string, req *port.AuthorizeRequest) (*port.AuthorizeResponse, error) {
	if req == nil {
		return nil, &ValidationError{Field: "req", Message: "must not be nil"}
	}
	src, err := resolvePaymentSource(req.Token, req.PaymentMethodID, req.CustomerID)
	if err != nil {
		return nil, err
	}
	current, err := g.retrieve(ctx, orderID)
	if err != nil {
		return nil, err
	}
	if current.Status == StatusAuthorized {
		return g.toAuthorizeResponse(current), nil
	}
	resp, err := g.executeRegistered(ctx, orderID, accessID, src)
	if err != nil {
		return nil, err
	}
	return g.toAuthorizeResponse(resp), nil
}

// registerAndExecute performs the two-step fincode payment flow.
func (g *Gateway) registerAndExecute(ctx context.Context, jobCode JobCode, amount int64, description, idempotencyKey string, src paymentSource) (*PaymentResponse, error) {
	createReq := &CreatePaymentRequest{
		ID:           deriveOrderID(idempotencyKey),
		PayType:      PayTypeCard,
		JobCode:      jobCode,
		Amount:       strconv.FormatInt(amount, 10),
		ClientField1: description,
	}
	createResp, err := g.client.CreatePayment(ctx, createReq, idempotencyKey)
	if err != nil {
		// Register failure: no order exists at fincode, plain gateway error.
		return nil, g.wrapGatewayError("register payment", err)
	}

	execResp, err := g.executeRegistered(ctx, createResp.ID, createResp.AccessID, src)
	if err != nil {
		// Execute failure: the order IS registered. Surface the IDs so the
		// caller can retry via CompleteCharge / CompleteAuthorize.
		return nil, &PartialAuthorizeError{
			OrderID:  createResp.ID,
			AccessID: createResp.AccessID,
			Cause:    err,
		}
	}
	return execResp, nil
}

// executeRegistered executes a previously registered payment.
func (g *Gateway) executeRegistered(ctx context.Context, orderID, accessID string, src paymentSource) (*PaymentResponse, error) {
	if orderID == "" {
		return nil, &ValidationError{Field: "orderID", Message: "must not be empty"}
	}
	if accessID == "" {
		return nil, &ValidationError{Field: "accessID", Message: "must not be empty"}
	}
	execReq := &ExecutePaymentRequest{
		PayType:    PayTypeCard,
		AccessID:   accessID,
		Method:     string(PayMethodLumpSum),
		Token:      src.token,
		CustomerID: src.customerID,
		CardID:     src.cardID,
	}
	resp, err := g.client.ExecutePayment(ctx, orderID, execReq)
	if err != nil {
		return nil, g.wrapGatewayError("execute payment", err)
	}
	return resp, nil
}

// --- Capture / Void / Cancel / Refund ---

// Capture captures a previously authorized transaction.
// req.AuthorizationID is the fincode payment (order) ID.
//
// If req.Amount is set and differs from the authorized total, the amount is
// first adjusted with PUT /v1/payments/{id}/change (job_code=AUTH) and then
// captured. A nil Amount captures the full authorized amount.
func (g *Gateway) Capture(ctx context.Context, req *port.CaptureRequest) (*port.CaptureResponse, error) {
	if req == nil {
		return nil, &ValidationError{Field: "req", Message: "must not be nil"}
	}
	current, err := g.retrieve(ctx, req.AuthorizationID)
	if err != nil {
		return nil, err
	}

	if req.Amount != nil {
		amount, err := jpyAmount("Amount", *req.Amount)
		if err != nil {
			return nil, err
		}
		if amount != currentTotal(current) {
			if _, err := g.client.ChangeAmount(ctx, req.AuthorizationID, &ChangeAmountRequest{
				PayType:  PayTypeCard,
				AccessID: current.AccessID,
				JobCode:  JobCodeAuth,
				Amount:   strconv.FormatInt(amount, 10),
			}, WithIdempotencyKey(req.IdempotencyKey)); err != nil {
				return nil, g.wrapGatewayError("change amount before capture", err)
			}
		}
	}

	resp, err := g.client.CapturePayment(ctx, req.AuthorizationID, &CapturePaymentRequest{
		PayType:  PayTypeCard,
		AccessID: current.AccessID,
	}, WithIdempotencyKey(req.IdempotencyKey))
	if err != nil {
		return nil, g.wrapGatewayError("capture payment", err)
	}

	return &port.CaptureResponse{
		TransactionID:   resp.ID,
		AuthorizationID: resp.ID,
		Status:          toTransactionStatus(resp.Status),
		Amount:          jpyMoney(currentTotal(resp)),
		CapturedAt:      g.timeOrNow(resp.Updated),
	}, nil
}

// Void cancels an authorized transaction before capture
// (PUT /v1/payments/{id}/cancel). req.AuthorizationID is the fincode payment
// (order) ID. Voiding a payment that has already been captured is rejected;
// use Refund instead.
func (g *Gateway) Void(ctx context.Context, req *port.VoidRequest) (*port.VoidResponse, error) {
	if req == nil {
		return nil, &ValidationError{Field: "req", Message: "must not be nil"}
	}
	current, err := g.retrieve(ctx, req.AuthorizationID)
	if err != nil {
		return nil, err
	}
	if current.Status == StatusCaptured {
		return nil, &port.GatewayError{
			Code:    port.ErrorCodeProcessingError,
			Message: fmt.Sprintf("payment %s is already captured; use Refund instead of Void", req.AuthorizationID),
		}
	}

	resp, err := g.client.CancelPayment(ctx, req.AuthorizationID, &CancelPaymentRequest{
		PayType:  PayTypeCard,
		AccessID: current.AccessID,
	}, WithIdempotencyKey(req.IdempotencyKey))
	if err != nil {
		return nil, g.wrapGatewayError("void payment", err)
	}
	return &port.VoidResponse{
		AuthorizationID: resp.ID,
		Status:          toTransactionStatus(resp.Status),
		VoidedAt:        g.timeOrNow(resp.Updated),
	}, nil
}

// Cancel cancels a pending transaction (PUT /v1/payments/{id}/cancel).
// req.TransactionID is the fincode payment (order) ID. For AUTHORIZED
// payments this voids the authorization; for CAPTURED card payments fincode
// attempts a reversal, which may not complete after the acquirer's
// settlement cutoff.
func (g *Gateway) Cancel(ctx context.Context, req *port.CancelRequest) (*port.CancelResponse, error) {
	if req == nil {
		return nil, &ValidationError{Field: "req", Message: "must not be nil"}
	}
	current, err := g.retrieve(ctx, req.TransactionID)
	if err != nil {
		return nil, err
	}
	resp, err := g.client.CancelPayment(ctx, req.TransactionID, &CancelPaymentRequest{
		PayType:  PayTypeCard,
		AccessID: current.AccessID,
	}, WithIdempotencyKey(req.IdempotencyKey))
	if err != nil {
		return nil, g.wrapGatewayError("cancel payment", err)
	}
	return &port.CancelResponse{
		TransactionID: resp.ID,
		Status:        toTransactionStatus(resp.Status),
		CanceledAt:    g.timeOrNow(resp.Updated),
	}, nil
}

// Refund refunds a captured transaction. req.TransactionID is the fincode
// payment (order) ID; a nil req.Amount means full refund.
//
// fincode has no dedicated refund endpoint or refund resource:
//
//   - full refund    → PUT /v1/payments/{id}/cancel
//   - partial refund → PUT /v1/payments/{id}/change with
//     new total = current total - refund amount
//
// Because there is no fincode refund object, RefundResponse.RefundID is the
// payment (order) ID.
//
// Concurrency: partial refunds are a read-modify-write (retrieve current
// total, then /change to total-refund) and fincode offers no compare-and-set,
// so the adapter cannot detect a concurrent modification. Callers MUST
// serialize refund operations against the same payment; running them
// concurrently can lose a refund (last write wins on the new total).
func (g *Gateway) Refund(ctx context.Context, req *port.RefundRequest) (*port.RefundResponse, error) {
	if req == nil {
		return nil, &ValidationError{Field: "req", Message: "must not be nil"}
	}
	current, err := g.retrieve(ctx, req.TransactionID)
	if err != nil {
		return nil, err
	}
	total := currentTotal(current)

	refundAmount := total
	if req.Amount != nil {
		refundAmount, err = jpyAmount("Amount", *req.Amount)
		if err != nil {
			return nil, err
		}
	}
	if refundAmount > total {
		return nil, &ValidationError{
			Field:   "Amount",
			Message: fmt.Sprintf("refund amount %d exceeds current total %d", refundAmount, total),
		}
	}

	var resp *PaymentResponse
	if refundAmount == total {
		resp, err = g.client.CancelPayment(ctx, req.TransactionID, &CancelPaymentRequest{
			PayType:  PayTypeCard,
			AccessID: current.AccessID,
		}, WithIdempotencyKey(req.IdempotencyKey))
		if err != nil {
			return nil, g.wrapGatewayError("refund (full, via cancel)", err)
		}
	} else {
		resp, err = g.client.ChangeAmount(ctx, req.TransactionID, &ChangeAmountRequest{
			PayType:  PayTypeCard,
			AccessID: current.AccessID,
			JobCode:  JobCodeCapture,
			Amount:   strconv.FormatInt(total-refundAmount, 10),
		}, WithIdempotencyKey(req.IdempotencyKey))
		if err != nil {
			return nil, g.wrapGatewayError("refund (partial, via change)", err)
		}
	}

	return &port.RefundResponse{
		RefundID:      resp.ID, // fincode has no separate refund resource
		TransactionID: resp.ID,
		Status:        port.RefundStatusSucceeded,
		Amount:        jpyMoney(refundAmount),
		Reason:        req.Reason,
		RefundedAt:    g.timeOrNow(resp.Updated),
	}, nil
}

// GetTransaction retrieves a transaction by its fincode payment (order) ID.
func (g *Gateway) GetTransaction(ctx context.Context, transactionID string) (*port.Transaction, error) {
	resp, err := g.retrieve(ctx, transactionID)
	if err != nil {
		return nil, err
	}

	txn := &port.Transaction{
		ID:         resp.ID,
		GatewayID:  GatewayID,
		Type:       toTransactionType(resp.JobCode),
		Status:     toTransactionStatus(resp.Status),
		Amount:     jpyMoney(currentTotal(resp)),
		CustomerID: resp.CustomerID,
		Metadata: map[string]string{
			"fincode_status":         string(resp.Status),
			"fincode_access_id":      resp.AccessID,
			"fincode_transaction_id": resp.TransactionID,
		},
		CreatedAt: g.timeOrNow(resp.Created),
		UpdatedAt: g.timeOrNow(resp.Updated),
	}
	if resp.CustomerID != "" && resp.CardID != "" {
		txn.PaymentMethodID = joinPaymentMethodID(resp.CustomerID, resp.CardID)
	}
	if resp.Status == StatusAuthorized || resp.Status == StatusCaptured || resp.Status == StatusCanceled {
		id := resp.ID
		txn.AuthorizationID = &id
	}
	return txn, nil
}

// retrieve fetches the current payment state (including access_id).
func (g *Gateway) retrieve(ctx context.Context, orderID string) (*PaymentResponse, error) {
	if orderID == "" {
		return nil, &ValidationError{Field: "transactionID", Message: "must not be empty"}
	}
	resp, err := g.client.RetrievePayment(ctx, orderID, PayTypeCard)
	if err != nil {
		return nil, g.wrapGatewayError("retrieve payment", err)
	}
	return resp, nil
}

// --- Payment methods (fincode customer cards) ---

// RegisterPaymentMethod stores a tokenized card on a fincode customer
// (POST /v1/customers/{customer_id}/cards). The customer must already exist
// at fincode. Only PaymentMethodTypeCreditCard is supported. The returned
// PaymentMethodDetail.ID is the composite "<customer_id>/<card_id>".
func (g *Gateway) RegisterPaymentMethod(ctx context.Context, req *port.RegisterPaymentMethodRequest) (*port.PaymentMethodDetail, error) {
	if req == nil {
		return nil, &ValidationError{Field: "req", Message: "must not be nil"}
	}
	if req.Type != port.PaymentMethodTypeCreditCard {
		return nil, &port.GatewayError{
			Code:    port.ErrorCodeMethodNotSupported,
			Message: fmt.Sprintf("fincode adapter supports credit_card only, got %q", req.Type),
		}
	}
	if req.CustomerID == "" {
		return nil, &ValidationError{Field: "CustomerID", Message: "must not be empty"}
	}
	if req.Token == "" {
		return nil, &ValidationError{Field: "Token", Message: "must not be empty"}
	}

	defaultFlag := "0"
	if req.SetAsDefault {
		defaultFlag = "1"
	}
	card, err := g.client.CreateCard(ctx, req.CustomerID, &CreateCardRequest{
		Token:       req.Token,
		DefaultFlag: defaultFlag,
	})
	if err != nil {
		return nil, g.wrapGatewayError("register card", err)
	}
	return g.toPaymentMethodDetail(card), nil
}

// DeletePaymentMethod removes a stored card. paymentMethodID is the
// composite "<customer_id>/<card_id>" returned by RegisterPaymentMethod.
func (g *Gateway) DeletePaymentMethod(ctx context.Context, paymentMethodID string) error {
	customerID, cardID, err := splitPaymentMethodID(paymentMethodID)
	if err != nil {
		return err
	}
	if _, err := g.client.DeleteCard(ctx, customerID, cardID); err != nil {
		return g.wrapGatewayError("delete card", err)
	}
	return nil
}

// GetPaymentMethod retrieves a stored card. paymentMethodID is the composite
// "<customer_id>/<card_id>".
func (g *Gateway) GetPaymentMethod(ctx context.Context, paymentMethodID string) (*port.PaymentMethodDetail, error) {
	customerID, cardID, err := splitPaymentMethodID(paymentMethodID)
	if err != nil {
		return nil, err
	}
	card, err := g.client.RetrieveCard(ctx, customerID, cardID)
	if err != nil {
		return nil, g.wrapGatewayError("retrieve card", err)
	}
	return g.toPaymentMethodDetail(card), nil
}

// ListPaymentMethods lists all stored cards for a fincode customer.
func (g *Gateway) ListPaymentMethods(ctx context.Context, customerID string) ([]*port.PaymentMethodDetail, error) {
	if customerID == "" {
		return nil, &ValidationError{Field: "customerID", Message: "must not be empty"}
	}
	resp, err := g.client.ListCards(ctx, customerID)
	if err != nil {
		return nil, g.wrapGatewayError("list cards", err)
	}
	details := make([]*port.PaymentMethodDetail, 0, len(resp.List))
	for i := range resp.List {
		details = append(details, g.toPaymentMethodDetail(&resp.List[i]))
	}
	return details, nil
}

// --- Payment source resolution ---

// paymentSource is the resolved card addressing for a fincode execute call.
type paymentSource struct {
	token      string
	customerID string
	cardID     string
}

// resolvePaymentSource maps the port's (Token, PaymentMethodID, CustomerID)
// triple onto fincode's (token | customer_id [+ card_id]) addressing.
func resolvePaymentSource(token *string, paymentMethodID *string, customerID string) (paymentSource, error) {
	if token != nil && *token != "" {
		return paymentSource{token: *token}, nil
	}
	if paymentMethodID != nil && *paymentMethodID != "" {
		pmCustomerID, cardID, err := splitPaymentMethodID(*paymentMethodID)
		if err != nil {
			return paymentSource{}, err
		}
		if customerID == "" {
			customerID = pmCustomerID
		}
		return paymentSource{customerID: customerID, cardID: cardID}, nil
	}
	if customerID != "" {
		// fincode charges the customer's default card.
		return paymentSource{customerID: customerID}, nil
	}
	return paymentSource{}, &ValidationError{
		Field:   "Token/PaymentMethodID/CustomerID",
		Message: "one of Token, PaymentMethodID, or CustomerID must be provided",
	}
}

// deriveOrderID deterministically maps a caller-supplied idempotency key onto
// a fincode order ID, giving permanent idempotency: a retried request
// registers the SAME order ID and fincode rejects the duplicate, so the
// header-based `idempotent_key` (30-minute TTL) is not the only guard against
// double charging. Returns "" (fincode assigns the ID) when the key is empty.
//
// Derivation: "o" + first 29 characters of lowercase unpadded base32 of
// SHA-256(key), 30 characters total. fincode's exact order-ID constraints are
// not confirmed from primary sources, so the format stays conservatively
// within [0-9a-z] and 30 characters; 29 base32 chars carry 145 bits of the
// hash, making collisions between distinct keys practically impossible.
func deriveOrderID(idempotencyKey string) string {
	if idempotencyKey == "" {
		return ""
	}
	sum := sha256.Sum256([]byte(idempotencyKey))
	enc := base32.StdEncoding.WithPadding(base32.NoPadding).EncodeToString(sum[:])
	return "o" + strings.ToLower(enc)[:29]
}

// joinPaymentMethodID builds the composite port payment method ID.
func joinPaymentMethodID(customerID, cardID string) string {
	return customerID + "/" + cardID
}

// splitPaymentMethodID splits a composite "<customer_id>/<card_id>" at the
// LAST slash (fincode card IDs are gateway-generated and never contain "/";
// merchant-chosen customer IDs might).
func splitPaymentMethodID(id string) (customerID, cardID string, err error) {
	i := strings.LastIndex(id, "/")
	if i <= 0 || i == len(id)-1 {
		return "", "", &ValidationError{
			Field:   "paymentMethodID",
			Message: fmt.Sprintf("must be \"<customer_id>/<card_id>\", got %q", id),
		}
	}
	return id[:i], id[i+1:], nil
}

// --- Response mapping ---

func (g *Gateway) toChargeResponse(resp *PaymentResponse) *port.ChargeResponse {
	out := &port.ChargeResponse{
		TransactionID:     resp.ID,
		Status:            toTransactionStatus(resp.Status),
		Amount:            jpyMoney(currentTotal(resp)),
		PaymentMethodType: port.PaymentMethodTypeCreditCard,
		CreatedAt:         g.timeOrNow(resp.Created),
		Metadata: map[string]string{
			"fincode_access_id": resp.AccessID,
		},
	}
	if resp.CustomerID != "" && resp.CardID != "" {
		out.PaymentMethodID = joinPaymentMethodID(resp.CustomerID, resp.CardID)
	}
	return out
}

func (g *Gateway) toAuthorizeResponse(resp *PaymentResponse) *port.AuthorizeResponse {
	out := &port.AuthorizeResponse{
		AuthorizationID: resp.ID,
		TransactionID:   resp.ID,
		Status:          toTransactionStatus(resp.Status),
		Amount:          jpyMoney(currentTotal(resp)),
		CreatedAt:       g.timeOrNow(resp.Created),
		Metadata: map[string]string{
			"fincode_access_id": resp.AccessID,
		},
	}
	if t, ok := parseFincodeTime(resp.AuthMaxDate); ok {
		out.ExpiresAt = &t
	}
	return out
}

func (g *Gateway) toPaymentMethodDetail(card *CardResponse) *port.PaymentMethodDetail {
	return &port.PaymentMethodDetail{
		ID:         joinPaymentMethodID(card.CustomerID, card.ID),
		CustomerID: card.CustomerID,
		Type:       port.PaymentMethodTypeCreditCard,
		IsDefault:  card.DefaultFlag == "1",
		CreatedAt:  g.timeOrNow(card.Created),
		Card:       toCardDetails(card),
	}
}

func toCardDetails(card *CardResponse) *port.CardDetails {
	d := &port.CardDetails{
		Brand: toCardBrand(card.Brand),
	}
	if n := card.CardNo; len(n) >= 4 {
		d.Last4 = n[len(n)-4:]
	}
	// Expire is "yymm".
	if len(card.Expire) == 4 {
		if yy, err := strconv.Atoi(card.Expire[:2]); err == nil {
			d.ExpYear = 2000 + yy
		}
		if mm, err := strconv.Atoi(card.Expire[2:]); err == nil {
			d.ExpMonth = mm
		}
	}
	return d
}

func toCardBrand(brand string) port.CardBrand {
	switch strings.ToUpper(brand) {
	case "VISA":
		return port.CardBrandVisa
	case "MASTER", "MASTERCARD":
		return port.CardBrandMastercard
	case "AMEX", "AMERICANEXPRESS":
		return port.CardBrandAmex
	case "JCB":
		return port.CardBrandJCB
	case "DINERS":
		return port.CardBrandDiners
	case "DISCOVER":
		return port.CardBrandDiscover
	default:
		return port.CardBrandUnknown
	}
}

// toTransactionStatus maps a fincode payment status to the port status.
func toTransactionStatus(s PaymentStatus) port.TransactionStatus {
	switch s {
	case StatusUnprocessed, StatusChecked:
		return port.TransactionStatusPending
	case StatusAuthenticated:
		return port.TransactionStatusRequiresAction
	case StatusAuthorized:
		return port.TransactionStatusAuthorized
	case StatusCaptured:
		return port.TransactionStatusCaptured
	case StatusCanceled:
		return port.TransactionStatusCanceled
	default:
		// Unknown / future fincode statuses are reported as pending; the raw
		// value is preserved in Metadata["fincode_status"] where applicable.
		return port.TransactionStatusPending
	}
}

func toTransactionType(jc JobCode) port.TransactionType {
	switch jc {
	case JobCodeAuth:
		return port.TransactionTypeAuthorize
	default:
		return port.TransactionTypeCharge
	}
}

// currentTotal returns the effective total of a payment; some fincode
// responses populate total_amount, others only amount.
func currentTotal(resp *PaymentResponse) int64 {
	if resp.TotalAmount != 0 {
		return resp.TotalAmount
	}
	return resp.Amount
}

// --- Amount / time helpers ---

// jpyMoney builds a shared.Money in JPY from a fincode integer amount.
func jpyMoney(amount int64) shared.Money {
	return shared.NewMoney(new(big.Rat).SetInt64(amount), shared.CurrencyJPY)
}

// jpyAmount validates a shared.Money for use with fincode: JPY, positive,
// integral, and within int64 range.
func jpyAmount(field string, m shared.Money) (int64, error) {
	if m.Currency() != shared.CurrencyJPY {
		return 0, &port.GatewayError{
			Code:    port.ErrorCodeCurrencyNotSupported,
			Message: fmt.Sprintf("fincode supports JPY only, got %q", m.Currency()),
		}
	}
	amt := m.Amount()
	if amt == nil || !amt.IsInt() {
		return 0, &ValidationError{Field: field, Message: "JPY amount must be an integer"}
	}
	// amt is a normalized big.Rat with denominator 1 here, so the numerator IS
	// the integer value. Range-check it before converting: big.Int.Int64 (and
	// therefore shared.Money.Int64) silently wraps values outside int64,
	// which would turn e.g. 2^64+100 yen into a 100 yen charge.
	num := amt.Num()
	if !num.IsInt64() {
		return 0, &ValidationError{Field: field, Message: "JPY amount exceeds int64 range"}
	}
	v := num.Int64()
	if v <= 0 {
		return 0, &ValidationError{Field: field, Message: "must be positive"}
	}
	return v, nil
}

// jstZone is the fixed JST offset. fincode is a Japanese service and its
// documented timestamp examples are in Japanese local time without an
// explicit offset; a fixed zone avoids a tzdata dependency.
var jstZone = time.FixedZone("JST", 9*60*60)

// fincodeTimeLayouts are the observed/assumed fincode timestamp formats.
var fincodeTimeLayouts = []string{
	"2006/01/02 15:04:05.000",
	"2006/01/02 15:04:05",
	"2006/01/02",
}

// parseFincodeTime parses a fincode timestamp (assumed JST) into UTC.
func parseFincodeTime(s string) (time.Time, bool) {
	if s == "" {
		return time.Time{}, false
	}
	for _, layout := range fincodeTimeLayouts {
		if t, err := time.ParseInLocation(layout, s, jstZone); err == nil {
			return t.UTC(), true
		}
	}
	return time.Time{}, false
}

// timeOrNow parses a fincode timestamp, falling back to the gateway clock.
func (g *Gateway) timeOrNow(s string) time.Time {
	if t, ok := parseFincodeTime(s); ok {
		return t
	}
	return g.clock.Now()
}

// --- Error mapping ---

// wrapGatewayError converts client-level errors (*HTTPError, network errors)
// into *port.GatewayError. The original error chain is preserved via
// RawError, so callers can still errors.As into *HTTPError / *ErrorResponse
// for fincode-specific inspection.
func (g *Gateway) wrapGatewayError(op string, err error) error {
	if err == nil {
		return nil
	}
	ge := &port.GatewayError{
		Code:     port.ErrorCodeProcessingError,
		Message:  fmt.Sprintf("fincode: %s failed", op),
		RawError: err,
	}

	var he *HTTPError
	if errors.As(err, &he) {
		switch {
		case he.StatusCode == http.StatusTooManyRequests:
			ge.Code = port.ErrorCodeRateLimitExceeded
			ge.Retryable = true
		case he.StatusCode == http.StatusRequestTimeout || he.StatusCode == http.StatusGatewayTimeout:
			ge.Code = port.ErrorCodeGatewayTimeout
			ge.Retryable = true
		case he.StatusCode >= 500:
			ge.Code = port.ErrorCodeGatewayUnavailable
			ge.Retryable = true
		}
		if he.APIError != nil && len(he.APIError.Errors) > 0 {
			// Preserve the fincode-specific error code for dispatching;
			// fincode publishes hundreds of codes, so no table mapping is
			// attempted here.
			ge.DeclineCode = he.APIError.Errors[0].ErrorCode
		}
		return ge
	}

	// Transport-level failure (DNS, TLS, connection reset, ctx timeout...).
	ge.Code = port.ErrorCodeGatewayUnavailable
	ge.Retryable = true
	return ge
}

func unsupported3DSError() error {
	return &port.GatewayError{
		Code:    port.ErrorCodeMethodNotSupported,
		Message: "fincode adapter does not implement the 3D Secure flow yet",
	}
}

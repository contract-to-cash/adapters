package stripe

import (
	"context"
	"errors"
	"fmt"
	"net/http"
	"time"

	stripego "github.com/stripe/stripe-go/v82"

	"github.com/contract-to-cash/core/application/port"
	"github.com/contract-to-cash/core/domain/shared"
)

// Gateway implements port.PaymentGateway using the Stripe API.
//
// See the package doc for the ID mapping and currency conventions. Every
// gateway error returned to the caller is a *port.GatewayError whose RawError
// preserves the underlying *stripego.Error (or transport error), so callers
// can errors.As into the Stripe error for provider-specific inspection.
type Gateway struct {
	client *Client
	clock  shared.Clock
}

var _ port.PaymentGateway = (*Gateway)(nil)

// GatewayOption configures the Gateway.
type GatewayOption func(*Gateway)

// WithClock sets the clock used for fallback timestamps when a Stripe response
// carries no timestamp for the operation (e.g. the capture/void time).
// Defaults to shared.SystemClock.
func WithClock(clock shared.Clock) GatewayOption {
	return func(g *Gateway) { g.clock = clock }
}

// NewGateway creates a new Stripe payment gateway over the given client.
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
// This adapter implements card payments (credit and debit share Stripe's
// "card" PaymentMethod type).
func (g *Gateway) SupportedMethods() []port.PaymentMethodType {
	return []port.PaymentMethodType{
		port.PaymentMethodTypeCreditCard,
		port.PaymentMethodTypeDebitCard,
	}
}

// --- Charge / Authorize ---

// Charge performs a one-step charge: it creates and confirms a PaymentIntent
// with automatic capture. A 3D Secure challenge surfaces as a response with
// TransactionStatusRequiresAction and a ThreeDSecure redirect URL rather than
// an error.
func (g *Gateway) Charge(ctx context.Context, req *port.ChargeRequest) (*port.ChargeResponse, error) {
	amount, currency, err := toMinorUnits("amount", req.Amount)
	if err != nil {
		return nil, err
	}
	pm, err := resolvePaymentMethod(req.PaymentMethodID, req.Token)
	if err != nil {
		return nil, err
	}

	params := &stripego.PaymentIntentParams{
		Amount:        stripego.Int64(amount),
		Currency:      stripego.String(currency),
		CaptureMethod: stripego.String(string(stripego.PaymentIntentCaptureMethodAutomatic)),
		Confirm:       stripego.Bool(true),
		PaymentMethod: stripego.String(pm),
	}
	if req.Description != "" {
		params.Description = stripego.String(req.Description)
	}
	if req.CustomerID != "" {
		params.Customer = stripego.String(req.CustomerID)
	}
	applyThreeDS(params, req.ThreeDSecure)
	applyMetadata(&params.Params, req.Metadata)
	setIdempotencyKey(&params.Params, req.IdempotencyKey)

	pi, err := g.client.paymentIntents.New(params)
	if err != nil {
		return nil, g.wrapGatewayError("charge", err)
	}
	return g.toChargeResponse(pi), nil
}

// Authorize places a hold on funds without capturing, by creating and
// confirming a PaymentIntent with manual capture. The returned
// AuthorizationID (and TransactionID) is the PaymentIntent ID; pass it to
// Capture or Void.
func (g *Gateway) Authorize(ctx context.Context, req *port.AuthorizeRequest) (*port.AuthorizeResponse, error) {
	amount, currency, err := toMinorUnits("amount", req.Amount)
	if err != nil {
		return nil, err
	}
	pm, err := resolvePaymentMethod(req.PaymentMethodID, req.Token)
	if err != nil {
		return nil, err
	}

	params := &stripego.PaymentIntentParams{
		Amount:        stripego.Int64(amount),
		Currency:      stripego.String(currency),
		CaptureMethod: stripego.String(string(stripego.PaymentIntentCaptureMethodManual)),
		Confirm:       stripego.Bool(true),
		PaymentMethod: stripego.String(pm),
	}
	if req.CustomerID != "" {
		params.Customer = stripego.String(req.CustomerID)
	}
	applyThreeDS(params, req.ThreeDSecure)
	applyMetadata(&params.Params, req.Metadata)
	setIdempotencyKey(&params.Params, req.IdempotencyKey)

	pi, err := g.client.paymentIntents.New(params)
	if err != nil {
		return nil, g.wrapGatewayError("authorize", err)
	}
	return g.toAuthorizeResponse(pi), nil
}

// Capture captures a previously authorized PaymentIntent. A nil Amount
// captures the full authorized amount; a smaller Amount performs a partial
// capture.
func (g *Gateway) Capture(ctx context.Context, req *port.CaptureRequest) (*port.CaptureResponse, error) {
	if req.AuthorizationID == "" {
		return nil, &ValidationError{Field: "AuthorizationID", Message: "must not be empty"}
	}
	params := &stripego.PaymentIntentCaptureParams{}
	if req.Amount != nil {
		amount, _, err := toMinorUnits("amount", *req.Amount)
		if err != nil {
			return nil, err
		}
		params.AmountToCapture = stripego.Int64(amount)
	}
	applyMetadata(&params.Params, req.Metadata)
	setIdempotencyKey(&params.Params, req.IdempotencyKey)

	pi, err := g.client.paymentIntents.Capture(req.AuthorizationID, params)
	if err != nil {
		return nil, g.wrapGatewayError("capture", err)
	}
	captured := fromMinorUnits(pi.AmountReceived, pi.Currency)
	if pi.AmountReceived == 0 {
		captured = fromMinorUnits(pi.Amount, pi.Currency)
	}
	return &port.CaptureResponse{
		TransactionID:   pi.ID,
		AuthorizationID: pi.ID,
		Status:          mapIntentStatus(pi.Status),
		Amount:          captured,
		CapturedAt:      g.clock.Now(),
	}, nil
}

// Void cancels a previously authorized PaymentIntent before capture (an auth
// reversal; no funds move).
func (g *Gateway) Void(ctx context.Context, req *port.VoidRequest) (*port.VoidResponse, error) {
	if req.AuthorizationID == "" {
		return nil, &ValidationError{Field: "AuthorizationID", Message: "must not be empty"}
	}
	params := &stripego.PaymentIntentCancelParams{
		CancellationReason: stripego.String(string(stripego.PaymentIntentCancellationReasonAbandoned)),
	}
	setIdempotencyKey(&params.Params, req.IdempotencyKey)

	pi, err := g.client.paymentIntents.Cancel(req.AuthorizationID, params)
	if err != nil {
		return nil, g.wrapGatewayError("void", err)
	}
	return &port.VoidResponse{
		AuthorizationID: pi.ID,
		Status:          mapIntentStatus(pi.Status),
		VoidedAt:        g.timeOrNow(pi.CanceledAt),
	}, nil
}

// Cancel cancels a pending (not yet captured) PaymentIntent. Stripe models
// both void and cancel through PaymentIntent cancellation; Cancel uses a
// generic "requested_by_customer" reason.
func (g *Gateway) Cancel(ctx context.Context, req *port.CancelRequest) (*port.CancelResponse, error) {
	if req.TransactionID == "" {
		return nil, &ValidationError{Field: "TransactionID", Message: "must not be empty"}
	}
	params := &stripego.PaymentIntentCancelParams{
		CancellationReason: stripego.String(string(stripego.PaymentIntentCancellationReasonRequestedByCustomer)),
	}
	setIdempotencyKey(&params.Params, req.IdempotencyKey)

	pi, err := g.client.paymentIntents.Cancel(req.TransactionID, params)
	if err != nil {
		return nil, g.wrapGatewayError("cancel", err)
	}
	return &port.CancelResponse{
		TransactionID: pi.ID,
		Status:        mapIntentStatus(pi.Status),
		CanceledAt:    g.timeOrNow(pi.CanceledAt),
	}, nil
}

// Refund refunds a captured or charged PaymentIntent. A nil Amount issues a
// full refund; a smaller Amount a partial one.
func (g *Gateway) Refund(ctx context.Context, req *port.RefundRequest) (*port.RefundResponse, error) {
	if req.TransactionID == "" {
		return nil, &ValidationError{Field: "TransactionID", Message: "must not be empty"}
	}
	params := &stripego.RefundParams{
		PaymentIntent: stripego.String(req.TransactionID),
	}
	if req.Amount != nil {
		amount, _, err := toMinorUnits("amount", *req.Amount)
		if err != nil {
			return nil, err
		}
		params.Amount = stripego.Int64(amount)
	}
	if reason := mapRefundReason(req.Reason); reason != nil {
		params.Reason = reason
	}
	applyMetadata(&params.Params, req.Metadata)
	setIdempotencyKey(&params.Params, req.IdempotencyKey)

	r, err := g.client.refunds.New(params)
	if err != nil {
		return nil, g.wrapGatewayError("refund", err)
	}
	return &port.RefundResponse{
		RefundID:      r.ID,
		TransactionID: req.TransactionID,
		Status:        mapRefundStatus(r.Status),
		Amount:        fromMinorUnits(r.Amount, r.Currency),
		Reason:        req.Reason,
		RefundedAt:    g.timeOrNow(r.Created),
	}, nil
}

// GetTransaction retrieves a PaymentIntent by its ID and maps it to a
// port.Transaction.
func (g *Gateway) GetTransaction(ctx context.Context, transactionID string) (*port.Transaction, error) {
	if transactionID == "" {
		return nil, &ValidationError{Field: "transactionID", Message: "must not be empty"}
	}
	pi, err := g.client.paymentIntents.Get(transactionID, nil)
	if err != nil {
		return nil, g.wrapGatewayError("get transaction", err)
	}
	return g.toTransaction(pi), nil
}

// --- Payment methods ---

// RegisterPaymentMethod attaches an existing Stripe PaymentMethod (created
// client-side via Stripe.js/Elements and passed as Token) to a customer. When
// SetAsDefault is true, it is also set as the customer's default invoice
// payment method.
func (g *Gateway) RegisterPaymentMethod(ctx context.Context, req *port.RegisterPaymentMethodRequest) (*port.PaymentMethodDetail, error) {
	if req.CustomerID == "" {
		return nil, &ValidationError{Field: "CustomerID", Message: "must not be empty"}
	}
	if req.Token == "" {
		return nil, &ValidationError{Field: "Token", Message: "a Stripe PaymentMethod ID (pm_...) is required"}
	}

	pm, err := g.client.paymentMethods.Attach(req.Token, &stripego.PaymentMethodAttachParams{
		Customer: stripego.String(req.CustomerID),
	})
	if err != nil {
		return nil, g.wrapGatewayError("attach payment method", err)
	}

	if req.SetAsDefault {
		if _, err := g.client.customers.Update(req.CustomerID, &stripego.CustomerParams{
			InvoiceSettings: &stripego.CustomerInvoiceSettingsParams{
				DefaultPaymentMethod: stripego.String(pm.ID),
			},
		}); err != nil {
			return nil, g.wrapGatewayError("set default payment method", err)
		}
	}

	return toPaymentMethodDetail(pm, req.SetAsDefault), nil
}

// DeletePaymentMethod detaches a PaymentMethod from its customer.
func (g *Gateway) DeletePaymentMethod(ctx context.Context, paymentMethodID string) error {
	if paymentMethodID == "" {
		return &ValidationError{Field: "paymentMethodID", Message: "must not be empty"}
	}
	if _, err := g.client.paymentMethods.Detach(paymentMethodID, nil); err != nil {
		return g.wrapGatewayError("detach payment method", err)
	}
	return nil
}

// GetPaymentMethod retrieves a PaymentMethod by ID.
func (g *Gateway) GetPaymentMethod(ctx context.Context, paymentMethodID string) (*port.PaymentMethodDetail, error) {
	if paymentMethodID == "" {
		return nil, &ValidationError{Field: "paymentMethodID", Message: "must not be empty"}
	}
	pm, err := g.client.paymentMethods.Get(paymentMethodID, nil)
	if err != nil {
		return nil, g.wrapGatewayError("get payment method", err)
	}
	return toPaymentMethodDetail(pm, false), nil
}

// ListPaymentMethods lists a customer's stored card payment methods.
func (g *Gateway) ListPaymentMethods(ctx context.Context, customerID string) ([]*port.PaymentMethodDetail, error) {
	if customerID == "" {
		return nil, &ValidationError{Field: "customerID", Message: "must not be empty"}
	}
	params := &stripego.PaymentMethodListParams{
		Customer: stripego.String(customerID),
		Type:     stripego.String(string(stripego.PaymentMethodTypeCard)),
	}
	iter := g.client.paymentMethods.List(params)

	var out []*port.PaymentMethodDetail
	for iter.Next() {
		out = append(out, toPaymentMethodDetail(iter.PaymentMethod(), false))
	}
	if err := iter.Err(); err != nil {
		return nil, g.wrapGatewayError("list payment methods", err)
	}
	return out, nil
}

// --- Response mapping ---

func (g *Gateway) toChargeResponse(pi *stripego.PaymentIntent) *port.ChargeResponse {
	return &port.ChargeResponse{
		TransactionID:     pi.ID,
		Status:            mapIntentStatus(pi.Status),
		Amount:            fromMinorUnits(pi.Amount, pi.Currency),
		PaymentMethodID:   intentPaymentMethodID(pi),
		PaymentMethodType: port.PaymentMethodTypeCreditCard,
		CreatedAt:         unixTime(pi.Created),
		Metadata:          pi.Metadata,
		ThreeDSecure:      threeDSResult(pi),
	}
}

func (g *Gateway) toAuthorizeResponse(pi *stripego.PaymentIntent) *port.AuthorizeResponse {
	return &port.AuthorizeResponse{
		AuthorizationID: pi.ID,
		TransactionID:   pi.ID,
		Status:          mapIntentStatus(pi.Status),
		Amount:          fromMinorUnits(pi.Amount, pi.Currency),
		CreatedAt:       unixTime(pi.Created),
		Metadata:        pi.Metadata,
		ThreeDSecure:    threeDSResult(pi),
	}
}

func (g *Gateway) toTransaction(pi *stripego.PaymentIntent) *port.Transaction {
	t := &port.Transaction{
		ID:              pi.ID,
		GatewayID:       GatewayID,
		Type:            port.TransactionTypeCharge,
		Status:          mapIntentStatus(pi.Status),
		Amount:          fromMinorUnits(pi.Amount, pi.Currency),
		Description:     pi.Description,
		PaymentMethodID: intentPaymentMethodID(pi),
		Metadata:        pi.Metadata,
		CreatedAt:       unixTime(pi.Created),
		UpdatedAt:       unixTime(pi.Created),
	}
	if pi.Customer != nil {
		t.CustomerID = pi.Customer.ID
	}
	if pi.Status == stripego.PaymentIntentStatusRequiresCapture {
		t.Type = port.TransactionTypeAuthorize
		aid := pi.ID
		t.AuthorizationID = &aid
	}
	return t
}

func toPaymentMethodDetail(pm *stripego.PaymentMethod, isDefault bool) *port.PaymentMethodDetail {
	d := &port.PaymentMethodDetail{
		ID:        pm.ID,
		Type:      port.PaymentMethodTypeCreditCard,
		IsDefault: isDefault,
		CreatedAt: unixTime(pm.Created),
	}
	if pm.Customer != nil {
		d.CustomerID = pm.Customer.ID
	}
	if pm.Card != nil {
		d.Card = &port.CardDetails{
			Brand:       toCardBrand(string(pm.Card.Brand)),
			Last4:       pm.Card.Last4,
			ExpMonth:    int(pm.Card.ExpMonth),
			ExpYear:     int(pm.Card.ExpYear),
			Fingerprint: pm.Card.Fingerprint,
			Country:     pm.Card.Country,
			Funding:     string(pm.Card.Funding),
		}
	}
	return d
}

// --- Small helpers ---

// resolvePaymentMethod returns the Stripe PaymentMethod ID to charge,
// preferring an explicit PaymentMethodID over a Token (both are "pm_..."
// identifiers produced client-side).
func resolvePaymentMethod(paymentMethodID, token *string) (string, error) {
	if paymentMethodID != nil && *paymentMethodID != "" {
		return *paymentMethodID, nil
	}
	if token != nil && *token != "" {
		return *token, nil
	}
	return "", &ValidationError{
		Field:   "PaymentMethodID",
		Message: "a payment method ID or token is required",
	}
}

func applyThreeDS(params *stripego.PaymentIntentParams, tds *port.ThreeDSecureRequest) {
	if tds != nil && tds.ReturnURL != "" {
		params.ReturnURL = stripego.String(tds.ReturnURL)
	}
}

func applyMetadata(params *stripego.Params, metadata map[string]string) {
	for k, v := range metadata {
		params.AddMetadata(k, v)
	}
}

func setIdempotencyKey(params *stripego.Params, key string) {
	if key != "" {
		params.SetIdempotencyKey(key)
	}
}

func intentPaymentMethodID(pi *stripego.PaymentIntent) string {
	if pi.PaymentMethod != nil {
		return pi.PaymentMethod.ID
	}
	return ""
}

func threeDSResult(pi *stripego.PaymentIntent) *port.ThreeDSecureResult {
	if pi.NextAction != nil && pi.NextAction.RedirectToURL != nil && pi.NextAction.RedirectToURL.URL != "" {
		u := pi.NextAction.RedirectToURL.URL
		return &port.ThreeDSecureResult{
			Status:      port.ThreeDSecureStatusRequired,
			RedirectURL: &u,
		}
	}
	return nil
}

// unixTime converts a Unix second timestamp to a UTC time. A zero timestamp
// yields the zero time.
func unixTime(sec int64) time.Time {
	if sec == 0 {
		return time.Time{}
	}
	return time.Unix(sec, 0).UTC()
}

// timeOrNow converts a Unix second timestamp to UTC, falling back to the
// gateway clock when the timestamp is absent (0).
func (g *Gateway) timeOrNow(sec int64) time.Time {
	if sec == 0 {
		return g.clock.Now()
	}
	return time.Unix(sec, 0).UTC()
}

func mapIntentStatus(s stripego.PaymentIntentStatus) port.TransactionStatus {
	switch s {
	case stripego.PaymentIntentStatusSucceeded:
		return port.TransactionStatusSucceeded
	case stripego.PaymentIntentStatusRequiresCapture:
		return port.TransactionStatusAuthorized
	case stripego.PaymentIntentStatusRequiresAction, stripego.PaymentIntentStatusRequiresConfirmation:
		return port.TransactionStatusRequiresAction
	case stripego.PaymentIntentStatusProcessing:
		return port.TransactionStatusPending
	case stripego.PaymentIntentStatusCanceled:
		return port.TransactionStatusCanceled
	case stripego.PaymentIntentStatusRequiresPaymentMethod:
		return port.TransactionStatusFailed
	default:
		return port.TransactionStatusPending
	}
}

func mapRefundStatus(s stripego.RefundStatus) port.RefundStatus {
	switch s {
	case stripego.RefundStatusSucceeded:
		return port.RefundStatusSucceeded
	case stripego.RefundStatusFailed:
		return port.RefundStatusFailed
	case stripego.RefundStatusCanceled:
		return port.RefundStatusCanceled
	default:
		// pending and requires_action are both not-yet-final.
		return port.RefundStatusPending
	}
}

// mapRefundReason maps a port refund reason to the Stripe reason string.
// Stripe only accepts duplicate / fraudulent / requested_by_customer; any
// other reason (including RefundReasonOther) is sent with no reason rather
// than a value Stripe would reject.
func mapRefundReason(r port.RefundReason) *string {
	switch r {
	case port.RefundReasonDuplicate:
		return stripego.String(string(stripego.RefundReasonDuplicate))
	case port.RefundReasonFraudulent:
		return stripego.String(string(stripego.RefundReasonFraudulent))
	case port.RefundReasonRequestedByCustomer:
		return stripego.String(string(stripego.RefundReasonRequestedByCustomer))
	default:
		return nil
	}
}

// --- Error mapping ---

// wrapGatewayError converts an SDK error into a *port.GatewayError, mapping
// the Stripe error code/type to a port.ErrorCode and preserving the original
// error chain via RawError.
func (g *Gateway) wrapGatewayError(op string, err error) error {
	if err == nil {
		return nil
	}
	ge := &port.GatewayError{
		Code:     port.ErrorCodeProcessingError,
		Message:  fmt.Sprintf("stripe: %s failed", op),
		RawError: err,
	}

	var se *stripego.Error
	if errors.As(err, &se) {
		if se.Msg != "" {
			ge.Message = se.Msg
		}
		ge.Param = se.Param
		ge.DeclineCode = string(se.DeclineCode)
		ge.Code = mapStripeErrorCode(se)
		ge.Retryable = isRetryable(se)
		return ge
	}

	// Transport-level failure (DNS, TLS, connection reset, ctx timeout...).
	ge.Code = port.ErrorCodeGatewayUnavailable
	ge.Retryable = true
	return ge
}

func mapStripeErrorCode(se *stripego.Error) port.ErrorCode {
	switch se.Code {
	case stripego.ErrorCodeCardDeclined:
		return port.ErrorCodeCardDeclined
	case stripego.ErrorCodeExpiredCard:
		return port.ErrorCodeCardExpired
	case stripego.ErrorCodeIncorrectCVC, stripego.ErrorCodeInvalidCVC:
		return port.ErrorCodeInvalidCVC
	case stripego.ErrorCodeInsufficientFunds:
		return port.ErrorCodeInsufficientFunds
	case stripego.ErrorCodeIncorrectNumber, stripego.ErrorCodeInvalidNumber:
		return port.ErrorCodeInvalidCard
	case stripego.ErrorCodeInvalidExpiryMonth:
		return port.ErrorCodeInvalidExpiryMonth
	case stripego.ErrorCodeInvalidExpiryYear:
		return port.ErrorCodeInvalidExpiryYear
	case stripego.ErrorCodeProcessingError:
		return port.ErrorCodeProcessingError
	case stripego.ErrorCodeRateLimit:
		return port.ErrorCodeRateLimitExceeded
	}

	// Fall back to the error type / HTTP status when there is no specific
	// code mapping.
	switch se.Type {
	case stripego.ErrorTypeCard:
		return port.ErrorCodeCardDeclined
	case stripego.ErrorTypeAPI:
		return port.ErrorCodeGatewayUnavailable
	case stripego.ErrorTypeIdempotency:
		return port.ErrorCodeDuplicateTransaction
	}
	switch se.HTTPStatusCode {
	case http.StatusTooManyRequests:
		return port.ErrorCodeRateLimitExceeded
	case http.StatusRequestTimeout, http.StatusGatewayTimeout:
		return port.ErrorCodeGatewayTimeout
	}
	if se.HTTPStatusCode >= 500 {
		return port.ErrorCodeGatewayUnavailable
	}
	return port.ErrorCodeProcessingError
}

// isRetryable reports whether a Stripe error is safe to retry: rate limits,
// transient API errors, and 5xx/timeout responses.
func isRetryable(se *stripego.Error) bool {
	if se.Code == stripego.ErrorCodeRateLimit || se.Type == stripego.ErrorTypeAPI {
		return true
	}
	switch se.HTTPStatusCode {
	case http.StatusTooManyRequests, http.StatusRequestTimeout, http.StatusGatewayTimeout:
		return true
	}
	return se.HTTPStatusCode >= 500
}

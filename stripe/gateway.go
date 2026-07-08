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
	if req == nil {
		return nil, &ValidationError{Field: "req", Message: "must not be nil"}
	}
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
	applyAutomaticPaymentMethods(params)
	applyMetadata(&params.Params, req.Metadata)
	setIdempotencyKey(&params.Params, req.IdempotencyKey)
	setContext(&params.Params, ctx)

	pi, err := g.client.paymentIntents.New(params)
	if err != nil {
		return nil, g.wrapGatewayError("charge", err)
	}
	return g.toChargeResponse(pi)
}

// Authorize places a hold on funds without capturing, by creating and
// confirming a PaymentIntent with manual capture. The returned
// AuthorizationID (and TransactionID) is the PaymentIntent ID; pass it to
// Capture or Void.
func (g *Gateway) Authorize(ctx context.Context, req *port.AuthorizeRequest) (*port.AuthorizeResponse, error) {
	if req == nil {
		return nil, &ValidationError{Field: "req", Message: "must not be nil"}
	}
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
	applyAutomaticPaymentMethods(params)
	applyMetadata(&params.Params, req.Metadata)
	setIdempotencyKey(&params.Params, req.IdempotencyKey)
	setContext(&params.Params, ctx)

	pi, err := g.client.paymentIntents.New(params)
	if err != nil {
		return nil, g.wrapGatewayError("authorize", err)
	}
	return g.toAuthorizeResponse(pi)
}

// Capture captures a previously authorized PaymentIntent. A nil Amount
// captures the full authorized amount; a smaller Amount performs a partial
// capture.
func (g *Gateway) Capture(ctx context.Context, req *port.CaptureRequest) (*port.CaptureResponse, error) {
	if req == nil {
		return nil, &ValidationError{Field: "req", Message: "must not be nil"}
	}
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
	setContext(&params.Params, ctx)

	pi, err := g.client.paymentIntents.Capture(req.AuthorizationID, params)
	if err != nil {
		return nil, g.wrapGatewayError("capture", err)
	}
	// AmountReceived is the amount Stripe has actually settled. Fall back to
	// the intent's Amount only when the capture has fully succeeded but the
	// SDK left AmountReceived unset; while the capture is still processing
	// (AmountReceived == 0, status != succeeded) report the received amount
	// rather than over-reporting the full authorization as captured.
	capturedMinor := pi.AmountReceived
	if capturedMinor == 0 && pi.Status == stripego.PaymentIntentStatusSucceeded {
		capturedMinor = pi.Amount
	}
	captured, err := fromMinorUnits(capturedMinor, pi.Currency)
	if err != nil {
		return nil, err
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
	if req == nil {
		return nil, &ValidationError{Field: "req", Message: "must not be nil"}
	}
	if req.AuthorizationID == "" {
		return nil, &ValidationError{Field: "AuthorizationID", Message: "must not be empty"}
	}
	params := &stripego.PaymentIntentCancelParams{
		CancellationReason: stripego.String(string(stripego.PaymentIntentCancellationReasonAbandoned)),
	}
	setIdempotencyKey(&params.Params, req.IdempotencyKey)
	setContext(&params.Params, ctx)

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
	if req == nil {
		return nil, &ValidationError{Field: "req", Message: "must not be nil"}
	}
	if req.TransactionID == "" {
		return nil, &ValidationError{Field: "TransactionID", Message: "must not be empty"}
	}
	params := &stripego.PaymentIntentCancelParams{
		CancellationReason: stripego.String(string(stripego.PaymentIntentCancellationReasonRequestedByCustomer)),
	}
	setIdempotencyKey(&params.Params, req.IdempotencyKey)
	setContext(&params.Params, ctx)

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
	if req == nil {
		return nil, &ValidationError{Field: "req", Message: "must not be nil"}
	}
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
	setContext(&params.Params, ctx)

	r, err := g.client.refunds.New(params)
	if err != nil {
		return nil, g.wrapGatewayError("refund", err)
	}
	refunded, err := fromMinorUnits(r.Amount, r.Currency)
	if err != nil {
		return nil, err
	}
	return &port.RefundResponse{
		RefundID:      r.ID,
		TransactionID: req.TransactionID,
		Status:        mapRefundStatus(r.Status),
		Amount:        refunded,
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
	params := &stripego.PaymentIntentParams{}
	setContext(&params.Params, ctx)
	pi, err := g.client.paymentIntents.Get(transactionID, params)
	if err != nil {
		return nil, g.wrapGatewayError("get transaction", err)
	}
	return g.toTransaction(pi)
}

// --- Payment methods ---

// RegisterPaymentMethod attaches an existing Stripe PaymentMethod (created
// client-side via Stripe.js/Elements and passed as Token) to a customer. When
// SetAsDefault is true, it is also set as the customer's default invoice
// payment method.
//
// The two steps (attach, then set-default) are NOT atomic: if the attach
// succeeds but the default update fails, the method returns the update error
// while the payment method remains attached to the customer. Retrying is safe
// — re-attaching an already-attached PaymentMethod to the same customer is a
// no-op on Stripe's side — so callers should retry the whole call rather than
// treat the error as "nothing happened".
func (g *Gateway) RegisterPaymentMethod(ctx context.Context, req *port.RegisterPaymentMethodRequest) (*port.PaymentMethodDetail, error) {
	if req == nil {
		return nil, &ValidationError{Field: "req", Message: "must not be nil"}
	}
	if req.CustomerID == "" {
		return nil, &ValidationError{Field: "CustomerID", Message: "must not be empty"}
	}
	if req.Token == "" {
		return nil, &ValidationError{Field: "Token", Message: "a Stripe PaymentMethod ID (pm_...) is required"}
	}

	attachParams := &stripego.PaymentMethodAttachParams{
		Customer: stripego.String(req.CustomerID),
	}
	setContext(&attachParams.Params, ctx)
	pm, err := g.client.paymentMethods.Attach(req.Token, attachParams)
	if err != nil {
		return nil, g.wrapGatewayError("attach payment method", err)
	}

	if req.SetAsDefault {
		updateParams := &stripego.CustomerParams{
			InvoiceSettings: &stripego.CustomerInvoiceSettingsParams{
				DefaultPaymentMethod: stripego.String(pm.ID),
			},
		}
		setContext(&updateParams.Params, ctx)
		if _, err := g.client.customers.Update(req.CustomerID, updateParams); err != nil {
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
	params := &stripego.PaymentMethodDetachParams{}
	setContext(&params.Params, ctx)
	if _, err := g.client.paymentMethods.Detach(paymentMethodID, params); err != nil {
		return g.wrapGatewayError("detach payment method", err)
	}
	return nil
}

// GetPaymentMethod retrieves a PaymentMethod by ID. The IsDefault flag is
// resolved from the owning customer's invoice settings when the payment method
// is attached to one (Stripe records the default on the Customer, not the
// PaymentMethod), which costs one extra customer lookup.
func (g *Gateway) GetPaymentMethod(ctx context.Context, paymentMethodID string) (*port.PaymentMethodDetail, error) {
	if paymentMethodID == "" {
		return nil, &ValidationError{Field: "paymentMethodID", Message: "must not be empty"}
	}
	params := &stripego.PaymentMethodParams{}
	setContext(&params.Params, ctx)
	pm, err := g.client.paymentMethods.Get(paymentMethodID, params)
	if err != nil {
		return nil, g.wrapGatewayError("get payment method", err)
	}

	isDefault := false
	if pm.Customer != nil {
		defaultID, err := g.defaultPaymentMethodID(ctx, pm.Customer.ID)
		if err != nil {
			return nil, err
		}
		isDefault = defaultID != "" && defaultID == pm.ID
	}
	return toPaymentMethodDetail(pm, isDefault), nil
}

// ListPaymentMethods lists a customer's stored card payment methods, marking
// the one recorded as the customer's default (Stripe stores the default on the
// Customer's invoice settings, not on each PaymentMethod).
func (g *Gateway) ListPaymentMethods(ctx context.Context, customerID string) ([]*port.PaymentMethodDetail, error) {
	if customerID == "" {
		return nil, &ValidationError{Field: "customerID", Message: "must not be empty"}
	}

	defaultID, err := g.defaultPaymentMethodID(ctx, customerID)
	if err != nil {
		return nil, err
	}

	params := &stripego.PaymentMethodListParams{
		Customer: stripego.String(customerID),
		Type:     stripego.String(string(stripego.PaymentMethodTypeCard)),
	}
	// ListParams carries its own Context field (promoted here); the SDK's list
	// backend applies it per page request.
	params.Context = ctx
	iter := g.client.paymentMethods.List(params)

	var out []*port.PaymentMethodDetail
	for iter.Next() {
		pm := iter.PaymentMethod()
		out = append(out, toPaymentMethodDetail(pm, defaultID != "" && pm.ID == defaultID))
	}
	if err := iter.Err(); err != nil {
		return nil, g.wrapGatewayError("list payment methods", err)
	}
	return out, nil
}

// defaultPaymentMethodID returns the customer's default invoice payment method
// ID, or "" when none is set. Used to populate PaymentMethodDetail.IsDefault
// on read, since Stripe records the default on the Customer rather than on each
// PaymentMethod.
func (g *Gateway) defaultPaymentMethodID(ctx context.Context, customerID string) (string, error) {
	if customerID == "" {
		return "", nil
	}
	params := &stripego.CustomerParams{}
	setContext(&params.Params, ctx)
	cust, err := g.client.customers.Get(customerID, params)
	if err != nil {
		return "", g.wrapGatewayError("get customer", err)
	}
	if cust.InvoiceSettings != nil && cust.InvoiceSettings.DefaultPaymentMethod != nil {
		return cust.InvoiceSettings.DefaultPaymentMethod.ID, nil
	}
	return "", nil
}

// --- Response mapping ---

func (g *Gateway) toChargeResponse(pi *stripego.PaymentIntent) (*port.ChargeResponse, error) {
	amount, err := fromMinorUnits(pi.Amount, pi.Currency)
	if err != nil {
		return nil, err
	}
	return &port.ChargeResponse{
		TransactionID:     pi.ID,
		Status:            mapIntentStatus(pi.Status),
		Amount:            amount,
		PaymentMethodID:   intentPaymentMethodID(pi),
		PaymentMethodType: intentPaymentMethodType(pi),
		CreatedAt:         unixTime(pi.Created),
		Metadata:          pi.Metadata,
		ThreeDSecure:      threeDSResult(pi),
	}, nil
}

func (g *Gateway) toAuthorizeResponse(pi *stripego.PaymentIntent) (*port.AuthorizeResponse, error) {
	amount, err := fromMinorUnits(pi.Amount, pi.Currency)
	if err != nil {
		return nil, err
	}
	return &port.AuthorizeResponse{
		AuthorizationID: pi.ID,
		TransactionID:   pi.ID,
		Status:          mapIntentStatus(pi.Status),
		Amount:          amount,
		CreatedAt:       unixTime(pi.Created),
		Metadata:        pi.Metadata,
		ThreeDSecure:    threeDSResult(pi),
	}, nil
}

func (g *Gateway) toTransaction(pi *stripego.PaymentIntent) (*port.Transaction, error) {
	amount, err := fromMinorUnits(pi.Amount, pi.Currency)
	if err != nil {
		return nil, err
	}
	t := &port.Transaction{
		ID:              pi.ID,
		GatewayID:       GatewayID,
		Type:            port.TransactionTypeCharge,
		Status:          mapIntentStatus(pi.Status),
		Amount:          amount,
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
	return t, nil
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
		d.Type = cardFundingToMethodType(pm.Card.Funding)
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

// applyThreeDS threads the port 3D Secure request onto the PaymentIntent
// params. ReturnURL, when set, is where Stripe redirects the customer back to
// after the challenge. Required=true asks Stripe to force a 3DS challenge by
// setting payment_method_options[card][request_three_d_secure]="any" (rather
// than Stripe's default of only challenging when the issuer/network requires
// it), so a caller that demands strong authentication is not silently charged
// without it.
func applyThreeDS(params *stripego.PaymentIntentParams, tds *port.ThreeDSecureRequest) {
	if tds == nil {
		return
	}
	if tds.ReturnURL != "" {
		params.ReturnURL = stripego.String(tds.ReturnURL)
	}
	if tds.Required {
		if params.PaymentMethodOptions == nil {
			params.PaymentMethodOptions = &stripego.PaymentIntentPaymentMethodOptionsParams{}
		}
		if params.PaymentMethodOptions.Card == nil {
			params.PaymentMethodOptions.Card = &stripego.PaymentIntentPaymentMethodOptionsCardParams{}
		}
		params.PaymentMethodOptions.Card.RequestThreeDSecure = stripego.String(
			string(stripego.PaymentIntentPaymentMethodOptionsCardRequestThreeDSecureAny),
		)
	}
}

// applyAutomaticPaymentMethods pins the PaymentIntent to the explicitly
// supplied payment method instead of the account's Dashboard-configured
// dynamic payment methods. Without it, Stripe treats Dashboard-enabled
// redirect-based payment methods as confirmation candidates and rejects the
// request with "you must provide a return_url" even though a card
// PaymentMethod is explicitly attached (issue #51).
//
// Must run after applyThreeDS: when the caller supplied no ReturnURL a
// redirect flow is impossible, so redirects are disabled outright
// (allow_redirects=never). When a ReturnURL is present (3DS redirect flow),
// allow_redirects is left at Stripe's default ("always") because Stripe
// rejects return_url combined with allow_redirects=never.
func applyAutomaticPaymentMethods(params *stripego.PaymentIntentParams) {
	apm := &stripego.PaymentIntentAutomaticPaymentMethodsParams{
		Enabled: stripego.Bool(true),
	}
	if params.ReturnURL == nil {
		apm.AllowRedirects = stripego.String(
			string(stripego.PaymentIntentAutomaticPaymentMethodsAllowRedirectsNever),
		)
	}
	params.AutomaticPaymentMethods = apm
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

// setContext threads the caller's context onto the Stripe request params. The
// SDK backend only applies it when non-nil (stripe.go: req.WithContext), so
// without this every call would ignore the caller's deadline/cancellation.
func setContext(params *stripego.Params, ctx context.Context) {
	params.Context = ctx
}

func intentPaymentMethodID(pi *stripego.PaymentIntent) string {
	if pi.PaymentMethod != nil {
		return pi.PaymentMethod.ID
	}
	return ""
}

// intentPaymentMethodType reports the actual method used, distinguishing debit
// from credit when the PaymentIntent's payment method is expanded and carries
// card funding. Falls back to credit_card when funding is unknown.
func intentPaymentMethodType(pi *stripego.PaymentIntent) port.PaymentMethodType {
	if pi.PaymentMethod != nil && pi.PaymentMethod.Card != nil {
		return cardFundingToMethodType(pi.PaymentMethod.Card.Funding)
	}
	return port.PaymentMethodTypeCreditCard
}

// cardFundingToMethodType maps Stripe card funding to a port payment method
// type. Only "debit" is distinguished; everything else (credit, prepaid,
// unknown) reports as credit_card.
func cardFundingToMethodType(funding stripego.CardFunding) port.PaymentMethodType {
	if funding == stripego.CardFundingDebit {
		return port.PaymentMethodTypeDebitCard
	}
	return port.PaymentMethodTypeCreditCard
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
	case stripego.ErrorCodeAmountTooSmall:
		return port.ErrorCodeAmountTooSmall
	case stripego.ErrorCodeAmountTooLarge:
		return port.ErrorCodeAmountTooLarge
	case stripego.ErrorCodeAuthenticationRequired:
		return port.ErrorCodeAuthenticationRequired
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

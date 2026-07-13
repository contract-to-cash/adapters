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
// Cards (credit and debit share Stripe's "card" PaymentMethod type) settle
// synchronously. Konbini (convenience_store) and JP bank transfer via
// customer_balance (bank_transfer) are asynchronous: Charge returns
// requires_action with a hosted voucher / bank-transfer-instructions URL, and
// settlement arrives later via webhook (payment_intent.succeeded). PayPay
// (qr_code) is a redirect-approval method: Charge returns requires_action
// with the PayPay approval redirect URL and requires a return URL. All three
// non-card methods are one-step-charge only — Authorize rejects them — and
// JPY-only.
func (g *Gateway) SupportedMethods() []port.PaymentMethodType {
	return []port.PaymentMethodType{
		port.PaymentMethodTypeCreditCard,
		port.PaymentMethodTypeDebitCard,
		port.PaymentMethodTypeConvenienceStore,
		port.PaymentMethodTypeBankTransfer,
		port.PaymentMethodTypeQRCode,
	}
}

// --- Charge / Authorize ---

// Charge performs a one-step charge: it creates and confirms a PaymentIntent
// with automatic capture. A required customer action — a 3D Secure challenge
// for cards, a hosted konbini voucher, hosted bank-transfer instructions for
// customer_balance, or the PayPay approval redirect — surfaces as a response
// with TransactionStatusRequiresAction and a ThreeDSecure redirect/instructions
// URL rather than an error. Non-card methods then settle via webhook
// (payment_intent.succeeded).
//
// The payment method's type is looked up first (one extra API call per
// charge): a bare "pm_..." ID does not reveal its type, and Stripe requires
// konbini / customer_balance / paypay intents to name their type in
// payment_method_types instead of the card-pinned automatic_payment_methods
// configuration (issue #51) used for cards. PayPay additionally requires
// ThreeDSecure.ReturnURL (the customer approves in the PayPay app/web and is
// redirected back).
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
	spm, err := g.fetchPaymentMethod(ctx, pm)
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
	if err := applyMethodTypeParams(params, spm.Type, req.ThreeDSecure, currency, req.CustomerID); err != nil {
		return nil, err
	}
	applyMetadata(&params.Params, req.Metadata)
	setIdempotencyKey(&params.Params, req.IdempotencyKey)
	setContext(&params.Params, ctx)

	pi, err := g.client.paymentIntents.New(params)
	if err != nil {
		return nil, g.wrapGatewayError("charge", err)
	}
	return g.toChargeResponse(pi, stripePMToPortType(spm))
}

// Authorize places a hold on funds without capturing, by creating and
// confirming a PaymentIntent with manual capture. The returned
// AuthorizationID (and TransactionID) is the PaymentIntent ID; pass it to
// Capture or Void.
//
// Konbini and customer_balance (JP bank transfer) do not support manual
// capture on Stripe — there is nothing to place a hold on until the customer
// pays at the register / wires the funds — so Authorize rejects them with a
// method_not_supported GatewayError instead of silently issuing a one-step
// charge. PayPay is rejected likewise: stripe-go v82 exposes no manual-capture
// surface for it, so this adapter conservatively treats it as charge-only.
// Use Charge for those methods.
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
	spm, err := g.fetchPaymentMethod(ctx, pm)
	if err != nil {
		return nil, err
	}
	if isChargeOnlyType(spm.Type) {
		return nil, &port.GatewayError{
			Code: port.ErrorCodeMethodNotSupported,
			Message: fmt.Sprintf(
				"stripe: %s does not support authorize (manual capture) — use Charge",
				spm.Type),
		}
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
// Only cards are attachable. Konbini and customer_balance PaymentMethods are
// single-use on Stripe — a fresh PaymentMethod is created per charge, there is
// nothing reusable to store — so req.Type convenience_store / bank_transfer is
// rejected with a method_not_supported GatewayError (as is any other non-card
// type this adapter does not implement). An unspecified Type ("") keeps the
// historical attach-as-card behavior for backward compatibility.
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
	switch req.Type {
	case "", port.PaymentMethodTypeCreditCard, port.PaymentMethodTypeDebitCard:
		// Cards (Stripe's single "card" type) attach below.
	case port.PaymentMethodTypeConvenienceStore, port.PaymentMethodTypeBankTransfer,
		port.PaymentMethodTypeQRCode:
		return nil, &port.GatewayError{
			Code: port.ErrorCodeMethodNotSupported,
			Message: fmt.Sprintf(
				"stripe: %q payment methods (konbini / customer_balance / paypay) are single-use in this adapter and cannot be stored on a customer; create one per charge instead",
				req.Type),
		}
	default:
		return nil, &port.GatewayError{
			Code:    port.ErrorCodeMethodNotSupported,
			Message: fmt.Sprintf("stripe adapter cannot register %q payment methods", req.Type),
		}
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
		if err := g.SetDefaultPaymentMethod(ctx, req.CustomerID, pm.ID); err != nil {
			return nil, err
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
	pm, err := g.fetchPaymentMethod(ctx, paymentMethodID)
	if err != nil {
		return nil, err
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

// ListPaymentMethods lists all of a customer's stored payment methods (Stripe
// returns every attached type when no type filter is sent), marking the one
// recorded as the customer's default (Stripe stores the default on the
// Customer's invoice settings, not on each PaymentMethod). Each entry's Type
// is mapped from the Stripe PaymentMethod type; unrecognized types pass
// through with their raw Stripe name (see stripePMToPortType).
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

// toChargeResponse maps a PaymentIntent to a ChargeResponse. methodType is
// the port type resolved from the actual PaymentMethod charged (Charge looks
// it up before creating the intent); when empty, it falls back to inspecting
// the intent's expanded payment method.
func (g *Gateway) toChargeResponse(pi *stripego.PaymentIntent, methodType port.PaymentMethodType) (*port.ChargeResponse, error) {
	amount, err := fromMinorUnits(pi.Amount, pi.Currency)
	if err != nil {
		return nil, err
	}
	if methodType == "" {
		methodType = intentPaymentMethodType(pi)
	}
	return &port.ChargeResponse{
		TransactionID:     pi.ID,
		Status:            mapIntentStatus(pi.Status),
		Amount:            amount,
		PaymentMethodID:   intentPaymentMethodID(pi),
		PaymentMethodType: methodType,
		CreatedAt:         unixTime(pi.Created),
		Metadata:          pi.Metadata,
		ThreeDSecure:      nextActionResult(pi),
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
		ThreeDSecure:    nextActionResult(pi),
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
		// Surface any pending customer-action URL (3DS redirect, konbini
		// voucher, bank-transfer instructions, PayPay approval) so integrators
		// reading back a requires_action intent via GetTransaction get the
		// same URL the original ChargeResponse carried.
		ThreeDSecure: nextActionResult(pi),
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
		Type:      stripePMToPortType(pm),
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
	// PayPay: stripe-go v82 exposes no PayPay detail struct on PaymentMethod,
	// so only the provider (known from the type itself) is populated — no data
	// is invented.
	if pm.Type == stripePaymentMethodTypePayPay {
		d.QRCode = &port.QRCodeDetails{Provider: string(stripePaymentMethodTypePayPay)}
	}
	// Stripe surfaces bank account details on us_bank_account PaymentMethods;
	// konbini and customer_balance carry no reusable account details.
	if pm.USBankAccount != nil {
		d.BankAccount = &port.BankAccountDetails{
			BankName:      pm.USBankAccount.BankName,
			BankCode:      pm.USBankAccount.RoutingNumber,
			AccountType:   string(pm.USBankAccount.AccountType),
			AccountNumber: pm.USBankAccount.Last4, // masked: Stripe exposes last4 only
		}
		if pm.BillingDetails != nil {
			d.BankAccount.AccountHolder = pm.BillingDetails.Name
		}
	}
	return d
}

// --- Small helpers ---

// fetchPaymentMethod retrieves a Stripe PaymentMethod. Charge and Authorize
// use it to learn the type behind a bare "pm_..." ID (one extra API call per
// charge): Stripe requires konbini / customer_balance / paypay PaymentIntents
// to name their type explicitly in payment_method_types, and Authorize must
// reject those types before creating a manual-capture intent.
func (g *Gateway) fetchPaymentMethod(ctx context.Context, id string) (*stripego.PaymentMethod, error) {
	params := &stripego.PaymentMethodParams{}
	setContext(&params.Params, ctx)
	pm, err := g.client.paymentMethods.Get(id, params)
	if err != nil {
		return nil, g.wrapGatewayError("get payment method", err)
	}
	return pm, nil
}

// stripePaymentMethodTypePayPay is Stripe's "paypay" PaymentMethod type.
// stripe-go v82.5.1 does not enumerate it (no constant, no PaymentMethod.PayPay
// struct, no PaymentIntent options), but the API is string-driven: the type is
// sent verbatim in payment_method_types and decodes fine into the string-typed
// PaymentMethod.Type, so the adapter defines the constant locally.
const stripePaymentMethodTypePayPay stripego.PaymentMethodType = "paypay"

// isAsyncSettlementType reports whether the Stripe payment method type is an
// async INSTRUCTION method (confirm yields requires_action with issued payment
// instructions; funds arrive later via webhook). PayPay is deliberately NOT
// included: its requires_action is a redirect approval — the same nature as a
// card 3DS challenge, not an instruction issuance — which matters for the
// webhook classifier (classifyRequiresActionEvent) that maps only instruction
// methods to payment_instruction.created.
func isAsyncSettlementType(t stripego.PaymentMethodType) bool {
	return t == stripego.PaymentMethodTypeKonbini || t == stripego.PaymentMethodTypeCustomerBalance
}

// isChargeOnlyType reports whether the Stripe payment method type supports
// one-step charges only in this adapter: the async instruction methods plus
// paypay (no manual-capture surface in stripe-go v82, so Authorize rejects it
// conservatively). None of these can be attached to a customer either
// (RegisterPaymentMethod rejects their port types).
func isChargeOnlyType(t stripego.PaymentMethodType) bool {
	return isAsyncSettlementType(t) || t == stripePaymentMethodTypePayPay
}

// applyMethodTypeParams configures the PaymentIntent params for the resolved
// payment method type.
//
// Card — and any type this adapter does not special-case — keeps the exact
// card-pinned behavior from issue #51: applyThreeDS + applyAutomaticPaymentMethods,
// where a charge without a ReturnURL sends
// automatic_payment_methods[allow_redirects]=never so Stripe does not demand a
// return_url for Dashboard-enabled redirect methods.
//
// Konbini and customer_balance are async, display-based methods that must be
// named explicitly in payment_method_types (mutually exclusive with
// automatic_payment_methods, whose card-pinning would exclude them). Both are
// JPY-only: konbini is a Japan-domestic method and the customer_balance path is
// configured for jp_bank_transfer funding. 3D Secure is card-only, so the
// ThreeDSecure request is not applied on these paths — the customer follows the
// hosted voucher/instructions URL surfaced via the requires_action response
// instead of a return_url redirect. customer_balance additionally requires a
// Stripe customer, because the received funds are tracked on the customer's
// cash balance.
//
// PayPay is a JPY-only REDIRECT-approval method: it is likewise pinned via
// payment_method_types, but confirmation REQUIRES a return_url (the customer
// approves in the PayPay app/web and is redirected back). The only return-URL
// carrier on the port request is ThreeDSecureRequest.ReturnURL — the same
// field the card 3DS redirect flow uses — so the paypay branch reads it from
// there and rejects the charge with a ValidationError when it is absent,
// rather than silently creating an unconfirmable intent. The approval URL
// comes back via next_action.redirect_to_url (nextActionResult's first
// branch). The card-only request_three_d_secure option is not applied.
func applyMethodTypeParams(
	params *stripego.PaymentIntentParams,
	pmType stripego.PaymentMethodType,
	tds *port.ThreeDSecureRequest,
	currency, customerID string,
) error {
	switch pmType {
	case stripego.PaymentMethodTypeKonbini:
		if err := requireJPY("konbini", currency); err != nil {
			return err
		}
		params.PaymentMethodTypes = stripego.StringSlice([]string{
			string(stripego.PaymentMethodTypeKonbini),
		})
	case stripego.PaymentMethodTypeCustomerBalance:
		if err := requireJPY("customer_balance (JP bank transfer)", currency); err != nil {
			return err
		}
		if customerID == "" {
			return &ValidationError{
				Field:   "CustomerID",
				Message: "a Stripe customer is required for customer_balance (bank transfer) charges",
			}
		}
		params.PaymentMethodTypes = stripego.StringSlice([]string{
			string(stripego.PaymentMethodTypeCustomerBalance),
		})
		params.PaymentMethodOptions = &stripego.PaymentIntentPaymentMethodOptionsParams{
			CustomerBalance: &stripego.PaymentIntentPaymentMethodOptionsCustomerBalanceParams{
				FundingType: stripego.String("bank_transfer"),
				BankTransfer: &stripego.PaymentIntentPaymentMethodOptionsCustomerBalanceBankTransferParams{
					Type: stripego.String("jp_bank_transfer"),
				},
			},
		}
	case stripePaymentMethodTypePayPay:
		if err := requireJPY("paypay", currency); err != nil {
			return err
		}
		if tds == nil || tds.ReturnURL == "" {
			return &ValidationError{
				Field:   "ThreeDSecure.ReturnURL",
				Message: "a return URL is required for paypay charges (the customer approves in PayPay and is redirected back)",
			}
		}
		params.ReturnURL = stripego.String(tds.ReturnURL)
		params.PaymentMethodTypes = stripego.StringSlice([]string{
			string(stripePaymentMethodTypePayPay),
		})
	default:
		applyThreeDS(params, tds)
		applyAutomaticPaymentMethods(params)
	}
	return nil
}

// requireJPY rejects a non-JPY charge for a Japan-only payment method with a
// typed currency_not_supported GatewayError before any network call.
func requireJPY(method, currency string) error {
	if currency != "jpy" {
		return &port.GatewayError{
			Code:    port.ErrorCodeCurrencyNotSupported,
			Message: fmt.Sprintf("stripe: %s charges support JPY only, got %q", method, currency),
		}
	}
	return nil
}

// stripePMToPortType maps a Stripe PaymentMethod to the port payment method
// type. Cards distinguish debit via funding; konbini and customer_balance map
// to the port's convenience_store / bank_transfer; paypay maps to qr_code;
// us_bank_account (the type on which Stripe exposes bank account details) maps
// to direct_debit. Any other type degrades gracefully to a typed pass-through
// of its raw Stripe name — mirroring the webhook handler's unknown-event
// pass-through — so a new Stripe method never panics or masquerades as a card.
func stripePMToPortType(pm *stripego.PaymentMethod) port.PaymentMethodType {
	switch pm.Type {
	case stripego.PaymentMethodTypeCard:
		if pm.Card != nil {
			return cardFundingToMethodType(pm.Card.Funding)
		}
		return port.PaymentMethodTypeCreditCard
	case stripego.PaymentMethodTypeKonbini:
		return port.PaymentMethodTypeConvenienceStore
	case stripego.PaymentMethodTypeCustomerBalance:
		return port.PaymentMethodTypeBankTransfer
	case stripePaymentMethodTypePayPay:
		return port.PaymentMethodTypeQRCode
	case stripego.PaymentMethodTypeUSBankAccount:
		return port.PaymentMethodTypeDirectDebit
	default:
		// Defensive: card details present but the type field is odd/absent.
		if pm.Card != nil {
			return cardFundingToMethodType(pm.Card.Funding)
		}
		if pm.Type == "" {
			return port.PaymentMethodTypeCreditCard
		}
		return port.PaymentMethodType(pm.Type)
	}
}

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

// nextActionResult surfaces the PaymentIntent's next_action through the port's
// requires-action channel (ThreeDSecureResult.RedirectURL) — the same field
// the 3DS redirect flow uses, so the core's existing requires_action handling
// covers every method:
//
//   - card 3DS challenge or PayPay approval: next_action.redirect_to_url.url
//   - konbini: next_action.konbini_display_details.hosted_voucher_url
//   - customer_balance (JP bank transfer):
//     next_action.display_bank_transfer_instructions.hosted_instructions_url
//
// In every case the caller sends the customer to the URL to complete the
// payment (authenticate / approve in PayPay / print the voucher / read the
// wire instructions).
func nextActionResult(pi *stripego.PaymentIntent) *port.ThreeDSecureResult {
	na := pi.NextAction
	if na == nil {
		return nil
	}
	var u string
	switch {
	case na.RedirectToURL != nil && na.RedirectToURL.URL != "":
		u = na.RedirectToURL.URL
	case na.KonbiniDisplayDetails != nil && na.KonbiniDisplayDetails.HostedVoucherURL != "":
		u = na.KonbiniDisplayDetails.HostedVoucherURL
	case na.DisplayBankTransferInstructions != nil && na.DisplayBankTransferInstructions.HostedInstructionsURL != "":
		u = na.DisplayBankTransferInstructions.HostedInstructionsURL
	default:
		return nil
	}
	return &port.ThreeDSecureResult{
		Status:      port.ThreeDSecureStatusRequired,
		RedirectURL: &u,
	}
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

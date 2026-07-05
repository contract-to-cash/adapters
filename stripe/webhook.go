package stripe

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"net/textproto"
	"strings"
	"time"

	stripego "github.com/stripe/stripe-go/v82"
	"github.com/stripe/stripe-go/v82/webhook"

	"github.com/contract-to-cash/core/application/port"
)

// DefaultSignatureHeader is the HTTP header Stripe uses to carry the webhook
// signature.
const DefaultSignatureHeader = "Stripe-Signature"

// WebhookConfig configures the Stripe WebhookHandler.
type WebhookConfig struct {
	// Secret is the endpoint's webhook signing secret ("whsec_..."). Required.
	Secret string

	// SignatureHeader is the header carrying the signature. Defaults to
	// DefaultSignatureHeader ("Stripe-Signature").
	SignatureHeader string
}

// WebhookHandler implements port.WebhookHandler for Stripe webhooks.
//
// Verification delegates to the official SDK's webhook.ConstructEventWithOptions,
// which checks the HMAC-SHA256 signature (over "{timestamp}.{payload}") in the
// Stripe-Signature header using a constant-time comparison.
//
// Timestamp/replay validation is intentionally NOT performed here: the SDK's
// tolerance check reads the wall clock directly, which would break this
// module's clock-injection testing convention. Freshness and deduplication
// are the core port.WebhookProcessor's responsibility — it validates
// event.CreatedAt against an injected shared.Clock and dedups on event.ID.
//
// Event mapping: known Stripe event types are translated to
// port.WebhookEventType constants (see toWebhookEventType). Unknown types are
// passed through with their raw Stripe name as the Type, so new event kinds
// remain observable without the adapter blocking them.
type WebhookHandler struct {
	secret          string
	signatureHeader string
}

var _ port.WebhookHandler = (*WebhookHandler)(nil)

// NewWebhookHandler creates a WebhookHandler. cfg.Secret is required.
func NewWebhookHandler(cfg WebhookConfig) (*WebhookHandler, error) {
	if cfg.Secret == "" {
		return nil, &ValidationError{Field: "Secret", Message: "webhook signing secret must not be empty"}
	}
	header := cfg.SignatureHeader
	if header == "" {
		header = DefaultSignatureHeader
	}
	return &WebhookHandler{
		secret:          cfg.Secret,
		signatureHeader: header,
	}, nil
}

// ParseAndVerify verifies the Stripe-Signature of a raw webhook request and
// returns the parsed event. All failures are reported as *port.WebhookError.
func (h *WebhookHandler) ParseAndVerify(_ context.Context, req *port.WebhookRequest) (*port.WebhookEvent, error) {
	if req == nil {
		return nil, &port.WebhookError{
			Code:    port.WebhookErrorCodeInvalidPayload,
			Message: "webhook request is nil",
		}
	}

	sig, ok := lookupHeader(req.Headers, h.signatureHeader)
	if !ok || sig == "" {
		return nil, &port.WebhookError{
			Code:    port.WebhookErrorCodeInvalidSignature,
			Message: fmt.Sprintf("missing %q header", h.signatureHeader),
		}
	}

	event, err := webhook.ConstructEventWithOptions(req.Body, sig, h.secret, webhook.ConstructEventOptions{
		// Freshness is enforced by the core WebhookProcessor against an
		// injected clock; the API version is not pinned by this adapter.
		IgnoreTolerance:          true,
		IgnoreAPIVersionMismatch: true,
	})
	if err != nil {
		return nil, classifyWebhookError(err)
	}

	raw := rawEventData(&event)
	return &port.WebhookEvent{
		ID:        event.ID,
		Type:      toWebhookEventType(string(event.Type), raw),
		CreatedAt: time.Unix(event.Created, 0).UTC(),
		Data:      raw,
		RawData:   req.Body,
	}, nil
}

// rawEventData returns the event's inner object JSON, falling back to nil when
// absent (the full request body is always available via WebhookEvent.RawData).
func rawEventData(event *stripego.Event) []byte {
	if event.Data != nil {
		return event.Data.Raw
	}
	return nil
}

// classifyWebhookError maps an SDK ConstructEvent error to a typed
// port.WebhookError. Signature/header problems become invalid_signature;
// anything else (e.g. a malformed JSON body) becomes invalid_payload.
func classifyWebhookError(err error) *port.WebhookError {
	switch {
	case errors.Is(err, webhook.ErrNotSigned),
		errors.Is(err, webhook.ErrInvalidHeader),
		errors.Is(err, webhook.ErrNoValidSignature),
		errors.Is(err, webhook.ErrTooOld):
		return &port.WebhookError{
			Code:    port.WebhookErrorCodeInvalidSignature,
			Message: "signature verification failed",
			Cause:   err,
		}
	default:
		return &port.WebhookError{
			Code:    port.WebhookErrorCodeInvalidPayload,
			Message: "webhook payload could not be parsed",
			Cause:   err,
		}
	}
}

// stripeEventMap translates Stripe event type names to port.WebhookEventType.
// Only confidently-mappable events are listed; anything else passes through
// with its raw Stripe name (see toWebhookEventType).
var stripeEventMap = map[string]port.WebhookEventType{
	"payment_intent.succeeded":      port.WebhookEventPaymentSucceeded,
	"payment_intent.payment_failed": port.WebhookEventPaymentFailed,
	"payment_intent.processing":     port.WebhookEventPaymentPending,
	// payment_intent.canceled is deliberately NOT mapped to payment.failed: a
	// canceled PaymentIntent (a voided/abandoned authorization or a
	// customer-requested cancel) is not a failed payment, and mapping it there
	// would trigger the core's dunning / past-due handling for a payment that
	// was never even attempted. It passes through with its raw Stripe name.
	//
	// charge.refunded is mapped directly because for card refunds — the only
	// payment method this adapter supports (see SupportedMethods) — it fires
	// after the refund has been recorded on the charge. For asynchronous
	// payment methods (e.g. bank transfers), charge.refunded can fire while
	// the refund is still pending, so if this adapter ever supports non-card
	// methods this event needs the same status inspection as the Refund-object
	// events. Those events (refund.created / refund.updated /
	// charge.refund.updated) are async and must be classified from their
	// status — see refundEventTypes / classifyRefundEvent — so they are
	// intentionally NOT listed here.
	"charge.refunded":               port.WebhookEventRefundSucceeded,
	"refund.failed":                 port.WebhookEventRefundFailed,
	"charge.dispute.created":        port.WebhookEventChargebackCreated,
	"charge.dispute.updated":        port.WebhookEventChargebackUpdated,
	"charge.dispute.closed":         port.WebhookEventChargebackClosed,
	"payment_method.attached":       port.WebhookEventPaymentMethodAttached,
	"payment_method.detached":       port.WebhookEventPaymentMethodDetached,
	"customer.subscription.created": port.WebhookEventSubscriptionCreated,
	"customer.subscription.updated": port.WebhookEventSubscriptionUpdated,
	"customer.subscription.deleted": port.WebhookEventSubscriptionCanceled,
}

// refundEventTypes are Stripe event types that carry a Refund object whose
// status must be inspected before classification. Card refunds are
// asynchronous: refund.created typically arrives with status "pending", and
// refund.updated / charge.refund.updated also fire on transitions to
// "failed"/"canceled". Mapping any of them unconditionally to RefundSucceeded
// would let a refund that later fails be booked as a completed refund in the
// consumer's ledger (issue #13).
var refundEventTypes = map[string]struct{}{
	"refund.created":        {},
	"refund.updated":        {},
	"charge.refund.updated": {},
}

// toWebhookEventType maps a Stripe event type to a port.WebhookEventType,
// passing unknown types through unchanged (typed pass-through) so new Stripe
// event kinds remain observable rather than rejected. Refund-object events are
// classified from their status via classifyRefundEvent.
func toWebhookEventType(stripeType string, data []byte) port.WebhookEventType {
	if _, ok := refundEventTypes[stripeType]; ok {
		return classifyRefundEvent(stripeType, data)
	}
	if t, ok := stripeEventMap[stripeType]; ok {
		return t
	}
	return port.WebhookEventType(stripeType)
}

// classifyRefundEvent inspects the Refund object's status to classify an async
// refund event. Only a settled "succeeded" refund becomes RefundSucceeded;
// "failed"/"canceled" become RefundFailed. A "pending"/"requires_action"
// (still in flight), empty, unknown, or unreadable status is NOT guessed — it
// passes through with the raw Stripe name so a non-terminal refund is never
// booked as a completed one. The eventual refund.failed / a later
// refund.updated(succeeded) carries the terminal outcome.
func classifyRefundEvent(stripeType string, data []byte) port.WebhookEventType {
	var refund struct {
		Status stripego.RefundStatus `json:"status"`
	}
	if err := json.Unmarshal(data, &refund); err != nil {
		return port.WebhookEventType(stripeType)
	}
	switch refund.Status {
	case stripego.RefundStatusSucceeded:
		return port.WebhookEventRefundSucceeded
	case stripego.RefundStatusFailed, stripego.RefundStatusCanceled:
		return port.WebhookEventRefundFailed
	default:
		return port.WebhookEventType(stripeType)
	}
}

// lookupHeader finds a header value case-insensitively; port.WebhookRequest
// headers are a plain map whose key casing depends on the transport.
func lookupHeader(headers map[string]string, name string) (string, bool) {
	if v, ok := headers[name]; ok {
		return v, true
	}
	canonical := textproto.CanonicalMIMEHeaderKey(name)
	if v, ok := headers[canonical]; ok {
		return v, true
	}
	for k, v := range headers {
		if strings.EqualFold(k, name) {
			return v, true
		}
	}
	return "", false
}

package fincode

import (
	"context"
	"crypto/hmac"
	"crypto/sha256"
	"crypto/subtle"
	"encoding/base64"
	"encoding/hex"
	"encoding/json"
	"fmt"
	"net/textproto"
	"strings"

	"github.com/contract-to-cash/core/application/port"
	"github.com/contract-to-cash/core/domain/shared"
)

// DefaultSignatureHeader is the HTTP header fincode uses to carry the
// webhook signature.
const DefaultSignatureHeader = "signature"

// SignatureMode selects how the value of the signature header is verified.
//
// fincode's official documentation does not state the signature scheme in a
// way this adapter could confirm from primary sources, so the mode MUST be
// chosen explicitly: check in the fincode dashboard / specification whether
// the "signature" issued for your webhook subscription is a fixed string
// echoed on every delivery (choose SignatureModeStatic) or an HMAC computed
// over each request body (choose SignatureModeHMAC). There is deliberately
// no default — leaving Mode empty is a constructor error, so an unverified
// assumption is never baked in silently.
type SignatureMode string

const (
	// SignatureModeStatic treats Secret as the fixed signature string issued
	// by fincode when the webhook subscription was registered, and compares
	// it against the header value in constant time.
	//
	// LIMITATION: a static signature authenticates the sender only — it is
	// not bound to the request body, so body integrity rests entirely on
	// HTTPS transport security. Anyone who learns the value can forge
	// arbitrary webhook payloads.
	SignatureModeStatic SignatureMode = "static"

	// SignatureModeHMAC treats Secret as an HMAC key and verifies
	// HMAC-SHA256(Secret, raw body), standard base64, against the header
	// value in constant time. This authenticates the sender AND the body.
	SignatureModeHMAC SignatureMode = "hmac"
)

// WebhookConfig configures the fincode WebhookHandler.
type WebhookConfig struct {
	// Mode selects the signature verification scheme. Required — there is no
	// default. See SignatureMode for how to determine the correct value for
	// your fincode tenant.
	Mode SignatureMode

	// Secret is, depending on Mode:
	//   - SignatureModeStatic: the exact signature string fincode issued for
	//     the webhook subscription (compared verbatim against the header).
	//   - SignatureModeHMAC: the shared HMAC key.
	// Required.
	Secret string

	// SignatureHeader is the header carrying the signature.
	// Defaults to DefaultSignatureHeader ("signature").
	SignatureHeader string
}

// WebhookHandler implements port.WebhookHandler for fincode webhooks.
//
// Signature verification is mode-dependent (see SignatureMode): either a
// constant-time equality check against a fixed expected signature, or
// HMAC-SHA256(secret, body) in standard base64.
//
// Event mapping: known fincode event names are translated to the
// port.WebhookEventType constants (see toWebhookEventType). Unknown event
// names do NOT fail verification — the event is returned with its raw
// fincode name as the Type, so consumers can observe future fincode event
// kinds without the adapter blocking them. Consumers switch on the port
// constants and can treat everything else as pass-through.
type WebhookHandler struct {
	mode            SignatureMode
	secret          []byte
	signatureHeader string
	clock           shared.Clock
}

var _ port.WebhookHandler = (*WebhookHandler)(nil)

// WebhookOption configures the WebhookHandler.
type WebhookOption func(*WebhookHandler)

// WithWebhookClock sets the clock used as CreatedAt fallback when the
// payload carries no parseable timestamp. Defaults to shared.SystemClock.
func WithWebhookClock(clock shared.Clock) WebhookOption {
	return func(h *WebhookHandler) { h.clock = clock }
}

// NewWebhookHandler creates a WebhookHandler. Both cfg.Mode and cfg.Secret
// are required; an unset or unknown Mode is rejected so that the (unconfirmed)
// fincode signature scheme is always an explicit integrator decision.
func NewWebhookHandler(cfg WebhookConfig, opts ...WebhookOption) (*WebhookHandler, error) {
	switch cfg.Mode {
	case SignatureModeStatic, SignatureModeHMAC:
	case "":
		return nil, &ValidationError{
			Field: "Mode",
			Message: "signature mode is required: check the fincode dashboard/spec " +
				"and set SignatureModeStatic or SignatureModeHMAC explicitly",
		}
	default:
		return nil, &ValidationError{
			Field:   "Mode",
			Message: fmt.Sprintf("unknown signature mode %q", cfg.Mode),
		}
	}
	if cfg.Secret == "" {
		return nil, &ValidationError{Field: "Secret", Message: "webhook secret must not be empty"}
	}
	header := cfg.SignatureHeader
	if header == "" {
		header = DefaultSignatureHeader
	}
	h := &WebhookHandler{
		mode:            cfg.Mode,
		secret:          []byte(cfg.Secret),
		signatureHeader: header,
		clock:           shared.SystemClock{},
	}
	for _, opt := range opts {
		opt(h)
	}
	return h, nil
}

// webhookPayload is the subset of a fincode webhook body the handler
// inspects. The full body is preserved in WebhookEvent.Data / RawData.
type webhookPayload struct {
	Event       string `json:"event"`
	ID          string `json:"order_id"`
	PaymentID   string `json:"id"`
	ProcessDate string `json:"process_date"`
	Created     string `json:"created"`
	Updated     string `json:"updated"`
}

// ParseAndVerify verifies the signature of a raw fincode webhook request
// (per the configured SignatureMode) and returns the parsed event. All
// failures are reported as *port.WebhookError with an appropriate code.
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
	if !h.verify(req.Body, sig) {
		return nil, &port.WebhookError{
			Code:    port.WebhookErrorCodeInvalidSignature,
			Message: "signature mismatch",
		}
	}

	var payload webhookPayload
	if err := json.Unmarshal(req.Body, &payload); err != nil {
		return nil, &port.WebhookError{
			Code:    port.WebhookErrorCodeInvalidPayload,
			Message: "webhook body is not valid JSON",
			Cause:   err,
		}
	}
	if payload.Event == "" {
		return nil, &port.WebhookError{
			Code:    port.WebhookErrorCodeInvalidPayload,
			Message: "webhook body has no event field",
		}
	}

	createdAt := h.clock.Now()
	for _, s := range []string{payload.ProcessDate, payload.Updated, payload.Created} {
		if t, ok := parseFincodeTime(s); ok {
			createdAt = t
			break
		}
	}

	return &port.WebhookEvent{
		ID:        h.eventID(payload, req.Body),
		Type:      toWebhookEventType(payload.Event),
		CreatedAt: createdAt,
		Data:      json.RawMessage(req.Body),
		RawData:   req.Body,
	}, nil
}

// verify checks the header signature according to the configured mode. Both
// modes use a constant-time comparison.
func (h *WebhookHandler) verify(body []byte, headerSig string) bool {
	got := []byte(strings.TrimSpace(headerSig))
	switch h.mode {
	case SignatureModeStatic:
		// Fixed signature string: sender authentication only; the body is
		// not covered (HTTPS is the only integrity guarantee).
		return subtle.ConstantTimeCompare(h.secret, got) == 1
	case SignatureModeHMAC:
		mac := hmac.New(sha256.New, h.secret)
		mac.Write(body)
		expected := base64.StdEncoding.EncodeToString(mac.Sum(nil))
		return subtle.ConstantTimeCompare([]byte(expected), got) == 1
	default:
		// Unreachable: NewWebhookHandler rejects unknown modes.
		return false
	}
}

// eventID derives a deterministic, deduplication-safe event ID. fincode
// payloads carry no dedicated webhook event ID, so the ID is composed from
// the event name, the payment/order identifiers, and the process timestamp.
// If none of those are present, a SHA-256 of the raw body is used.
func (h *WebhookHandler) eventID(p webhookPayload, body []byte) string {
	ref := p.PaymentID
	if ref == "" {
		ref = p.ID
	}
	if ref != "" {
		return fmt.Sprintf("%s:%s:%s", p.Event, ref, p.ProcessDate)
	}
	sum := sha256.Sum256(body)
	return p.Event + ":" + hex.EncodeToString(sum[:])
}

// lookupHeader finds a header value case-insensitively; port.WebhookRequest
// headers are a plain map whose casing depends on the transport.
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

// fincodeEventMap translates fincode webhook event names to port event
// types. Only confidently-mappable card payment events are listed; anything
// else is passed through with its raw fincode name.
var fincodeEventMap = map[string]port.WebhookEventType{
	"payments.card.regist":     port.WebhookEventPaymentPending,
	"payments.card.exec":       port.WebhookEventPaymentSucceeded,
	"payments.card.capture":    port.WebhookEventPaymentSucceeded,
	"payments.card.cancel":     port.WebhookEventRefundSucceeded,
	"card.regist":              port.WebhookEventPaymentMethodAttached,
	"subscription.card.regist": port.WebhookEventSubscriptionCreated,
	"subscription.card.update": port.WebhookEventSubscriptionUpdated,
	"subscription.card.delete": port.WebhookEventSubscriptionCanceled,
}

// toWebhookEventType maps a fincode event name to a port.WebhookEventType.
// Unknown names are preserved as-is (typed pass-through) rather than
// rejected, so new fincode event kinds remain observable.
func toWebhookEventType(event string) port.WebhookEventType {
	if t, ok := fincodeEventMap[event]; ok {
		return t
	}
	return port.WebhookEventType(event)
}

// SignBody computes the signature a SignatureModeHMAC handler expects for the
// given body. Exposed for tests and for consumers that need to verify their
// fincode dashboard configuration end-to-end. (In SignatureModeStatic the
// expected signature is simply the configured Secret itself.)
func SignBody(secret string, body []byte) string {
	mac := hmac.New(sha256.New, []byte(secret))
	mac.Write(body)
	return base64.StdEncoding.EncodeToString(mac.Sum(nil))
}

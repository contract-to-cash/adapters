package stripe

import (
	"context"
	"crypto/hmac"
	"crypto/sha256"
	"encoding/hex"
	"errors"
	"fmt"
	"testing"
	"time"

	"github.com/contract-to-cash/core/application/port"
	"github.com/contract-to-cash/core/domain/shared"
)

const testWebhookSecret = "whsec_test_secret"

// testSignTime is the fixed instant the test payloads are signed at. newHandler
// injects a FixedClock pinned here so the signed transport timestamp stays
// within tolerance deterministically (the replay check added for core#191 uses
// the injected clock).
const testSignTime = int64(1_700_000_000)

// signStripe builds a valid Stripe-Signature header for the given body and
// timestamp, mirroring the scheme the SDK verifies:
// v1 = hex(HMAC-SHA256(secret, "t.payload")).
func signStripe(secret string, timestamp int64, body []byte) string {
	mac := hmac.New(sha256.New, []byte(secret))
	fmt.Fprintf(mac, "%d.", timestamp)
	mac.Write(body)
	return fmt.Sprintf("t=%d,v1=%s", timestamp, hex.EncodeToString(mac.Sum(nil)))
}

func eventBody(id, eventType string) []byte {
	return []byte(fmt.Sprintf(
		`{"id":%q,"object":"event","type":%q,"created":1700000000,"data":{"object":{"id":"pi_1","object":"payment_intent"}}}`,
		id, eventType))
}

// refundEventBody builds an event whose data.object is a Refund with the given
// status, mirroring the shape Stripe sends for refund.* / charge.refund.updated.
func refundEventBody(id, eventType, status string) []byte {
	return []byte(fmt.Sprintf(
		`{"id":%q,"object":"event","type":%q,"created":1700000000,"data":{"object":{"id":"re_1","object":"refund","status":%q}}}`,
		id, eventType, status))
}

func newHandler(t *testing.T) *WebhookHandler {
	t.Helper()
	h, err := NewWebhookHandler(WebhookConfig{Secret: testWebhookSecret},
		WithWebhookClock(shared.FixedClock{FixedTime: time.Unix(testSignTime, 0)}))
	if err != nil {
		t.Fatalf("NewWebhookHandler: %v", err)
	}
	return h
}

func TestWebhook_ParseAndVerify_Success(t *testing.T) {
	h := newHandler(t)
	body := eventBody("evt_1", "payment_intent.succeeded")
	sig := signStripe(testWebhookSecret, 1_700_000_000, body)

	event, err := h.ParseAndVerify(context.Background(), &port.WebhookRequest{
		Headers: map[string]string{"Stripe-Signature": sig},
		Body:    body,
	})
	if err != nil {
		t.Fatalf("ParseAndVerify: %v", err)
	}
	if event.ID != "evt_1" {
		t.Errorf("ID = %q", event.ID)
	}
	if event.Type != port.WebhookEventPaymentSucceeded {
		t.Errorf("Type = %q, want payment.succeeded", event.Type)
	}
	if len(event.RawData) == 0 || len(event.Data) == 0 {
		t.Errorf("Data/RawData should be populated")
	}
}

func TestWebhook_UnknownEventPassthrough(t *testing.T) {
	h := newHandler(t)
	body := eventBody("evt_2", "invoice.finalized")
	sig := signStripe(testWebhookSecret, 1_700_000_000, body)

	event, err := h.ParseAndVerify(context.Background(), &port.WebhookRequest{
		Headers: map[string]string{"Stripe-Signature": sig},
		Body:    body,
	})
	if err != nil {
		t.Fatalf("ParseAndVerify: %v", err)
	}
	if event.Type != port.WebhookEventType("invoice.finalized") {
		t.Errorf("Type = %q, want raw passthrough", event.Type)
	}
}

func TestWebhook_EventMapping(t *testing.T) {
	cases := map[string]port.WebhookEventType{
		// payment_intent.succeeded covers card charges and, for async methods
		// (konbini / customer_balance), the later settlement webhook;
		// processing means funds in flight. requires_action is classified from
		// the payload instead — see TestWebhook_RequiresActionClassification.
		"payment_intent.succeeded":      port.WebhookEventPaymentSucceeded,
		"payment_intent.payment_failed": port.WebhookEventPaymentFailed,
		"payment_intent.processing":     port.WebhookEventPaymentPending,
		"charge.dispute.created":        port.WebhookEventChargebackCreated,
		"payment_method.attached":       port.WebhookEventPaymentMethodAttached,
		"customer.subscription.deleted": port.WebhookEventSubscriptionCanceled,
	}
	h := newHandler(t)
	for stripeType, want := range cases {
		body := eventBody("evt_x", stripeType)
		sig := signStripe(testWebhookSecret, 1_700_000_000, body)
		event, err := h.ParseAndVerify(context.Background(), &port.WebhookRequest{
			Headers: map[string]string{"Stripe-Signature": sig},
			Body:    body,
		})
		if err != nil {
			t.Fatalf("%s: ParseAndVerify: %v", stripeType, err)
		}
		if event.Type != want {
			t.Errorf("%s → %q, want %q", stripeType, event.Type, want)
		}
	}
}

// TestWebhook_RefundStatusClassification verifies that async refund events are
// classified from the embedded Refund object's status rather than being mapped
// unconditionally to refund.succeeded (issue #13).
func TestWebhook_RefundStatusClassification(t *testing.T) {
	h := newHandler(t)
	cases := []struct {
		name      string
		eventType string
		status    string
		want      port.WebhookEventType
	}{
		{"pending refund.created is not succeeded", "refund.created", "pending", port.WebhookEventType("refund.created")},
		{"succeeded refund.created", "refund.created", "succeeded", port.WebhookEventRefundSucceeded},
		{"failed refund.updated", "refund.updated", "failed", port.WebhookEventRefundFailed},
		{"canceled refund.updated", "refund.updated", "canceled", port.WebhookEventRefundFailed},
		{"succeeded refund.updated", "refund.updated", "succeeded", port.WebhookEventRefundSucceeded},
		{"pending charge.refund.updated passthrough", "charge.refund.updated", "pending", port.WebhookEventType("charge.refund.updated")},
		{"failed charge.refund.updated", "charge.refund.updated", "failed", port.WebhookEventRefundFailed},
		{"requires_action refund.updated passthrough", "refund.updated", "requires_action", port.WebhookEventType("refund.updated")},
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			body := refundEventBody("evt_r", tc.eventType, tc.status)
			sig := signStripe(testWebhookSecret, 1_700_000_000, body)
			event, err := h.ParseAndVerify(context.Background(), &port.WebhookRequest{
				Headers: map[string]string{"Stripe-Signature": sig},
				Body:    body,
			})
			if err != nil {
				t.Fatalf("ParseAndVerify: %v", err)
			}
			if event.Type != tc.want {
				t.Errorf("%s/%s → %q, want %q", tc.eventType, tc.status, event.Type, tc.want)
			}
		})
	}
}

// chargeRefundedEventBody builds a charge.refunded event whose data.object is
// a Charge embedding a refunds list (older API versions expand it by default).
// An empty status produces a charge with no refunds list at all.
func chargeRefundedEventBody(id, status string) []byte {
	refunds := ""
	if status != "" {
		refunds = fmt.Sprintf(`,"refunds":{"object":"list","data":[{"id":"re_1","object":"refund","status":%q}]}`, status)
	}
	return []byte(fmt.Sprintf(
		`{"id":%q,"object":"event","type":"charge.refunded","created":1700000000,"data":{"object":{"id":"ch_1","object":"charge","status":"succeeded"%s}}}`,
		id, refunds))
}

// TestWebhook_ChargeRefundedStatusClassification verifies charge.refunded gets
// the same status inspection as the Refund-object events: on async methods
// (konbini / customer_balance bank transfers) Stripe can fire it while the
// refund is still pending, and booking that as refund.succeeded would record
// an unsettled refund as completed.
func TestWebhook_ChargeRefundedStatusClassification(t *testing.T) {
	h := newHandler(t)
	cases := []struct {
		name   string
		status string
		want   port.WebhookEventType
	}{
		{"succeeded refund classifies", "succeeded", port.WebhookEventRefundSucceeded},
		{"pending refund passes through", "pending", port.WebhookEventType("charge.refunded")},
		{"failed refund classifies", "failed", port.WebhookEventRefundFailed},
		{"canceled refund classifies", "canceled", port.WebhookEventRefundFailed},
		// API versions 2022-11-15+ omit the refunds list from the Charge:
		// the status cannot be inspected, so the event is not guessed as
		// succeeded — consumers use refund.created/updated instead.
		{"no refunds list passes through", "", port.WebhookEventType("charge.refunded")},
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			body := chargeRefundedEventBody("evt_cr", tc.status)
			sig := signStripe(testWebhookSecret, 1_700_000_000, body)
			event, err := h.ParseAndVerify(context.Background(), &port.WebhookRequest{
				Headers: map[string]string{"Stripe-Signature": sig},
				Body:    body,
			})
			if err != nil {
				t.Fatalf("ParseAndVerify: %v", err)
			}
			if event.Type != tc.want {
				t.Errorf("charge.refunded/%s → %q, want %q", tc.status, event.Type, tc.want)
			}
		})
	}
}

func TestWebhook_CanceledIntentPassthrough(t *testing.T) {
	h := newHandler(t)
	body := eventBody("evt_c", "payment_intent.canceled")
	sig := signStripe(testWebhookSecret, 1_700_000_000, body)

	event, err := h.ParseAndVerify(context.Background(), &port.WebhookRequest{
		Headers: map[string]string{"Stripe-Signature": sig},
		Body:    body,
	})
	if err != nil {
		t.Fatalf("ParseAndVerify: %v", err)
	}
	// Must NOT be mapped to payment.failed; passes through with raw name.
	if event.Type != port.WebhookEventType("payment_intent.canceled") {
		t.Errorf("Type = %q, want raw passthrough (not payment.failed)", event.Type)
	}
}

func TestWebhook_MissingSignature(t *testing.T) {
	h := newHandler(t)
	body := eventBody("evt_3", "payment_intent.succeeded")

	_, err := h.ParseAndVerify(context.Background(), &port.WebhookRequest{
		Headers: map[string]string{},
		Body:    body,
	})
	var we *port.WebhookError
	if !errors.As(err, &we) || we.Code != port.WebhookErrorCodeInvalidSignature {
		t.Fatalf("want invalid_signature WebhookError, got %v", err)
	}
}

func TestWebhook_BadSignature(t *testing.T) {
	h := newHandler(t)
	body := eventBody("evt_4", "payment_intent.succeeded")
	// Sign with the wrong secret.
	sig := signStripe("whsec_wrong", 1_700_000_000, body)

	_, err := h.ParseAndVerify(context.Background(), &port.WebhookRequest{
		Headers: map[string]string{"Stripe-Signature": sig},
		Body:    body,
	})
	var we *port.WebhookError
	if !errors.As(err, &we) || we.Code != port.WebhookErrorCodeInvalidSignature {
		t.Fatalf("want invalid_signature WebhookError, got %v", err)
	}
}

func TestWebhook_CaseInsensitiveHeader(t *testing.T) {
	h := newHandler(t)
	body := eventBody("evt_5", "payment_intent.succeeded")
	sig := signStripe(testWebhookSecret, 1_700_000_000, body)

	event, err := h.ParseAndVerify(context.Background(), &port.WebhookRequest{
		Headers: map[string]string{"stripe-signature": sig}, // lowercase
		Body:    body,
	})
	if err != nil {
		t.Fatalf("ParseAndVerify: %v", err)
	}
	if event.ID != "evt_5" {
		t.Errorf("ID = %q", event.ID)
	}
}

func TestNewWebhookHandler_RequiresSecret(t *testing.T) {
	_, err := NewWebhookHandler(WebhookConfig{})
	if !errors.Is(err, ErrValidation) {
		t.Fatalf("want ValidationError, got %v", err)
	}
}

func TestNewWebhookHandler_RejectsNegativeTolerance(t *testing.T) {
	_, err := NewWebhookHandler(WebhookConfig{Secret: testWebhookSecret, Tolerance: -time.Second})
	if !errors.Is(err, ErrValidation) {
		t.Fatalf("want ValidationError for negative tolerance, got %v", err)
	}
}

// TestWebhook_StaleTimestampRejected verifies transport-level replay protection
// (core#191): a validly-signed request whose transport timestamp is older than
// the tolerance is rejected as invalid_signature, even though the signature is
// perfectly valid.
func TestWebhook_StaleTimestampRejected(t *testing.T) {
	// Clock is far ahead of the signed timestamp (default tolerance is 5m).
	h, err := NewWebhookHandler(WebhookConfig{Secret: testWebhookSecret},
		WithWebhookClock(shared.FixedClock{FixedTime: time.Unix(testSignTime+3600, 0)}))
	if err != nil {
		t.Fatalf("NewWebhookHandler: %v", err)
	}
	body := eventBody("evt_stale", "payment_intent.succeeded")
	sig := signStripe(testWebhookSecret, testSignTime, body)

	_, err = h.ParseAndVerify(context.Background(), &port.WebhookRequest{
		Headers: map[string]string{"Stripe-Signature": sig},
		Body:    body,
	})
	var we *port.WebhookError
	if !errors.As(err, &we) || we.Code != port.WebhookErrorCodeInvalidSignature {
		t.Fatalf("want invalid_signature WebhookError for stale timestamp, got %v", err)
	}
}

// TestWebhook_FutureTimestampRejected verifies the tolerance is bidirectional:
// a signed timestamp too far in the future (a clock-skew forgery attempt) is
// also rejected.
func TestWebhook_FutureTimestampRejected(t *testing.T) {
	h, err := NewWebhookHandler(WebhookConfig{Secret: testWebhookSecret},
		WithWebhookClock(shared.FixedClock{FixedTime: time.Unix(testSignTime-3600, 0)}))
	if err != nil {
		t.Fatalf("NewWebhookHandler: %v", err)
	}
	body := eventBody("evt_future", "payment_intent.succeeded")
	sig := signStripe(testWebhookSecret, testSignTime, body)

	_, err = h.ParseAndVerify(context.Background(), &port.WebhookRequest{
		Headers: map[string]string{"Stripe-Signature": sig},
		Body:    body,
	})
	var we *port.WebhookError
	if !errors.As(err, &we) || we.Code != port.WebhookErrorCodeInvalidSignature {
		t.Fatalf("want invalid_signature WebhookError for future timestamp, got %v", err)
	}
}

// TestWebhook_WithinToleranceAccepted confirms a signed timestamp within the
// tolerance window (but not identical to the clock) still verifies.
func TestWebhook_WithinToleranceAccepted(t *testing.T) {
	h, err := NewWebhookHandler(WebhookConfig{Secret: testWebhookSecret},
		WithWebhookClock(shared.FixedClock{FixedTime: time.Unix(testSignTime+120, 0)})) // 2m skew < 5m
	if err != nil {
		t.Fatalf("NewWebhookHandler: %v", err)
	}
	body := eventBody("evt_ok", "payment_intent.succeeded")
	sig := signStripe(testWebhookSecret, testSignTime, body)

	if _, err := h.ParseAndVerify(context.Background(), &port.WebhookRequest{
		Headers: map[string]string{"Stripe-Signature": sig},
		Body:    body,
	}); err != nil {
		t.Fatalf("ParseAndVerify within tolerance: %v", err)
	}
}

// requiresActionEventBody builds a payment_intent.requires_action event whose
// data.object is a PaymentIntent with the given raw JSON fragment spliced into
// the object (e.g. a payment_method_types list or an expanded payment_method).
func requiresActionEventBody(id, intentExtra string) []byte {
	if intentExtra != "" {
		intentExtra = "," + intentExtra
	}
	return []byte(fmt.Sprintf(
		`{"id":%q,"object":"event","type":"payment_intent.requires_action","created":1700000000,"data":{"object":{"id":"pi_1","object":"payment_intent","status":"requires_action"%s}}}`,
		id, intentExtra))
}

// TestWebhook_RequiresActionClassification verifies that
// payment_intent.requires_action only becomes payment_instruction.created for
// async instruction methods (konbini / customer_balance). For a card the same
// Stripe event is a 3DS authentication prompt — not an instruction issuance —
// and it keeps the raw pass-through it had before this adapter supported async
// methods; an uninspectable payload is not guessed either.
func TestWebhook_RequiresActionClassification(t *testing.T) {
	h := newHandler(t)
	raw := port.WebhookEventType("payment_intent.requires_action")
	cases := []struct {
		name        string
		intentExtra string
		want        port.WebhookEventType
	}{
		{"konbini types classify", `"payment_method_types":["konbini"]`, port.WebhookEventPaymentInstructionCreated},
		{"customer_balance types classify", `"payment_method_types":["customer_balance"]`, port.WebhookEventPaymentInstructionCreated},
		{"expanded konbini payment_method classifies", `"payment_method":{"id":"pm_1","object":"payment_method","type":"konbini"}`, port.WebhookEventPaymentInstructionCreated},
		{"konbini types with unexpanded pm string classify", `"payment_method_types":["konbini"],"payment_method":"pm_1"`, port.WebhookEventPaymentInstructionCreated},
		{"card 3DS passes through", `"payment_method_types":["card"]`, raw},
		{"card with unexpanded pm string passes through", `"payment_method_types":["card"],"payment_method":"pm_1"`, raw},
		{"missing payment_method_types passes through", ``, raw},
		{"unparseable payment_method_types passes through", `"payment_method_types":"konbini"`, raw},
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			body := requiresActionEventBody("evt_ra", tc.intentExtra)
			sig := signStripe(testWebhookSecret, 1_700_000_000, body)
			event, err := h.ParseAndVerify(context.Background(), &port.WebhookRequest{
				Headers: map[string]string{"Stripe-Signature": sig},
				Body:    body,
			})
			if err != nil {
				t.Fatalf("ParseAndVerify: %v", err)
			}
			if event.Type != tc.want {
				t.Errorf("requires_action(%s) → %q, want %q", tc.intentExtra, event.Type, tc.want)
			}
		})
	}
}

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
		"payment_intent.succeeded":      port.WebhookEventPaymentSucceeded,
		"payment_intent.payment_failed": port.WebhookEventPaymentFailed,
		"charge.refunded":               port.WebhookEventRefundSucceeded,
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

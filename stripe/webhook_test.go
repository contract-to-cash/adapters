package stripe

import (
	"context"
	"crypto/hmac"
	"crypto/sha256"
	"encoding/hex"
	"errors"
	"fmt"
	"testing"

	"github.com/contract-to-cash/core/application/port"
)

const testWebhookSecret = "whsec_test_secret"

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

func newHandler(t *testing.T) *WebhookHandler {
	t.Helper()
	h, err := NewWebhookHandler(WebhookConfig{Secret: testWebhookSecret})
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

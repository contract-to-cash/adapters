package fincode

import (
	"context"
	"errors"
	"testing"
	"time"

	"github.com/contract-to-cash/core/application/port"
	"github.com/contract-to-cash/core/domain/shared"
)

// compile-time conformance check.
var _ port.WebhookHandler = (*WebhookHandler)(nil)

const whSecret = "wh_secret_test_abc"

func newTestWebhookHandler(t *testing.T, cfg WebhookConfig) *WebhookHandler {
	t.Helper()
	h, err := NewWebhookHandler(cfg, WithWebhookClock(shared.FixedClock{FixedTime: gwFixedTime}))
	if err != nil {
		t.Fatalf("NewWebhookHandler: %v", err)
	}
	return h
}

func signedRequest(body []byte) *port.WebhookRequest {
	return &port.WebhookRequest{
		Headers: map[string]string{"signature": SignBody(whSecret, body)},
		Body:    body,
	}
}

func TestNewWebhookHandler_RequiresSecret(t *testing.T) {
	_, err := NewWebhookHandler(WebhookConfig{Mode: SignatureModeHMAC})
	if err == nil {
		t.Fatal("expected error for empty secret")
	}
	var ve *ValidationError
	if !errors.As(err, &ve) {
		t.Fatalf("expected *ValidationError, got %T: %v", err, err)
	}
}

// The signature mode has NO default: fincode's signature scheme is not
// confirmed from primary sources, so the integrator must choose explicitly.
func TestNewWebhookHandler_RequiresExplicitMode(t *testing.T) {
	_, err := NewWebhookHandler(WebhookConfig{Secret: whSecret})
	if err == nil {
		t.Fatal("expected error when Mode is unset")
	}
	var ve *ValidationError
	if !errors.As(err, &ve) {
		t.Fatalf("expected *ValidationError, got %T: %v", err, err)
	}
	if ve.Field != "Mode" {
		t.Errorf("Field = %q, want Mode", ve.Field)
	}
}

func TestNewWebhookHandler_RejectsUnknownMode(t *testing.T) {
	_, err := NewWebhookHandler(WebhookConfig{Mode: "hmac-sha512", Secret: whSecret})
	var ve *ValidationError
	if !errors.As(err, &ve) || ve.Field != "Mode" {
		t.Fatalf("expected Mode ValidationError for unknown mode, got %v", err)
	}
}

// --- Static signature mode ---

func TestWebhook_StaticMode_ValidSignature(t *testing.T) {
	const staticSig = "fincode-issued-fixed-signature"
	h := newTestWebhookHandler(t, WebhookConfig{Mode: SignatureModeStatic, Secret: staticSig})
	body := []byte(`{"event":"payments.card.exec","id":"o_wh_st1","status":"CAPTURED","process_date":"2026/06/01 12:00:00.000"}`)

	event, err := h.ParseAndVerify(context.Background(), &port.WebhookRequest{
		Headers: map[string]string{"signature": staticSig},
		Body:    body,
	})
	if err != nil {
		t.Fatalf("ParseAndVerify: %v", err)
	}
	if event.Type != port.WebhookEventPaymentSucceeded {
		t.Errorf("Type = %q, want payment.succeeded", event.Type)
	}
}

func TestWebhook_StaticMode_WrongSignatureIsRejected(t *testing.T) {
	h := newTestWebhookHandler(t, WebhookConfig{Mode: SignatureModeStatic, Secret: "expected-signature"})
	_, err := h.ParseAndVerify(context.Background(), &port.WebhookRequest{
		Headers: map[string]string{"signature": "some-other-signature"},
		Body:    []byte(`{"event":"payments.card.exec","id":"o1"}`),
	})
	var we *port.WebhookError
	if !errors.As(err, &we) || we.Code != port.WebhookErrorCodeInvalidSignature {
		t.Fatalf("expected invalid_signature, got %v", err)
	}
}

func TestWebhook_StaticMode_MissingHeaderIsRejected(t *testing.T) {
	h := newTestWebhookHandler(t, WebhookConfig{Mode: SignatureModeStatic, Secret: "expected-signature"})
	_, err := h.ParseAndVerify(context.Background(), &port.WebhookRequest{
		Headers: map[string]string{},
		Body:    []byte(`{"event":"payments.card.exec"}`),
	})
	var we *port.WebhookError
	if !errors.As(err, &we) || we.Code != port.WebhookErrorCodeInvalidSignature {
		t.Fatalf("expected invalid_signature for missing header, got %v", err)
	}
}

// Documented limitation of static mode: the signature is not bound to the
// body, so a tampered body with the correct fixed signature VERIFIES.
// Body integrity in static mode rests entirely on HTTPS.
func TestWebhook_StaticMode_DoesNotAuthenticateBody(t *testing.T) {
	const staticSig = "fincode-issued-fixed-signature"
	h := newTestWebhookHandler(t, WebhookConfig{Mode: SignatureModeStatic, Secret: staticSig})
	if _, err := h.ParseAndVerify(context.Background(), &port.WebhookRequest{
		Headers: map[string]string{"signature": staticSig},
		Body:    []byte(`{"event":"payments.card.exec","id":"o1","amount":999999}`),
	}); err != nil {
		t.Fatalf("static mode intentionally does not cover the body; got %v", err)
	}
}

// A static handler must not accept an HMAC signature and vice versa: the two
// modes are distinct schemes, never interchangeable fallbacks.
func TestWebhook_ModesAreNotInterchangeable(t *testing.T) {
	body := []byte(`{"event":"payments.card.exec","id":"o1"}`)

	static := newTestWebhookHandler(t, WebhookConfig{Mode: SignatureModeStatic, Secret: whSecret})
	if _, err := static.ParseAndVerify(context.Background(), signedRequest(body)); err == nil {
		t.Error("static handler must reject an HMAC-computed header")
	}

	hmacH := newTestWebhookHandler(t, WebhookConfig{Mode: SignatureModeHMAC, Secret: whSecret})
	if _, err := hmacH.ParseAndVerify(context.Background(), &port.WebhookRequest{
		Headers: map[string]string{"signature": whSecret},
		Body:    body,
	}); err == nil {
		t.Error("hmac handler must reject the raw secret as a signature")
	}
}

func TestWebhook_ValidSignature(t *testing.T) {
	h := newTestWebhookHandler(t, WebhookConfig{Mode: SignatureModeHMAC, Secret: whSecret})
	body := []byte(`{"event":"payments.card.exec","id":"o_wh_001","order_id":"o_wh_001","process_date":"2026/06/01 12:00:00.000","amount":1000,"status":"CAPTURED"}`)

	event, err := h.ParseAndVerify(context.Background(), signedRequest(body))
	if err != nil {
		t.Fatalf("ParseAndVerify: %v", err)
	}
	if event.Type != port.WebhookEventPaymentSucceeded {
		t.Errorf("Type = %q, want payment.succeeded", event.Type)
	}
	if event.ID == "" {
		t.Error("event ID must be non-empty for deduplication")
	}
	if string(event.RawData) != string(body) {
		t.Error("RawData must preserve the exact body bytes")
	}
	if string(event.Data) != string(body) {
		t.Error("Data must carry the raw JSON payload")
	}
	// process_date is JST → 03:00 UTC.
	want := time.Date(2026, 6, 1, 3, 0, 0, 0, time.UTC)
	if !event.CreatedAt.Equal(want) {
		t.Errorf("CreatedAt = %v, want %v", event.CreatedAt, want)
	}
}

func TestWebhook_TamperedBodyIsRejected(t *testing.T) {
	h := newTestWebhookHandler(t, WebhookConfig{Mode: SignatureModeHMAC, Secret: whSecret})
	body := []byte(`{"event":"payments.card.exec","id":"o_wh_001","amount":1000}`)
	req := signedRequest(body)
	// Tamper AFTER signing.
	req.Body = []byte(`{"event":"payments.card.exec","id":"o_wh_001","amount":999999}`)

	_, err := h.ParseAndVerify(context.Background(), req)
	var we *port.WebhookError
	if !errors.As(err, &we) {
		t.Fatalf("expected *port.WebhookError, got %T: %v", err, err)
	}
	if we.Code != port.WebhookErrorCodeInvalidSignature {
		t.Errorf("Code = %q, want invalid_signature", we.Code)
	}
}

func TestWebhook_WrongSecretIsRejected(t *testing.T) {
	h := newTestWebhookHandler(t, WebhookConfig{Mode: SignatureModeHMAC, Secret: whSecret})
	body := []byte(`{"event":"payments.card.exec","id":"o_wh_001"}`)
	req := &port.WebhookRequest{
		Headers: map[string]string{"signature": SignBody("some-other-secret", body)},
		Body:    body,
	}
	_, err := h.ParseAndVerify(context.Background(), req)
	var we *port.WebhookError
	if !errors.As(err, &we) || we.Code != port.WebhookErrorCodeInvalidSignature {
		t.Fatalf("expected invalid_signature, got %v", err)
	}
}

func TestWebhook_MissingSignatureHeader(t *testing.T) {
	h := newTestWebhookHandler(t, WebhookConfig{Mode: SignatureModeHMAC, Secret: whSecret})
	_, err := h.ParseAndVerify(context.Background(), &port.WebhookRequest{
		Headers: map[string]string{},
		Body:    []byte(`{"event":"payments.card.exec"}`),
	})
	var we *port.WebhookError
	if !errors.As(err, &we) || we.Code != port.WebhookErrorCodeInvalidSignature {
		t.Fatalf("expected invalid_signature for missing header, got %v", err)
	}
}

func TestWebhook_HeaderLookupIsCaseInsensitive(t *testing.T) {
	h := newTestWebhookHandler(t, WebhookConfig{Mode: SignatureModeHMAC, Secret: whSecret})
	body := []byte(`{"event":"payments.card.exec","id":"o1"}`)
	req := &port.WebhookRequest{
		// net/http canonicalizes to "Signature"; the handler must still find it.
		Headers: map[string]string{"Signature": SignBody(whSecret, body)},
		Body:    body,
	}
	if _, err := h.ParseAndVerify(context.Background(), req); err != nil {
		t.Fatalf("ParseAndVerify with canonicalized header: %v", err)
	}
}

func TestWebhook_CustomSignatureHeader(t *testing.T) {
	h := newTestWebhookHandler(t, WebhookConfig{Mode: SignatureModeHMAC, Secret: whSecret, SignatureHeader: "x-fincode-signature"})
	body := []byte(`{"event":"payments.card.exec","id":"o1"}`)

	req := &port.WebhookRequest{
		Headers: map[string]string{"x-fincode-signature": SignBody(whSecret, body)},
		Body:    body,
	}
	if _, err := h.ParseAndVerify(context.Background(), req); err != nil {
		t.Fatalf("ParseAndVerify with custom header: %v", err)
	}

	// The default header must no longer be accepted.
	if _, err := h.ParseAndVerify(context.Background(), signedRequest(body)); err == nil {
		t.Fatal("expected rejection when signature is on the wrong header")
	}
}

func TestWebhook_InvalidJSONBody(t *testing.T) {
	h := newTestWebhookHandler(t, WebhookConfig{Mode: SignatureModeHMAC, Secret: whSecret})
	body := []byte(`not-json`)
	_, err := h.ParseAndVerify(context.Background(), signedRequest(body))
	var we *port.WebhookError
	if !errors.As(err, &we) || we.Code != port.WebhookErrorCodeInvalidPayload {
		t.Fatalf("expected invalid_payload, got %v", err)
	}
}

func TestWebhook_MissingEventField(t *testing.T) {
	h := newTestWebhookHandler(t, WebhookConfig{Mode: SignatureModeHMAC, Secret: whSecret})
	body := []byte(`{"id":"o1"}`)
	_, err := h.ParseAndVerify(context.Background(), signedRequest(body))
	var we *port.WebhookError
	if !errors.As(err, &we) || we.Code != port.WebhookErrorCodeInvalidPayload {
		t.Fatalf("expected invalid_payload, got %v", err)
	}
}

// Unknown fincode event names must be verified and passed through with the
// raw name as the Type, not rejected.
func TestWebhook_UnknownEventIsPassedThrough(t *testing.T) {
	h := newTestWebhookHandler(t, WebhookConfig{Mode: SignatureModeHMAC, Secret: whSecret})
	body := []byte(`{"event":"payments.applepay.exec","id":"o1"}`)

	event, err := h.ParseAndVerify(context.Background(), signedRequest(body))
	if err != nil {
		t.Fatalf("ParseAndVerify: %v", err)
	}
	if event.Type != port.WebhookEventType("payments.applepay.exec") {
		t.Errorf("Type = %q, want raw pass-through", event.Type)
	}
}

func TestWebhook_EventTypeMapping(t *testing.T) {
	cases := map[string]port.WebhookEventType{
		"payments.card.regist": port.WebhookEventPaymentPending,
		// capture always commits funds, so it is the reliable success signal.
		"payments.card.capture":    port.WebhookEventPaymentSucceeded,
		"card.regist":              port.WebhookEventPaymentMethodAttached,
		"subscription.card.regist": port.WebhookEventSubscriptionCreated,
	}
	for name, want := range cases {
		if got := toWebhookEventType(webhookPayload{Event: name}); got != want {
			t.Errorf("toWebhookEventType(%q) = %q, want %q", name, got, want)
		}
	}
}

// payments.card.exec fires for both one-step charges (status CAPTURED, funds
// committed) and auth-only executions (status AUTHORIZED, funds merely held).
// Only a CAPTURED exec may be reported as a completed payment; anything else
// is a typed pass-through so an authorization hold is never booked as money
// received.
func TestWebhook_ExecStatusDisambiguation(t *testing.T) {
	h := newTestWebhookHandler(t, WebhookConfig{Mode: SignatureModeHMAC, Secret: whSecret})

	cases := []struct {
		name string
		body string
		want port.WebhookEventType
	}{
		{
			name: "captured exec is a completed payment",
			body: `{"event":"payments.card.exec","id":"o_exec_cap","status":"CAPTURED"}`,
			want: port.WebhookEventPaymentSucceeded,
		},
		{
			name: "authorized exec is a hold, not a payment",
			body: `{"event":"payments.card.exec","id":"o_exec_auth","status":"AUTHORIZED"}`,
			want: port.WebhookEventType("payments.card.exec"),
		},
		{
			name: "exec without a status is not guessed",
			body: `{"event":"payments.card.exec","id":"o_exec_nostatus"}`,
			want: port.WebhookEventType("payments.card.exec"),
		},
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			event, err := h.ParseAndVerify(context.Background(), signedRequest([]byte(tc.body)))
			if err != nil {
				t.Fatalf("ParseAndVerify: %v", err)
			}
			if event.Type != tc.want {
				t.Errorf("Type = %q, want %q", event.Type, tc.want)
			}
		})
	}
}

// payments.card.cancel is ambiguous by construction: this adapter implements
// Void (auth reversal, no funds moved), Cancel, and full Refund all through
// the same /cancel endpoint, so the resulting event cannot be classified as
// a refund. It must be passed through with its raw name — mapping it to
// refund.succeeded would let an Authorize→Void appear as a full refund.
func TestWebhook_CancelIsPassedThroughNotRefund(t *testing.T) {
	h := newTestWebhookHandler(t, WebhookConfig{Mode: SignatureModeHMAC, Secret: whSecret})
	body := []byte(`{"event":"payments.card.cancel","id":"o_cancel_1","status":"CANCELED"}`)

	event, err := h.ParseAndVerify(context.Background(), signedRequest(body))
	if err != nil {
		t.Fatalf("ParseAndVerify: %v", err)
	}
	if event.Type == port.WebhookEventRefundSucceeded {
		t.Fatal("payments.card.cancel must NOT be classified as refund.succeeded")
	}
	if event.Type != port.WebhookEventType("payments.card.cancel") {
		t.Errorf("Type = %q, want raw pass-through", event.Type)
	}
}

// Event IDs must be stable for the same payload (dedup) and distinct across
// different payments/events.
func TestWebhook_EventIDStability(t *testing.T) {
	h := newTestWebhookHandler(t, WebhookConfig{Mode: SignatureModeHMAC, Secret: whSecret})

	body1 := []byte(`{"event":"payments.card.exec","id":"o1","process_date":"2026/06/01 12:00:00.000"}`)
	body2 := []byte(`{"event":"payments.card.exec","id":"o2","process_date":"2026/06/01 12:00:00.000"}`)

	e1a, err := h.ParseAndVerify(context.Background(), signedRequest(body1))
	if err != nil {
		t.Fatal(err)
	}
	e1b, err := h.ParseAndVerify(context.Background(), signedRequest(body1))
	if err != nil {
		t.Fatal(err)
	}
	e2, err := h.ParseAndVerify(context.Background(), signedRequest(body2))
	if err != nil {
		t.Fatal(err)
	}
	if e1a.ID != e1b.ID {
		t.Errorf("same payload must yield the same event ID: %q vs %q", e1a.ID, e1b.ID)
	}
	if e1a.ID == e2.ID {
		t.Errorf("different payments must yield different event IDs: %q", e1a.ID)
	}
}

// A payload without a parseable timestamp falls back to the injected clock.
func TestWebhook_CreatedAtFallsBackToClock(t *testing.T) {
	h := newTestWebhookHandler(t, WebhookConfig{Mode: SignatureModeHMAC, Secret: whSecret})
	body := []byte(`{"event":"payments.card.exec","id":"o1"}`)

	event, err := h.ParseAndVerify(context.Background(), signedRequest(body))
	if err != nil {
		t.Fatal(err)
	}
	if !event.CreatedAt.Equal(gwFixedTime) {
		t.Errorf("CreatedAt = %v, want clock fallback %v", event.CreatedAt, gwFixedTime)
	}
}

func TestWebhook_NilRequest(t *testing.T) {
	h := newTestWebhookHandler(t, WebhookConfig{Mode: SignatureModeHMAC, Secret: whSecret})
	_, err := h.ParseAndVerify(context.Background(), nil)
	var we *port.WebhookError
	if !errors.As(err, &we) || we.Code != port.WebhookErrorCodeInvalidPayload {
		t.Fatalf("expected invalid_payload for nil request, got %v", err)
	}
}

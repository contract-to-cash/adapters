package stripe

import (
	"context"
	"encoding/json"
	"errors"
	"math/big"
	"net/http"
	"net/http/httptest"
	"net/url"
	"strings"
	"testing"
	"time"

	"github.com/contract-to-cash/core/application/port"
	"github.com/contract-to-cash/core/domain/shared"
)

// fakeStripe is an httptest-backed stand-in for the Stripe API. The routes
// map is keyed by "METHOD /path" (path with the /v1 prefix); a wildcard "*"
// segment matches any single path element. The handler records the last
// request's parsed form and headers for assertions.
type fakeStripe struct {
	t          *testing.T
	server     *httptest.Server
	routes     map[string]http.HandlerFunc
	lastForm   url.Values
	lastHeader http.Header
	lastPath   string
}

func newFakeStripe(t *testing.T) *fakeStripe {
	t.Helper()
	f := &fakeStripe{t: t, routes: map[string]http.HandlerFunc{}}
	f.server = httptest.NewServer(http.HandlerFunc(f.dispatch))
	t.Cleanup(f.server.Close)
	return f
}

func (f *fakeStripe) dispatch(w http.ResponseWriter, r *http.Request) {
	if err := r.ParseForm(); err != nil {
		f.t.Fatalf("parse form: %v", err)
	}
	f.lastForm = r.PostForm
	f.lastHeader = r.Header.Clone()
	f.lastPath = r.URL.Path

	if h := f.match(r.Method, r.URL.Path); h != nil {
		h(w, r)
		return
	}
	f.t.Fatalf("unexpected request: %s %s", r.Method, r.URL.Path)
}

func (f *fakeStripe) match(method, path string) http.HandlerFunc {
	if h, ok := f.routes[method+" "+path]; ok {
		return h
	}
	reqSeg := strings.Split(path, "/")
	for key, h := range f.routes {
		parts := strings.SplitN(key, " ", 2)
		if parts[0] != method {
			continue
		}
		patSeg := strings.Split(parts[1], "/")
		if len(patSeg) != len(reqSeg) {
			continue
		}
		matched := true
		for i := range patSeg {
			if patSeg[i] != "*" && patSeg[i] != reqSeg[i] {
				matched = false
				break
			}
		}
		if matched {
			return h
		}
	}
	return nil
}

func (f *fakeStripe) on(route string, body string) {
	f.routes[route] = func(w http.ResponseWriter, _ *http.Request) {
		w.Header().Set("Content-Type", "application/json")
		_, _ = w.Write([]byte(body))
	}
}

func (f *fakeStripe) onStatus(route string, status int, body string) {
	f.routes[route] = func(w http.ResponseWriter, _ *http.Request) {
		w.Header().Set("Content-Type", "application/json")
		w.WriteHeader(status)
		_, _ = w.Write([]byte(body))
	}
}

func (f *fakeStripe) gateway(opts ...GatewayOption) *Gateway {
	f.t.Helper()
	client, err := NewClient(Config{SecretKey: "sk_test_123", APIBase: f.server.URL})
	if err != nil {
		f.t.Fatalf("NewClient: %v", err)
	}
	return NewGateway(client, opts...)
}

func jpy(n int64) shared.Money {
	return shared.NewMoney(new(big.Rat).SetInt64(n), shared.CurrencyJPY)
}

func usd(cents int64) shared.Money {
	return shared.NewMoney(new(big.Rat).SetFrac64(cents, 100), shared.CurrencyUSD)
}

var fixedClock = shared.FixedClock{FixedTime: time.Date(2026, 2, 1, 0, 0, 0, 0, time.UTC)}

// pmJSON builds a PaymentMethod response body of the given Stripe type, with
// optional extra top-level fields (e.g. "card": {...}).
func pmJSON(id, typ string, extra map[string]any) string {
	obj := map[string]any{"id": id, "object": "payment_method", "type": typ, "created": 1_700_000_000}
	for k, v := range extra {
		obj[k] = v
	}
	b, _ := json.Marshal(obj)
	return string(b)
}

// onCardPM registers the payment-method type lookup that Charge/Authorize
// perform before creating the PaymentIntent, answering with a plain card.
func (f *fakeStripe) onCardPM(id string) {
	f.on("GET /v1/payment_methods/"+id, pmJSON(id, "card", map[string]any{
		"card": map[string]any{"brand": "visa", "last4": "4242", "funding": "credit"},
	}))
}

func piJSON(id, status string, amount int64, currency string) string {
	obj := map[string]any{
		"id":       id,
		"object":   "payment_intent",
		"amount":   amount,
		"currency": currency,
		"status":   status,
		"created":  1_700_000_000,
	}
	b, _ := json.Marshal(obj)
	return string(b)
}

func TestGateway_Charge_Success(t *testing.T) {
	f := newFakeStripe(t)
	f.onCardPM("pm_card")
	f.on("POST /v1/payment_intents", piJSON("pi_1", "succeeded", 1000, "jpy"))
	g := f.gateway(WithClock(fixedClock))

	pmID := "pm_card"
	resp, err := g.Charge(context.Background(), &port.ChargeRequest{
		Amount:          jpy(1000),
		CustomerID:      "cus_1",
		PaymentMethodID: &pmID,
		IdempotencyKey:  "idem-1",
		Metadata:        map[string]string{"order": "o1"},
	})
	if err != nil {
		t.Fatalf("Charge: %v", err)
	}
	if resp.TransactionID != "pi_1" {
		t.Errorf("TransactionID = %q, want pi_1", resp.TransactionID)
	}
	if resp.Status != port.TransactionStatusSucceeded {
		t.Errorf("Status = %q, want succeeded", resp.Status)
	}
	if got := resp.Amount.Amount(); got.Cmp(big.NewRat(1000, 1)) != 0 {
		t.Errorf("Amount = %s, want 1000", got.RatString())
	}
	// The request forwards amount, currency, capture_method, confirm, the
	// idempotency key header, and metadata.
	if f.lastForm.Get("amount") != "1000" {
		t.Errorf("amount form = %q", f.lastForm.Get("amount"))
	}
	if f.lastForm.Get("currency") != "jpy" {
		t.Errorf("currency form = %q", f.lastForm.Get("currency"))
	}
	if f.lastForm.Get("capture_method") != "automatic" {
		t.Errorf("capture_method = %q", f.lastForm.Get("capture_method"))
	}
	if f.lastForm.Get("confirm") != "true" {
		t.Errorf("confirm = %q", f.lastForm.Get("confirm"))
	}
	if f.lastHeader.Get("Idempotency-Key") != "idem-1" {
		t.Errorf("Idempotency-Key header = %q", f.lastHeader.Get("Idempotency-Key"))
	}
	if f.lastForm.Get("metadata[order]") != "o1" {
		t.Errorf("metadata form = %q", f.lastForm.Get("metadata[order]"))
	}
}

func TestGateway_Charge_USDMinorUnits(t *testing.T) {
	f := newFakeStripe(t)
	f.onCardPM("pm_card")
	f.on("POST /v1/payment_intents", piJSON("pi_usd", "succeeded", 2599, "usd"))
	g := f.gateway(WithClock(fixedClock))

	pmID := "pm_card"
	resp, err := g.Charge(context.Background(), &port.ChargeRequest{
		Amount:          usd(2599), // $25.99
		PaymentMethodID: &pmID,
	})
	if err != nil {
		t.Fatalf("Charge: %v", err)
	}
	if f.lastForm.Get("amount") != "2599" {
		t.Errorf("amount form = %q, want 2599 (cents)", f.lastForm.Get("amount"))
	}
	if got := resp.Amount.Amount(); got.Cmp(big.NewRat(2599, 100)) != 0 {
		t.Errorf("Amount = %s, want 25.99", got.RatString())
	}
}

func TestGateway_Charge_UnsupportedCurrency(t *testing.T) {
	f := newFakeStripe(t)
	g := f.gateway()

	pmID := "pm_card"
	_, err := g.Charge(context.Background(), &port.ChargeRequest{
		Amount:          shared.NewMoney(big.NewRat(100, 1), shared.Currency("GBP")),
		PaymentMethodID: &pmID,
	})
	var ge *port.GatewayError
	if !errors.As(err, &ge) || ge.Code != port.ErrorCodeCurrencyNotSupported {
		t.Fatalf("want currency_not_supported GatewayError, got %v", err)
	}
}

func TestGateway_Charge_FractionalMinorUnitRejected(t *testing.T) {
	f := newFakeStripe(t)
	g := f.gateway()

	pmID := "pm_card"
	// 100.5 JPY has no integer minor-unit representation (JPY exponent 0).
	_, err := g.Charge(context.Background(), &port.ChargeRequest{
		Amount:          shared.NewMoney(big.NewRat(201, 2), shared.CurrencyJPY),
		PaymentMethodID: &pmID,
	})
	if !errors.Is(err, ErrValidation) {
		t.Fatalf("want ValidationError, got %v", err)
	}
}

func TestGateway_Charge_MissingPaymentMethod(t *testing.T) {
	f := newFakeStripe(t)
	g := f.gateway()
	_, err := g.Charge(context.Background(), &port.ChargeRequest{Amount: jpy(1000)})
	if !errors.Is(err, ErrValidation) {
		t.Fatalf("want ValidationError, got %v", err)
	}
}

func TestGateway_Charge_RequiresAction3DS(t *testing.T) {
	f := newFakeStripe(t)
	f.onCardPM("pm_card")
	obj := map[string]any{
		"id": "pi_3ds", "object": "payment_intent", "amount": 1000,
		"currency": "jpy", "status": "requires_action", "created": 1_700_000_000,
		"next_action": map[string]any{
			"type":            "redirect_to_url",
			"redirect_to_url": map[string]any{"url": "https://hooks.stripe.com/redirect/abc"},
		},
	}
	b, _ := json.Marshal(obj)
	f.on("POST /v1/payment_intents", string(b))
	g := f.gateway(WithClock(fixedClock))

	pmID := "pm_card"
	resp, err := g.Charge(context.Background(), &port.ChargeRequest{
		Amount: jpy(1000), PaymentMethodID: &pmID,
		ThreeDSecure: &port.ThreeDSecureRequest{Required: true, ReturnURL: "https://app/return"},
	})
	if err != nil {
		t.Fatalf("Charge: %v", err)
	}
	if resp.Status != port.TransactionStatusRequiresAction {
		t.Errorf("Status = %q, want requires_action", resp.Status)
	}
	if resp.ThreeDSecure == nil || resp.ThreeDSecure.RedirectURL == nil {
		t.Fatalf("expected 3DS redirect URL")
	}
	if *resp.ThreeDSecure.RedirectURL != "https://hooks.stripe.com/redirect/abc" {
		t.Errorf("redirect = %q", *resp.ThreeDSecure.RedirectURL)
	}
	if f.lastForm.Get("return_url") != "https://app/return" {
		t.Errorf("return_url = %q", f.lastForm.Get("return_url"))
	}
	// Required:true must request a 3DS challenge rather than being silently ignored.
	if got := f.lastForm.Get("payment_method_options[card][request_three_d_secure]"); got != "any" {
		t.Errorf("request_three_d_secure = %q, want any", got)
	}
}

// TestGateway_Charge_ThreeDSNotRequested verifies that when the caller does not
// force 3DS, no request_three_d_secure param is sent (Stripe applies its own
// default challenge policy).
func TestGateway_Charge_ThreeDSNotRequested(t *testing.T) {
	f := newFakeStripe(t)
	f.onCardPM("pm_card")
	f.on("POST /v1/payment_intents", piJSON("pi_1", "succeeded", 1000, "jpy"))
	g := f.gateway(WithClock(fixedClock))

	pmID := "pm_card"
	_, err := g.Charge(context.Background(), &port.ChargeRequest{
		Amount: jpy(1000), PaymentMethodID: &pmID,
		ThreeDSecure: &port.ThreeDSecureRequest{Required: false, ReturnURL: "https://app/return"},
	})
	if err != nil {
		t.Fatalf("Charge: %v", err)
	}
	if _, ok := f.lastForm["payment_method_options[card][request_three_d_secure]"]; ok {
		t.Errorf("request_three_d_secure should be omitted when Required is false")
	}
	if f.lastForm.Get("return_url") != "https://app/return" {
		t.Errorf("return_url = %q", f.lastForm.Get("return_url"))
	}
}

// TestGateway_Authorize_ThreeDSRequested verifies the 3DS request flag is also
// applied on the Authorize path.
func TestGateway_Authorize_ThreeDSRequested(t *testing.T) {
	f := newFakeStripe(t)
	f.onCardPM("pm_card")
	f.on("POST /v1/payment_intents", piJSON("pi_auth", "requires_capture", 5000, "jpy"))
	g := f.gateway(WithClock(fixedClock))

	pmID := "pm_card"
	_, err := g.Authorize(context.Background(), &port.AuthorizeRequest{
		Amount: jpy(5000), PaymentMethodID: &pmID,
		ThreeDSecure: &port.ThreeDSecureRequest{Required: true},
	})
	if err != nil {
		t.Fatalf("Authorize: %v", err)
	}
	if got := f.lastForm.Get("payment_method_options[card][request_three_d_secure]"); got != "any" {
		t.Errorf("request_three_d_secure = %q, want any", got)
	}
}

// TestGateway_Charge_AutomaticPaymentMethodsNoRedirect verifies that a plain
// charge (no return URL) disables redirect-based payment methods so Stripe
// does not demand a return_url for Dashboard-enabled dynamic payment methods
// (issue #51).
func TestGateway_Charge_AutomaticPaymentMethodsNoRedirect(t *testing.T) {
	f := newFakeStripe(t)
	f.onCardPM("pm_card")
	f.on("POST /v1/payment_intents", piJSON("pi_1", "succeeded", 1000, "jpy"))
	g := f.gateway(WithClock(fixedClock))

	pmID := "pm_card"
	_, err := g.Charge(context.Background(), &port.ChargeRequest{
		Amount: jpy(1000), PaymentMethodID: &pmID,
	})
	if err != nil {
		t.Fatalf("Charge: %v", err)
	}
	if got := f.lastForm.Get("automatic_payment_methods[enabled]"); got != "true" {
		t.Errorf("automatic_payment_methods[enabled] = %q, want true", got)
	}
	if got := f.lastForm.Get("automatic_payment_methods[allow_redirects]"); got != "never" {
		t.Errorf("automatic_payment_methods[allow_redirects] = %q, want never", got)
	}
}

// TestGateway_Charge_AutomaticPaymentMethodsWithReturnURL verifies that when
// the caller provides a 3DS return URL, redirects stay allowed: Stripe rejects
// return_url combined with allow_redirects=never, so the restriction must be
// omitted on that path.
func TestGateway_Charge_AutomaticPaymentMethodsWithReturnURL(t *testing.T) {
	f := newFakeStripe(t)
	f.onCardPM("pm_card")
	f.on("POST /v1/payment_intents", piJSON("pi_1", "succeeded", 1000, "jpy"))
	g := f.gateway(WithClock(fixedClock))

	pmID := "pm_card"
	_, err := g.Charge(context.Background(), &port.ChargeRequest{
		Amount: jpy(1000), PaymentMethodID: &pmID,
		ThreeDSecure: &port.ThreeDSecureRequest{ReturnURL: "https://app/return"},
	})
	if err != nil {
		t.Fatalf("Charge: %v", err)
	}
	if got := f.lastForm.Get("automatic_payment_methods[enabled]"); got != "true" {
		t.Errorf("automatic_payment_methods[enabled] = %q, want true", got)
	}
	if _, ok := f.lastForm["automatic_payment_methods[allow_redirects]"]; ok {
		t.Errorf("allow_redirects should be omitted when a return_url is set, got %q",
			f.lastForm.Get("automatic_payment_methods[allow_redirects]"))
	}
}

// TestGateway_Authorize_AutomaticPaymentMethodsNoRedirect verifies the same
// pinning applies on the Authorize path.
func TestGateway_Authorize_AutomaticPaymentMethodsNoRedirect(t *testing.T) {
	f := newFakeStripe(t)
	f.onCardPM("pm_card")
	f.on("POST /v1/payment_intents", piJSON("pi_auth", "requires_capture", 5000, "jpy"))
	g := f.gateway(WithClock(fixedClock))

	pmID := "pm_card"
	_, err := g.Authorize(context.Background(), &port.AuthorizeRequest{
		Amount: jpy(5000), PaymentMethodID: &pmID,
	})
	if err != nil {
		t.Fatalf("Authorize: %v", err)
	}
	if got := f.lastForm.Get("automatic_payment_methods[enabled]"); got != "true" {
		t.Errorf("automatic_payment_methods[enabled] = %q, want true", got)
	}
	if got := f.lastForm.Get("automatic_payment_methods[allow_redirects]"); got != "never" {
		t.Errorf("automatic_payment_methods[allow_redirects] = %q, want never", got)
	}
}

func TestGateway_Charge_CardDeclined(t *testing.T) {
	f := newFakeStripe(t)
	f.onCardPM("pm_card")
	f.onStatus("POST /v1/payment_intents", http.StatusPaymentRequired, `{
		"error": {"type": "card_error", "code": "card_declined",
		"decline_code": "generic_decline", "message": "Your card was declined."}}`)
	g := f.gateway()

	pmID := "pm_card"
	_, err := g.Charge(context.Background(), &port.ChargeRequest{Amount: jpy(1000), PaymentMethodID: &pmID})
	var ge *port.GatewayError
	if !errors.As(err, &ge) {
		t.Fatalf("want GatewayError, got %v", err)
	}
	if ge.Code != port.ErrorCodeCardDeclined {
		t.Errorf("Code = %q, want card_declined", ge.Code)
	}
	if ge.DeclineCode != "generic_decline" {
		t.Errorf("DeclineCode = %q", ge.DeclineCode)
	}
}

func TestGateway_Authorize_And_Capture(t *testing.T) {
	f := newFakeStripe(t)
	f.onCardPM("pm_card")
	f.on("POST /v1/payment_intents", piJSON("pi_auth", "requires_capture", 5000, "jpy"))
	g := f.gateway(WithClock(fixedClock))

	pmID := "pm_card"
	auth, err := g.Authorize(context.Background(), &port.AuthorizeRequest{Amount: jpy(5000), PaymentMethodID: &pmID})
	if err != nil {
		t.Fatalf("Authorize: %v", err)
	}
	if auth.Status != port.TransactionStatusAuthorized {
		t.Errorf("Status = %q, want authorized", auth.Status)
	}
	if auth.AuthorizationID != "pi_auth" {
		t.Errorf("AuthorizationID = %q", auth.AuthorizationID)
	}
	if f.lastForm.Get("capture_method") != "manual" {
		t.Errorf("capture_method = %q, want manual", f.lastForm.Get("capture_method"))
	}

	// Capture the authorization (partial).
	capObj := map[string]any{
		"id": "pi_auth", "object": "payment_intent", "amount": 5000,
		"amount_received": 3000, "currency": "jpy", "status": "succeeded", "created": 1_700_000_000,
	}
	cb, _ := json.Marshal(capObj)
	f.on("POST /v1/payment_intents/pi_auth/capture", string(cb))

	amt := jpy(3000)
	cap, err := g.Capture(context.Background(), &port.CaptureRequest{AuthorizationID: "pi_auth", Amount: &amt})
	if err != nil {
		t.Fatalf("Capture: %v", err)
	}
	if got := cap.Amount.Amount(); got.Cmp(big.NewRat(3000, 1)) != 0 {
		t.Errorf("captured Amount = %s, want 3000", got.RatString())
	}
	if f.lastForm.Get("amount_to_capture") != "3000" {
		t.Errorf("amount_to_capture = %q", f.lastForm.Get("amount_to_capture"))
	}
	if !cap.CapturedAt.Equal(fixedClock.Now()) {
		t.Errorf("CapturedAt = %v, want clock now", cap.CapturedAt)
	}
}

func TestGateway_Void(t *testing.T) {
	f := newFakeStripe(t)
	obj := map[string]any{
		"id": "pi_v", "object": "payment_intent", "amount": 1000, "currency": "jpy",
		"status": "canceled", "created": 1_700_000_000, "canceled_at": 1_700_000_500,
	}
	b, _ := json.Marshal(obj)
	f.on("POST /v1/payment_intents/pi_v/cancel", string(b))
	g := f.gateway(WithClock(fixedClock))

	resp, err := g.Void(context.Background(), &port.VoidRequest{AuthorizationID: "pi_v"})
	if err != nil {
		t.Fatalf("Void: %v", err)
	}
	if resp.Status != port.TransactionStatusCanceled {
		t.Errorf("Status = %q, want canceled", resp.Status)
	}
	if !resp.VoidedAt.Equal(time.Unix(1_700_000_500, 0).UTC()) {
		t.Errorf("VoidedAt = %v", resp.VoidedAt)
	}
}

func TestGateway_Refund(t *testing.T) {
	f := newFakeStripe(t)
	obj := map[string]any{
		"id": "re_1", "object": "refund", "amount": 1000, "currency": "jpy",
		"status": "succeeded", "created": 1_700_000_600,
	}
	b, _ := json.Marshal(obj)
	f.on("POST /v1/refunds", string(b))
	g := f.gateway(WithClock(fixedClock))

	amt := jpy(1000)
	resp, err := g.Refund(context.Background(), &port.RefundRequest{
		TransactionID: "pi_1", Amount: &amt, Reason: port.RefundReasonRequestedByCustomer,
	})
	if err != nil {
		t.Fatalf("Refund: %v", err)
	}
	if resp.RefundID != "re_1" {
		t.Errorf("RefundID = %q", resp.RefundID)
	}
	if resp.Status != port.RefundStatusSucceeded {
		t.Errorf("Status = %q", resp.Status)
	}
	if f.lastForm.Get("payment_intent") != "pi_1" {
		t.Errorf("payment_intent = %q", f.lastForm.Get("payment_intent"))
	}
	if f.lastForm.Get("reason") != "requested_by_customer" {
		t.Errorf("reason = %q", f.lastForm.Get("reason"))
	}
}

func TestGateway_Refund_OtherReasonOmitted(t *testing.T) {
	f := newFakeStripe(t)
	f.on("POST /v1/refunds", `{"id":"re_2","object":"refund","amount":1000,"currency":"jpy","status":"pending","created":1700000600}`)
	g := f.gateway(WithClock(fixedClock))

	_, err := g.Refund(context.Background(), &port.RefundRequest{TransactionID: "pi_1", Reason: port.RefundReasonOther})
	if err != nil {
		t.Fatalf("Refund: %v", err)
	}
	if _, ok := f.lastForm["reason"]; ok {
		t.Errorf("reason should be omitted for RefundReasonOther, got %q", f.lastForm.Get("reason"))
	}
}

func TestGateway_GetTransaction(t *testing.T) {
	f := newFakeStripe(t)
	f.on("GET /v1/payment_intents/pi_1", piJSON("pi_1", "succeeded", 1000, "jpy"))
	g := f.gateway(WithClock(fixedClock))

	tx, err := g.GetTransaction(context.Background(), "pi_1")
	if err != nil {
		t.Fatalf("GetTransaction: %v", err)
	}
	if tx.ID != "pi_1" || tx.GatewayID != GatewayID {
		t.Errorf("tx = %+v", tx)
	}
	if tx.Status != port.TransactionStatusSucceeded {
		t.Errorf("Status = %q", tx.Status)
	}
	// No next_action on the intent → no requires-action URL (regression).
	if tx.ThreeDSecure != nil {
		t.Errorf("ThreeDSecure = %+v, want nil without next_action", tx.ThreeDSecure)
	}
}

// TestGateway_GetTransaction_PendingActionURL verifies that reading back a
// requires_action intent surfaces the pending customer-action URL on
// Transaction.ThreeDSecure — the platform's only read-back surface for the
// voucher / instructions / approval / 3DS URL of a pending charge.
func TestGateway_GetTransaction_PendingActionURL(t *testing.T) {
	cases := []struct {
		name       string
		nextAction map[string]any
		wantURL    string
	}{
		{
			name: "konbini voucher",
			nextAction: map[string]any{
				"type": "konbini_display_details",
				"konbini_display_details": map[string]any{
					"hosted_voucher_url": "https://payments.stripe.com/konbini/voucher/abc",
				},
			},
			wantURL: "https://payments.stripe.com/konbini/voucher/abc",
		},
		{
			name: "redirect approval (PayPay / 3DS)",
			nextAction: map[string]any{
				"type":            "redirect_to_url",
				"redirect_to_url": map[string]any{"url": "https://hooks.stripe.com/redirect/xyz"},
			},
			wantURL: "https://hooks.stripe.com/redirect/xyz",
		},
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			f := newFakeStripe(t)
			obj := map[string]any{
				"id": "pi_pending", "object": "payment_intent", "amount": 1000,
				"currency": "jpy", "status": "requires_action", "created": 1_700_000_000,
				"next_action": tc.nextAction,
			}
			b, _ := json.Marshal(obj)
			f.on("GET /v1/payment_intents/pi_pending", string(b))
			g := f.gateway(WithClock(fixedClock))

			tx, err := g.GetTransaction(context.Background(), "pi_pending")
			if err != nil {
				t.Fatalf("GetTransaction: %v", err)
			}
			if tx.Status != port.TransactionStatusRequiresAction {
				t.Errorf("Status = %q, want requires_action", tx.Status)
			}
			if tx.ThreeDSecure == nil || tx.ThreeDSecure.RedirectURL == nil {
				t.Fatalf("expected pending action URL on ThreeDSecure, got %+v", tx.ThreeDSecure)
			}
			if *tx.ThreeDSecure.RedirectURL != tc.wantURL {
				t.Errorf("RedirectURL = %q, want %q", *tx.ThreeDSecure.RedirectURL, tc.wantURL)
			}
		})
	}
}

func TestGateway_RegisterPaymentMethod_Default(t *testing.T) {
	f := newFakeStripe(t)
	pmObj := map[string]any{
		"id": "pm_1", "object": "payment_method", "type": "card", "created": 1_700_000_000,
		"customer": map[string]any{"id": "cus_1"},
		"card":     map[string]any{"brand": "visa", "last4": "4242", "exp_month": 12, "exp_year": 2030, "funding": "credit", "country": "US"},
	}
	pb, _ := json.Marshal(pmObj)
	f.on("POST /v1/payment_methods/pm_1/attach", string(pb))
	f.on("POST /v1/customers/cus_1", `{"id":"cus_1","object":"customer"}`)
	g := f.gateway(WithClock(fixedClock))

	detail, err := g.RegisterPaymentMethod(context.Background(), &port.RegisterPaymentMethodRequest{
		CustomerID: "cus_1", Token: "pm_1", SetAsDefault: true,
	})
	if err != nil {
		t.Fatalf("RegisterPaymentMethod: %v", err)
	}
	if detail.ID != "pm_1" || !detail.IsDefault {
		t.Errorf("detail = %+v", detail)
	}
	if detail.Card == nil || detail.Card.Brand != port.CardBrandVisa || detail.Card.Last4 != "4242" {
		t.Errorf("card = %+v", detail.Card)
	}
	if f.lastPath != "/v1/customers/cus_1" {
		t.Errorf("expected default-update call last, got %s", f.lastPath)
	}
	if f.lastForm.Get("invoice_settings[default_payment_method]") != "pm_1" {
		t.Errorf("default pm form = %q", f.lastForm.Get("invoice_settings[default_payment_method]"))
	}
}

func TestGateway_ListPaymentMethods(t *testing.T) {
	f := newFakeStripe(t)
	list := map[string]any{
		"object": "list", "has_more": false, "url": "/v1/payment_methods",
		"data": []any{
			map[string]any{"id": "pm_1", "object": "payment_method", "type": "card", "created": 1_700_000_000,
				"card": map[string]any{"brand": "visa", "last4": "4242", "exp_month": 12, "exp_year": 2030}},
			map[string]any{"id": "pm_2", "object": "payment_method", "type": "card", "created": 1_700_000_000,
				"card": map[string]any{"brand": "mastercard", "last4": "5555", "exp_month": 1, "exp_year": 2031}},
		},
	}
	lb, _ := json.Marshal(list)
	f.on("GET /v1/payment_methods", string(lb))
	// The customer records pm_2 as its default invoice payment method.
	f.on("GET /v1/customers/cus_1", `{"id":"cus_1","object":"customer","invoice_settings":{"default_payment_method":{"id":"pm_2","object":"payment_method"}}}`)
	g := f.gateway(WithClock(fixedClock))

	methods, err := g.ListPaymentMethods(context.Background(), "cus_1")
	if err != nil {
		t.Fatalf("ListPaymentMethods: %v", err)
	}
	if len(methods) != 2 {
		t.Fatalf("len = %d, want 2", len(methods))
	}
	if methods[0].ID != "pm_1" || methods[1].ID != "pm_2" {
		t.Errorf("ids = %s,%s", methods[0].ID, methods[1].ID)
	}
	if methods[1].Card == nil || methods[1].Card.Brand != port.CardBrandMastercard {
		t.Errorf("second card = %+v", methods[1].Card)
	}
	// IsDefault is resolved from the customer's invoice settings.
	if methods[0].IsDefault {
		t.Errorf("pm_1 should not be default")
	}
	if !methods[1].IsDefault {
		t.Errorf("pm_2 should be default")
	}
}

func TestGateway_IDAndSupportedMethods(t *testing.T) {
	g := (&fakeStripe{}).gatewayNoServer(t)
	if g.ID() != "stripe" {
		t.Errorf("ID = %q", g.ID())
	}
	methods := g.SupportedMethods()
	want := []port.PaymentMethodType{
		port.PaymentMethodTypeCreditCard,
		port.PaymentMethodTypeDebitCard,
		port.PaymentMethodTypeConvenienceStore,
		port.PaymentMethodTypeBankTransfer,
		port.PaymentMethodTypeQRCode,
	}
	if len(methods) != len(want) {
		t.Fatalf("SupportedMethods = %v, want %v", methods, want)
	}
	for i := range want {
		if methods[i] != want[i] {
			t.Errorf("SupportedMethods[%d] = %q, want %q", i, methods[i], want[i])
		}
	}
}

// gatewayNoServer builds a gateway without a live server for pure-metadata tests.
func (f *fakeStripe) gatewayNoServer(t *testing.T) *Gateway {
	client, err := NewClient(Config{SecretKey: "sk_test_123"})
	if err != nil {
		t.Fatalf("NewClient: %v", err)
	}
	return NewGateway(client)
}

func TestNewClient_RequiresSecret(t *testing.T) {
	_, err := NewClient(Config{})
	if !errors.Is(err, ErrValidation) {
		t.Fatalf("want ValidationError, got %v", err)
	}
}

// TestGateway_ContextCancellation verifies the caller's context is propagated
// to the Stripe SDK: a canceled context aborts the request rather than being
// silently ignored. The fake server blocks until the request context is done.
func TestGateway_ContextPropagated(t *testing.T) {
	f := newFakeStripe(t)
	block := func(w http.ResponseWriter, r *http.Request) {
		<-r.Context().Done() // block until the caller cancels
		http.Error(w, "context canceled", http.StatusRequestTimeout)
	}
	// The payment-method lookup now precedes the intent creation; both hang
	// until the context is canceled so either request exercises propagation.
	f.routes["GET /v1/payment_methods/pm_card"] = block
	f.routes["POST /v1/payment_intents"] = block
	g := f.gateway(WithClock(fixedClock))

	ctx, cancel := context.WithCancel(context.Background())
	cancel() // cancel immediately

	pmID := "pm_card"
	_, err := g.Charge(ctx, &port.ChargeRequest{Amount: jpy(1000), PaymentMethodID: &pmID})
	if err == nil {
		t.Fatal("expected error from canceled context, got nil")
	}
	if !errors.Is(err, context.Canceled) {
		t.Errorf("error should wrap context.Canceled, got %v", err)
	}
}

func TestGateway_Charge_DebitCardType(t *testing.T) {
	f := newFakeStripe(t)
	f.on("GET /v1/payment_methods/pm_d", pmJSON("pm_d", "card", map[string]any{
		"card": map[string]any{"brand": "visa", "funding": "debit"},
	}))
	obj := map[string]any{
		"id": "pi_d", "object": "payment_intent", "amount": 1000, "currency": "jpy",
		"status": "succeeded", "created": 1_700_000_000,
		"payment_method": map[string]any{
			"id": "pm_d", "object": "payment_method", "type": "card",
			"card": map[string]any{"brand": "visa", "funding": "debit"},
		},
	}
	b, _ := json.Marshal(obj)
	f.on("POST /v1/payment_intents", string(b))
	g := f.gateway(WithClock(fixedClock))

	pmID := "pm_d"
	resp, err := g.Charge(context.Background(), &port.ChargeRequest{Amount: jpy(1000), PaymentMethodID: &pmID})
	if err != nil {
		t.Fatalf("Charge: %v", err)
	}
	if resp.PaymentMethodType != port.PaymentMethodTypeDebitCard {
		t.Errorf("PaymentMethodType = %q, want debit_card", resp.PaymentMethodType)
	}
}

// TestGateway_Charge_UnknownResponseCurrency verifies that a Stripe response in
// a currency the adapter cannot decode is rejected loudly instead of being
// silently read at a zero exponent (which would misreport the amount by 100x).
func TestGateway_Charge_UnknownResponseCurrency(t *testing.T) {
	f := newFakeStripe(t)
	f.onCardPM("pm_card")
	// Request in a supported currency; response comes back in GBP (unsupported).
	f.on("POST /v1/payment_intents", piJSON("pi_gbp", "succeeded", 2599, "gbp"))
	g := f.gateway(WithClock(fixedClock))

	pmID := "pm_card"
	_, err := g.Charge(context.Background(), &port.ChargeRequest{Amount: jpy(1000), PaymentMethodID: &pmID})
	var ge *port.GatewayError
	if !errors.As(err, &ge) || ge.Code != port.ErrorCodeCurrencyNotSupported {
		t.Fatalf("want currency_not_supported GatewayError, got %v", err)
	}
}

// TestGateway_Refund_UnknownResponseCurrency covers the refund response path
// through fromMinorUnits.
func TestGateway_Refund_UnknownResponseCurrency(t *testing.T) {
	f := newFakeStripe(t)
	f.on("POST /v1/refunds", `{"id":"re_x","object":"refund","amount":2599,"currency":"gbp","status":"succeeded","created":1700000600}`)
	g := f.gateway(WithClock(fixedClock))

	amt := jpy(1000)
	_, err := g.Refund(context.Background(), &port.RefundRequest{TransactionID: "pi_1", Amount: &amt})
	var ge *port.GatewayError
	if !errors.As(err, &ge) || ge.Code != port.ErrorCodeCurrencyNotSupported {
		t.Fatalf("want currency_not_supported GatewayError, got %v", err)
	}
}

// TestGateway_Capture_ProcessingNotOverReported verifies that a capture still
// processing (amount_received == 0, status != succeeded) reports the received
// amount (0) rather than over-reporting the full authorization as captured.
func TestGateway_Capture_ProcessingNotOverReported(t *testing.T) {
	f := newFakeStripe(t)
	obj := map[string]any{
		"id": "pi_p", "object": "payment_intent", "amount": 5000,
		"amount_received": 0, "currency": "jpy", "status": "processing", "created": 1_700_000_000,
	}
	b, _ := json.Marshal(obj)
	f.on("POST /v1/payment_intents/pi_p/capture", string(b))
	g := f.gateway(WithClock(fixedClock))

	cap, err := g.Capture(context.Background(), &port.CaptureRequest{AuthorizationID: "pi_p"})
	if err != nil {
		t.Fatalf("Capture: %v", err)
	}
	if got := cap.Amount.Amount(); got.Sign() != 0 {
		t.Errorf("captured Amount = %s, want 0 while processing", got.RatString())
	}
	if cap.Status != port.TransactionStatusPending {
		t.Errorf("Status = %q, want pending", cap.Status)
	}
}

// TestGateway_Capture_SucceededZeroReceivedFallsBack verifies the full-amount
// fallback still applies when the capture succeeded but the SDK left
// amount_received unset.
func TestGateway_Capture_SucceededZeroReceivedFallsBack(t *testing.T) {
	f := newFakeStripe(t)
	obj := map[string]any{
		"id": "pi_s", "object": "payment_intent", "amount": 5000,
		"amount_received": 0, "currency": "jpy", "status": "succeeded", "created": 1_700_000_000,
	}
	b, _ := json.Marshal(obj)
	f.on("POST /v1/payment_intents/pi_s/capture", string(b))
	g := f.gateway(WithClock(fixedClock))

	cap, err := g.Capture(context.Background(), &port.CaptureRequest{AuthorizationID: "pi_s"})
	if err != nil {
		t.Fatalf("Capture: %v", err)
	}
	if got := cap.Amount.Amount(); got.Cmp(big.NewRat(5000, 1)) != 0 {
		t.Errorf("captured Amount = %s, want 5000", got.RatString())
	}
}

// TestGateway_NilRequestGuards verifies the request-taking methods reject a nil
// request with a ValidationError instead of panicking.
func TestGateway_NilRequestGuards(t *testing.T) {
	g := (&fakeStripe{}).gatewayNoServer(t)
	ctx := context.Background()

	if _, err := g.Charge(ctx, nil); !errors.Is(err, ErrValidation) {
		t.Errorf("Charge(nil): want ValidationError, got %v", err)
	}
	if _, err := g.Authorize(ctx, nil); !errors.Is(err, ErrValidation) {
		t.Errorf("Authorize(nil): want ValidationError, got %v", err)
	}
	if _, err := g.Capture(ctx, nil); !errors.Is(err, ErrValidation) {
		t.Errorf("Capture(nil): want ValidationError, got %v", err)
	}
	if _, err := g.Void(ctx, nil); !errors.Is(err, ErrValidation) {
		t.Errorf("Void(nil): want ValidationError, got %v", err)
	}
	if _, err := g.Cancel(ctx, nil); !errors.Is(err, ErrValidation) {
		t.Errorf("Cancel(nil): want ValidationError, got %v", err)
	}
	if _, err := g.Refund(ctx, nil); !errors.Is(err, ErrValidation) {
		t.Errorf("Refund(nil): want ValidationError, got %v", err)
	}
	if _, err := g.RegisterPaymentMethod(ctx, nil); !errors.Is(err, ErrValidation) {
		t.Errorf("RegisterPaymentMethod(nil): want ValidationError, got %v", err)
	}
}

// TestGateway_ErrorCodeMapping checks the newly mapped Stripe error codes.
func TestGateway_ErrorCodeMapping(t *testing.T) {
	cases := []struct {
		code string
		want port.ErrorCode
	}{
		{"amount_too_small", port.ErrorCodeAmountTooSmall},
		{"amount_too_large", port.ErrorCodeAmountTooLarge},
		{"authentication_required", port.ErrorCodeAuthenticationRequired},
	}
	for _, tc := range cases {
		t.Run(tc.code, func(t *testing.T) {
			f := newFakeStripe(t)
			f.onCardPM("pm_card")
			f.onStatus("POST /v1/payment_intents", http.StatusPaymentRequired,
				`{"error":{"type":"card_error","code":"`+tc.code+`","message":"x"}}`)
			g := f.gateway()

			pmID := "pm_card"
			_, err := g.Charge(context.Background(), &port.ChargeRequest{Amount: jpy(1000), PaymentMethodID: &pmID})
			var ge *port.GatewayError
			if !errors.As(err, &ge) {
				t.Fatalf("want GatewayError, got %v", err)
			}
			if ge.Code != tc.want {
				t.Errorf("Code = %q, want %q", ge.Code, tc.want)
			}
		})
	}
}

func TestGateway_GetPaymentMethod_DefaultResolved(t *testing.T) {
	f := newFakeStripe(t)
	f.on("GET /v1/payment_methods/pm_1", `{"id":"pm_1","object":"payment_method","type":"card","customer":{"id":"cus_1"},"card":{"brand":"visa","last4":"4242"}}`)
	f.on("GET /v1/customers/cus_1", `{"id":"cus_1","object":"customer","invoice_settings":{"default_payment_method":{"id":"pm_1","object":"payment_method"}}}`)
	g := f.gateway(WithClock(fixedClock))

	detail, err := g.GetPaymentMethod(context.Background(), "pm_1")
	if err != nil {
		t.Fatalf("GetPaymentMethod: %v", err)
	}
	if !detail.IsDefault {
		t.Errorf("pm_1 should be reported as default")
	}
}

// --- Multi payment method support (konbini / JP bank transfer) ---

// TestGateway_Charge_Konbini verifies a konbini charge names the type
// explicitly in payment_method_types (instead of the card-pinned
// automatic_payment_methods path), surfaces the hosted voucher URL through the
// requires-action channel, and reports the real method type.
func TestGateway_Charge_Konbini(t *testing.T) {
	f := newFakeStripe(t)
	f.on("GET /v1/payment_methods/pm_konbini", pmJSON("pm_konbini", "konbini", nil))
	obj := map[string]any{
		"id": "pi_k", "object": "payment_intent", "amount": 5000, "currency": "jpy",
		"status": "requires_action", "created": 1_700_000_000,
		"next_action": map[string]any{
			"type": "konbini_display_details",
			"konbini_display_details": map[string]any{
				"hosted_voucher_url": "https://payments.stripe.com/konbini/voucher/abc",
				"expires_at":         1_700_300_000,
			},
		},
	}
	b, _ := json.Marshal(obj)
	f.on("POST /v1/payment_intents", string(b))
	g := f.gateway(WithClock(fixedClock))

	pmID := "pm_konbini"
	resp, err := g.Charge(context.Background(), &port.ChargeRequest{
		Amount: jpy(5000), CustomerID: "cus_1", PaymentMethodID: &pmID,
	})
	if err != nil {
		t.Fatalf("Charge: %v", err)
	}
	if got := f.lastForm.Get("payment_method_types[0]"); got != "konbini" {
		t.Errorf("payment_method_types[0] = %q, want konbini", got)
	}
	if _, ok := f.lastForm["automatic_payment_methods[enabled]"]; ok {
		t.Errorf("automatic_payment_methods must not be sent with explicit payment_method_types")
	}
	if resp.Status != port.TransactionStatusRequiresAction {
		t.Errorf("Status = %q, want requires_action", resp.Status)
	}
	if resp.PaymentMethodType != port.PaymentMethodTypeConvenienceStore {
		t.Errorf("PaymentMethodType = %q, want convenience_store", resp.PaymentMethodType)
	}
	if resp.ThreeDSecure == nil || resp.ThreeDSecure.RedirectURL == nil {
		t.Fatalf("expected voucher URL in the requires-action result")
	}
	if *resp.ThreeDSecure.RedirectURL != "https://payments.stripe.com/konbini/voucher/abc" {
		t.Errorf("voucher URL = %q", *resp.ThreeDSecure.RedirectURL)
	}
}

// TestGateway_Charge_BankTransfer verifies a customer_balance charge pins the
// type and configures jp_bank_transfer funding, and surfaces the hosted
// bank-transfer instructions URL through the requires-action channel.
func TestGateway_Charge_BankTransfer(t *testing.T) {
	f := newFakeStripe(t)
	f.on("GET /v1/payment_methods/pm_cb", pmJSON("pm_cb", "customer_balance", nil))
	obj := map[string]any{
		"id": "pi_bt", "object": "payment_intent", "amount": 120000, "currency": "jpy",
		"status": "requires_action", "created": 1_700_000_000,
		"next_action": map[string]any{
			"type": "display_bank_transfer_instructions",
			"display_bank_transfer_instructions": map[string]any{
				"type":                    "jp_bank_transfer",
				"hosted_instructions_url": "https://payments.stripe.com/instructions/xyz",
			},
		},
	}
	b, _ := json.Marshal(obj)
	f.on("POST /v1/payment_intents", string(b))
	g := f.gateway(WithClock(fixedClock))

	pmID := "pm_cb"
	resp, err := g.Charge(context.Background(), &port.ChargeRequest{
		Amount: jpy(120000), CustomerID: "cus_1", PaymentMethodID: &pmID,
	})
	if err != nil {
		t.Fatalf("Charge: %v", err)
	}
	if got := f.lastForm.Get("payment_method_types[0]"); got != "customer_balance" {
		t.Errorf("payment_method_types[0] = %q, want customer_balance", got)
	}
	if got := f.lastForm.Get("payment_method_options[customer_balance][funding_type]"); got != "bank_transfer" {
		t.Errorf("funding_type = %q, want bank_transfer", got)
	}
	if got := f.lastForm.Get("payment_method_options[customer_balance][bank_transfer][type]"); got != "jp_bank_transfer" {
		t.Errorf("bank_transfer type = %q, want jp_bank_transfer", got)
	}
	if _, ok := f.lastForm["automatic_payment_methods[enabled]"]; ok {
		t.Errorf("automatic_payment_methods must not be sent with explicit payment_method_types")
	}
	if resp.Status != port.TransactionStatusRequiresAction {
		t.Errorf("Status = %q, want requires_action", resp.Status)
	}
	if resp.PaymentMethodType != port.PaymentMethodTypeBankTransfer {
		t.Errorf("PaymentMethodType = %q, want bank_transfer", resp.PaymentMethodType)
	}
	if resp.ThreeDSecure == nil || resp.ThreeDSecure.RedirectURL == nil ||
		*resp.ThreeDSecure.RedirectURL != "https://payments.stripe.com/instructions/xyz" {
		t.Fatalf("expected hosted instructions URL, got %+v", resp.ThreeDSecure)
	}
}

// TestGateway_Charge_BankTransfer_RequiresCustomer verifies customer_balance
// charges are rejected client-side without a Stripe customer (the received
// funds are tracked on the customer's cash balance).
func TestGateway_Charge_BankTransfer_RequiresCustomer(t *testing.T) {
	f := newFakeStripe(t)
	f.on("GET /v1/payment_methods/pm_cb", pmJSON("pm_cb", "customer_balance", nil))
	g := f.gateway(WithClock(fixedClock))

	pmID := "pm_cb"
	_, err := g.Charge(context.Background(), &port.ChargeRequest{Amount: jpy(1000), PaymentMethodID: &pmID})
	if !errors.Is(err, ErrValidation) {
		t.Fatalf("want ValidationError, got %v", err)
	}
}

// TestGateway_Charge_AsyncMethodNonJPYRejected verifies the JPY-only guard for
// konbini and JP bank transfer fires before any PaymentIntent is created.
func TestGateway_Charge_AsyncMethodNonJPYRejected(t *testing.T) {
	for _, typ := range []string{"konbini", "customer_balance"} {
		t.Run(typ, func(t *testing.T) {
			f := newFakeStripe(t)
			f.on("GET /v1/payment_methods/pm_a", pmJSON("pm_a", typ, nil))
			g := f.gateway(WithClock(fixedClock))

			pmID := "pm_a"
			_, err := g.Charge(context.Background(), &port.ChargeRequest{
				Amount: usd(2599), CustomerID: "cus_1", PaymentMethodID: &pmID,
			})
			var ge *port.GatewayError
			if !errors.As(err, &ge) || ge.Code != port.ErrorCodeCurrencyNotSupported {
				t.Fatalf("want currency_not_supported GatewayError, got %v", err)
			}
		})
	}
}

// TestGateway_Authorize_AsyncMethodsRejected verifies Authorize refuses
// konbini / customer_balance (Stripe has no manual capture for them) with a
// method_not_supported GatewayError instead of silently mis-charging.
func TestGateway_Authorize_AsyncMethodsRejected(t *testing.T) {
	for _, typ := range []string{"konbini", "customer_balance"} {
		t.Run(typ, func(t *testing.T) {
			f := newFakeStripe(t)
			f.on("GET /v1/payment_methods/pm_a", pmJSON("pm_a", typ, nil))
			g := f.gateway(WithClock(fixedClock))

			pmID := "pm_a"
			_, err := g.Authorize(context.Background(), &port.AuthorizeRequest{
				Amount: jpy(1000), CustomerID: "cus_1", PaymentMethodID: &pmID,
			})
			var ge *port.GatewayError
			if !errors.As(err, &ge) || ge.Code != port.ErrorCodeMethodNotSupported {
				t.Fatalf("want method_not_supported GatewayError, got %v", err)
			}
		})
	}
}

// TestGateway_Charge_CardKeepsAllowRedirectsNever is the issue #51 regression
// guard for the method-aware branch: a card charge with no ReturnURL must
// still pin automatic_payment_methods with allow_redirects=never, must not
// send payment_method_types, and must still report a card type.
func TestGateway_Charge_CardKeepsAllowRedirectsNever(t *testing.T) {
	f := newFakeStripe(t)
	f.onCardPM("pm_card")
	f.on("POST /v1/payment_intents", piJSON("pi_1", "succeeded", 1000, "jpy"))
	g := f.gateway(WithClock(fixedClock))

	pmID := "pm_card"
	resp, err := g.Charge(context.Background(), &port.ChargeRequest{Amount: jpy(1000), PaymentMethodID: &pmID})
	if err != nil {
		t.Fatalf("Charge: %v", err)
	}
	if got := f.lastForm.Get("automatic_payment_methods[allow_redirects]"); got != "never" {
		t.Errorf("allow_redirects = %q, want never", got)
	}
	if _, ok := f.lastForm["payment_method_types[0]"]; ok {
		t.Errorf("payment_method_types must not be sent for card charges")
	}
	if resp.PaymentMethodType != port.PaymentMethodTypeCreditCard {
		t.Errorf("PaymentMethodType = %q, want credit_card", resp.PaymentMethodType)
	}
}

// TestGateway_RegisterPaymentMethod_TypeHonored verifies req.Type gates the
// attach: cards (and the unspecified backward-compat default) attach,
// single-use async types and unimplemented types are rejected with
// method_not_supported before any API call.
func TestGateway_RegisterPaymentMethod_TypeHonored(t *testing.T) {
	t.Run("credit_card attaches", func(t *testing.T) {
		f := newFakeStripe(t)
		f.on("POST /v1/payment_methods/pm_1/attach", pmJSON("pm_1", "card", map[string]any{
			"customer": map[string]any{"id": "cus_1"},
			"card":     map[string]any{"brand": "visa", "last4": "4242"},
		}))
		g := f.gateway(WithClock(fixedClock))

		detail, err := g.RegisterPaymentMethod(context.Background(), &port.RegisterPaymentMethodRequest{
			CustomerID: "cus_1", Token: "pm_1", Type: port.PaymentMethodTypeCreditCard,
		})
		if err != nil {
			t.Fatalf("RegisterPaymentMethod: %v", err)
		}
		if detail.Type != port.PaymentMethodTypeCreditCard {
			t.Errorf("Type = %q, want credit_card", detail.Type)
		}
	})

	rejected := []port.PaymentMethodType{
		port.PaymentMethodTypeConvenienceStore,
		port.PaymentMethodTypeBankTransfer,
		port.PaymentMethodTypeQRCode,
	}
	for _, typ := range rejected {
		t.Run(string(typ)+" rejected", func(t *testing.T) {
			// No routes registered: a rejected type must not hit the API at all
			// (the fake fatals on any unexpected request).
			f := newFakeStripe(t)
			g := f.gateway(WithClock(fixedClock))

			_, err := g.RegisterPaymentMethod(context.Background(), &port.RegisterPaymentMethodRequest{
				CustomerID: "cus_1", Token: "pm_1", Type: typ,
			})
			var ge *port.GatewayError
			if !errors.As(err, &ge) || ge.Code != port.ErrorCodeMethodNotSupported {
				t.Fatalf("want method_not_supported GatewayError, got %v", err)
			}
		})
	}
}

// TestGateway_ListPaymentMethods_MultiType verifies the list no longer
// hard-filters to card: no type param is sent, and every attached method is
// returned with its mapped port type (unknown types pass through by raw name).
func TestGateway_ListPaymentMethods_MultiType(t *testing.T) {
	f := newFakeStripe(t)
	list := map[string]any{
		"object": "list", "has_more": false, "url": "/v1/payment_methods",
		"data": []any{
			map[string]any{"id": "pm_card1", "object": "payment_method", "type": "card", "created": 1_700_000_000,
				"card": map[string]any{"brand": "visa", "last4": "4242", "funding": "credit"}},
			map[string]any{"id": "pm_usb", "object": "payment_method", "type": "us_bank_account", "created": 1_700_000_000,
				"billing_details": map[string]any{"name": "Taro Yamada"},
				"us_bank_account": map[string]any{
					"bank_name": "STRIPE TEST BANK", "routing_number": "110000000",
					"account_type": "checking", "last4": "6789",
				}},
			map[string]any{"id": "pm_pp", "object": "payment_method", "type": "paypay", "created": 1_700_000_000},
			map[string]any{"id": "pm_link", "object": "payment_method", "type": "link", "created": 1_700_000_000},
		},
	}
	lb, _ := json.Marshal(list)
	f.routes["GET /v1/payment_methods"] = func(w http.ResponseWriter, r *http.Request) {
		if got := r.URL.Query().Get("type"); got != "" {
			t.Errorf("type query param = %q, want none (all attached types)", got)
		}
		w.Header().Set("Content-Type", "application/json")
		_, _ = w.Write(lb)
	}
	f.on("GET /v1/customers/cus_1", `{"id":"cus_1","object":"customer"}`)
	g := f.gateway(WithClock(fixedClock))

	methods, err := g.ListPaymentMethods(context.Background(), "cus_1")
	if err != nil {
		t.Fatalf("ListPaymentMethods: %v", err)
	}
	if len(methods) != 4 {
		t.Fatalf("len = %d, want 4", len(methods))
	}
	if methods[0].Type != port.PaymentMethodTypeCreditCard || methods[0].Card == nil {
		t.Errorf("card method = %+v", methods[0])
	}
	if methods[1].Type != port.PaymentMethodTypeDirectDebit {
		t.Errorf("us_bank_account Type = %q, want direct_debit", methods[1].Type)
	}
	ba := methods[1].BankAccount
	if ba == nil {
		t.Fatalf("us_bank_account should populate BankAccount details")
	}
	if ba.BankName != "STRIPE TEST BANK" || ba.BankCode != "110000000" ||
		ba.AccountType != "checking" || ba.AccountNumber != "6789" || ba.AccountHolder != "Taro Yamada" {
		t.Errorf("BankAccount = %+v", ba)
	}
	if methods[2].Type != port.PaymentMethodTypeQRCode {
		t.Errorf("paypay Type = %q, want qr_code", methods[2].Type)
	}
	if methods[2].QRCode == nil || methods[2].QRCode.Provider != "paypay" {
		t.Errorf("paypay QRCode = %+v, want provider paypay", methods[2].QRCode)
	}
	// Unknown Stripe types degrade to a typed pass-through, not a fake card.
	if methods[3].Type != port.PaymentMethodType("link") {
		t.Errorf("link Type = %q, want raw pass-through", methods[3].Type)
	}
	if methods[3].Card != nil || methods[3].BankAccount != nil {
		t.Errorf("link method should carry no card/bank details")
	}
}

// TestGateway_GetPaymentMethod_Konbini verifies non-card detail mapping on the
// single-get path.
func TestGateway_GetPaymentMethod_Konbini(t *testing.T) {
	f := newFakeStripe(t)
	f.on("GET /v1/payment_methods/pm_k", pmJSON("pm_k", "konbini", nil))
	g := f.gateway(WithClock(fixedClock))

	detail, err := g.GetPaymentMethod(context.Background(), "pm_k")
	if err != nil {
		t.Fatalf("GetPaymentMethod: %v", err)
	}
	if detail.Type != port.PaymentMethodTypeConvenienceStore {
		t.Errorf("Type = %q, want convenience_store", detail.Type)
	}
	if detail.Card != nil {
		t.Errorf("konbini detail should carry no card details")
	}
}

// --- PayPay (qr_code) ---

// TestGateway_Charge_PayPay verifies a paypay charge pins the type in
// payment_method_types, forwards the required return_url, surfaces the PayPay
// approval redirect URL through the requires-action channel, and reports
// PaymentMethodType qr_code.
func TestGateway_Charge_PayPay(t *testing.T) {
	f := newFakeStripe(t)
	f.on("GET /v1/payment_methods/pm_pp", pmJSON("pm_pp", "paypay", nil))
	obj := map[string]any{
		"id": "pi_pp", "object": "payment_intent", "amount": 3000, "currency": "jpy",
		"status": "requires_action", "created": 1_700_000_000,
		"next_action": map[string]any{
			"type":            "redirect_to_url",
			"redirect_to_url": map[string]any{"url": "https://hooks.stripe.com/redirect/paypay/abc"},
		},
	}
	b, _ := json.Marshal(obj)
	f.on("POST /v1/payment_intents", string(b))
	g := f.gateway(WithClock(fixedClock))

	pmID := "pm_pp"
	resp, err := g.Charge(context.Background(), &port.ChargeRequest{
		Amount: jpy(3000), CustomerID: "cus_1", PaymentMethodID: &pmID,
		ThreeDSecure: &port.ThreeDSecureRequest{ReturnURL: "https://app/paypay/return"},
	})
	if err != nil {
		t.Fatalf("Charge: %v", err)
	}
	if got := f.lastForm.Get("payment_method_types[0]"); got != "paypay" {
		t.Errorf("payment_method_types[0] = %q, want paypay", got)
	}
	if got := f.lastForm.Get("return_url"); got != "https://app/paypay/return" {
		t.Errorf("return_url = %q", got)
	}
	if _, ok := f.lastForm["automatic_payment_methods[enabled]"]; ok {
		t.Errorf("automatic_payment_methods must not be sent with explicit payment_method_types")
	}
	// The card-only 3DS challenge option must not leak onto the paypay intent.
	if _, ok := f.lastForm["payment_method_options[card][request_three_d_secure]"]; ok {
		t.Errorf("request_three_d_secure must not be sent for paypay")
	}
	if resp.Status != port.TransactionStatusRequiresAction {
		t.Errorf("Status = %q, want requires_action", resp.Status)
	}
	if resp.PaymentMethodType != port.PaymentMethodTypeQRCode {
		t.Errorf("PaymentMethodType = %q, want qr_code", resp.PaymentMethodType)
	}
	if resp.ThreeDSecure == nil || resp.ThreeDSecure.RedirectURL == nil ||
		*resp.ThreeDSecure.RedirectURL != "https://hooks.stripe.com/redirect/paypay/abc" {
		t.Fatalf("expected PayPay approval redirect URL, got %+v", resp.ThreeDSecure)
	}
}

// TestGateway_Charge_PayPay_RequiresReturnURL verifies a paypay charge without
// a return URL fails client-side with a ValidationError instead of creating an
// unconfirmable PaymentIntent (only the PM lookup route is registered — the
// fake fatals if the intent creation is attempted).
func TestGateway_Charge_PayPay_RequiresReturnURL(t *testing.T) {
	cases := map[string]*port.ThreeDSecureRequest{
		"nil ThreeDSecure": nil,
		"empty ReturnURL":  {Required: false, ReturnURL: ""},
	}
	for name, tds := range cases {
		t.Run(name, func(t *testing.T) {
			f := newFakeStripe(t)
			f.on("GET /v1/payment_methods/pm_pp", pmJSON("pm_pp", "paypay", nil))
			g := f.gateway(WithClock(fixedClock))

			pmID := "pm_pp"
			_, err := g.Charge(context.Background(), &port.ChargeRequest{
				Amount: jpy(3000), PaymentMethodID: &pmID, ThreeDSecure: tds,
			})
			if !errors.Is(err, ErrValidation) {
				t.Fatalf("want ValidationError, got %v", err)
			}
		})
	}
}

// TestGateway_Charge_PayPay_NonJPYRejected verifies the JPY-only guard.
func TestGateway_Charge_PayPay_NonJPYRejected(t *testing.T) {
	f := newFakeStripe(t)
	f.on("GET /v1/payment_methods/pm_pp", pmJSON("pm_pp", "paypay", nil))
	g := f.gateway(WithClock(fixedClock))

	pmID := "pm_pp"
	_, err := g.Charge(context.Background(), &port.ChargeRequest{
		Amount: usd(2599), PaymentMethodID: &pmID,
		ThreeDSecure: &port.ThreeDSecureRequest{ReturnURL: "https://app/return"},
	})
	var ge *port.GatewayError
	if !errors.As(err, &ge) || ge.Code != port.ErrorCodeCurrencyNotSupported {
		t.Fatalf("want currency_not_supported GatewayError, got %v", err)
	}
}

// TestGateway_Authorize_PayPayRejected verifies the conservative charge-only
// stance: stripe-go v82 exposes no manual-capture surface for paypay, so
// Authorize rejects it rather than risking a mis-charge.
func TestGateway_Authorize_PayPayRejected(t *testing.T) {
	f := newFakeStripe(t)
	f.on("GET /v1/payment_methods/pm_pp", pmJSON("pm_pp", "paypay", nil))
	g := f.gateway(WithClock(fixedClock))

	pmID := "pm_pp"
	_, err := g.Authorize(context.Background(), &port.AuthorizeRequest{
		Amount: jpy(3000), PaymentMethodID: &pmID,
	})
	var ge *port.GatewayError
	if !errors.As(err, &ge) || ge.Code != port.ErrorCodeMethodNotSupported {
		t.Fatalf("want method_not_supported GatewayError, got %v", err)
	}
}

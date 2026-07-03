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
}

func TestGateway_Charge_CardDeclined(t *testing.T) {
	f := newFakeStripe(t)
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
}

func TestGateway_IDAndSupportedMethods(t *testing.T) {
	g := (&fakeStripe{}).gatewayNoServer(t)
	if g.ID() != "stripe" {
		t.Errorf("ID = %q", g.ID())
	}
	methods := g.SupportedMethods()
	if len(methods) == 0 || methods[0] != port.PaymentMethodTypeCreditCard {
		t.Errorf("SupportedMethods = %v", methods)
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

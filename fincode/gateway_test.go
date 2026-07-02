package fincode

import (
	"context"
	"encoding/json"
	"errors"
	"math/big"
	"net/http"
	"net/http/httptest"
	"strconv"
	"strings"
	"testing"
	"time"

	"github.com/contract-to-cash/core/application/port"
	"github.com/contract-to-cash/core/domain/shared"
)

var gwFixedTime = time.Date(2026, 6, 1, 12, 0, 0, 0, time.UTC)

func jpy(amount int64) shared.Money {
	return shared.NewMoney(new(big.Rat).SetInt64(amount), shared.CurrencyJPY)
}

func strPtr(s string) *string { return &s }

// compile-time conformance check.
var _ port.PaymentGateway = (*Gateway)(nil)

// fakeFincode is a minimal fincode-like API server.
type fakeFincode struct {
	createCalled   bool
	executeCalled  bool
	captureCalled  bool
	cancelCalled   bool
	changeCalled   bool
	retrieveCalled bool

	cardCreateCalled bool
	cardDeleteCalled bool
	cardGetCalled    bool
	cardListCalled   bool

	lastCreateJobCode JobCode
	lastCreate        CreatePaymentRequest
	lastExecute       ExecutePaymentRequest
	lastChange        ChangeAmountRequest
	lastCardCreate    CreateCardRequest

	// currentTotal / currentStatus drive GET /v1/payments/{id}.
	currentTotal  int64
	currentStatus PaymentStatus

	idempotencyHeader        string // POST /v1/payments (register)
	captureIdempotencyHeader string // PUT .../capture
	cancelIdempotencyHeader  string // PUT .../cancel
	changeIdempotencyHeader  string // PUT .../change
}

func (f *fakeFincode) paymentJSON(status PaymentStatus, total int64) PaymentResponse {
	return PaymentResponse{
		ID: "o_gw_order_001", AccessID: "a_gw_access_001",
		Amount: total, TotalAmount: total,
		Status: status, PayType: PayTypeCard,
		CustomerID: "cust_001", CardID: "card_001",
		TransactionID: "txn_gw_001",
		Created:       "2026/06/01 12:00:00.000",
		Updated:       "2026/06/01 12:00:00.000",
	}
}

func (f *fakeFincode) handler() http.Handler {
	return http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		w.Header().Set("Content-Type", "application/json")

		switch {
		case r.Method == http.MethodPost && r.URL.Path == "/v1/payments":
			f.createCalled = true
			f.idempotencyHeader = r.Header.Get("idempotent_key")
			_ = json.NewDecoder(r.Body).Decode(&f.lastCreate)
			f.lastCreateJobCode = f.lastCreate.JobCode
			resp := f.paymentJSON(StatusUnprocessed, 1000)
			resp.JobCode = f.lastCreate.JobCode
			_ = json.NewEncoder(w).Encode(resp)

		case r.Method == http.MethodPut && r.URL.Path == "/v1/payments/o_gw_order_001":
			f.executeCalled = true
			_ = json.NewDecoder(r.Body).Decode(&f.lastExecute)
			status := StatusCaptured
			if f.lastCreateJobCode == JobCodeAuth {
				status = StatusAuthorized
			}
			resp := f.paymentJSON(status, 1000)
			resp.AuthMaxDate = "2026/08/30"
			_ = json.NewEncoder(w).Encode(resp)

		case r.Method == http.MethodGet && r.URL.Path == "/v1/payments/o_gw_order_001":
			f.retrieveCalled = true
			total := f.currentTotal
			if total == 0 {
				total = 1000
			}
			status := f.currentStatus
			if status == "" {
				status = StatusCaptured
			}
			_ = json.NewEncoder(w).Encode(f.paymentJSON(status, total))

		case r.Method == http.MethodPut && r.URL.Path == "/v1/payments/o_gw_order_001/capture":
			f.captureCalled = true
			f.captureIdempotencyHeader = r.Header.Get("idempotent_key")
			_ = json.NewEncoder(w).Encode(f.paymentJSON(StatusCaptured, 1000))

		case r.Method == http.MethodPut && r.URL.Path == "/v1/payments/o_gw_order_001/cancel":
			f.cancelCalled = true
			f.cancelIdempotencyHeader = r.Header.Get("idempotent_key")
			_ = json.NewEncoder(w).Encode(f.paymentJSON(StatusCanceled, 0))

		case r.Method == http.MethodPut && r.URL.Path == "/v1/payments/o_gw_order_001/change":
			f.changeCalled = true
			f.changeIdempotencyHeader = r.Header.Get("idempotent_key")
			_ = json.NewDecoder(r.Body).Decode(&f.lastChange)
			newTotal, _ := strconv.ParseInt(f.lastChange.Amount, 10, 64)
			_ = json.NewEncoder(w).Encode(f.paymentJSON(StatusCaptured, newTotal))

		// --- customer cards ---
		case r.Method == http.MethodPost && r.URL.Path == "/v1/customers/cust_001/cards":
			f.cardCreateCalled = true
			_ = json.NewDecoder(r.Body).Decode(&f.lastCardCreate)
			_ = json.NewEncoder(w).Encode(CardResponse{
				CustomerID: "cust_001", ID: "card_001",
				DefaultFlag: f.lastCardCreate.DefaultFlag,
				CardNo:      "************1234", Expire: "2907",
				Brand:   "VISA",
				Created: "2026/06/01 12:00:00.000",
			})

		case r.Method == http.MethodGet && r.URL.Path == "/v1/customers/cust_001/cards/card_001":
			f.cardGetCalled = true
			_ = json.NewEncoder(w).Encode(CardResponse{
				CustomerID: "cust_001", ID: "card_001",
				DefaultFlag: "1", CardNo: "************1234", Expire: "2907",
				Brand: "MASTER", Created: "2026/06/01 12:00:00.000",
			})

		case r.Method == http.MethodGet && r.URL.Path == "/v1/customers/cust_001/cards":
			f.cardListCalled = true
			_ = json.NewEncoder(w).Encode(CardListResponse{List: []CardResponse{
				{CustomerID: "cust_001", ID: "card_001", Brand: "VISA", CardNo: "************1111", Expire: "2807"},
				{CustomerID: "cust_001", ID: "card_002", Brand: "JCB", CardNo: "************2222", Expire: "2807"},
			}})

		case r.Method == http.MethodDelete && r.URL.Path == "/v1/customers/cust_001/cards/card_001":
			f.cardDeleteCalled = true
			_ = json.NewEncoder(w).Encode(DeleteCardResponse{
				CustomerID: "cust_001", ID: "card_001", DeleteFlag: "1",
			})

		default:
			w.WriteHeader(http.StatusNotFound)
			_ = json.NewEncoder(w).Encode(ErrorResponse{
				Errors: []APIError{{ErrorCode: "E9999", ErrorMessage: "not found: " + r.Method + " " + r.URL.Path}},
			})
		}
	})
}

func setupGateway(t *testing.T) (*Gateway, *fakeFincode, func()) {
	t.Helper()
	fake := &fakeFincode{}
	srv := httptest.NewServer(fake.handler())
	client := NewClient(Config{APIKey: "sk_test_gw", BaseURL: srv.URL})
	gw := NewGateway(client, WithClock(shared.FixedClock{FixedTime: gwFixedTime}))
	return gw, fake, srv.Close
}

func unreachableGateway() *Gateway {
	client := NewClient(Config{APIKey: "sk", BaseURL: "http://unreachable.invalid"})
	return NewGateway(client, WithClock(shared.FixedClock{FixedTime: gwFixedTime}))
}

// --- ID / SupportedMethods ---

func TestGateway_IDAndSupportedMethods(t *testing.T) {
	gw := unreachableGateway()
	if gw.ID() != "fincode" {
		t.Errorf("ID = %q, want fincode", gw.ID())
	}
	methods := gw.SupportedMethods()
	if len(methods) != 1 || methods[0] != port.PaymentMethodTypeCreditCard {
		t.Errorf("SupportedMethods = %v, want [credit_card] only", methods)
	}
}

// --- Charge ---

func TestGateway_Charge_TokenSuccess(t *testing.T) {
	gw, fake, cleanup := setupGateway(t)
	defer cleanup()

	resp, err := gw.Charge(context.Background(), &port.ChargeRequest{
		Amount:         jpy(1000),
		Token:          strPtr("tok_test_001"),
		IdempotencyKey: "idem-uuid-abc",
		Description:    "June invoice",
	})
	if err != nil {
		t.Fatalf("Charge: %v", err)
	}
	if !fake.createCalled || !fake.executeCalled {
		t.Error("expected register and execute to be called")
	}
	if fake.lastCreateJobCode != JobCodeCapture {
		t.Errorf("job_code = %q, want CAPTURE", fake.lastCreateJobCode)
	}
	if fake.lastCreate.Amount != "1000" {
		t.Errorf("amount = %q, want 1000", fake.lastCreate.Amount)
	}
	if fake.lastCreate.ClientField1 != "June invoice" {
		t.Errorf("client_field_1 = %q, want description", fake.lastCreate.ClientField1)
	}
	if fake.idempotencyHeader != "idem-uuid-abc" {
		t.Errorf("idempotent_key = %q, want idem-uuid-abc", fake.idempotencyHeader)
	}
	if fake.lastExecute.Token != "tok_test_001" {
		t.Errorf("token = %q, want tok_test_001", fake.lastExecute.Token)
	}
	if resp.TransactionID != "o_gw_order_001" {
		t.Errorf("TransactionID = %q, want fincode order id", resp.TransactionID)
	}
	if resp.Status != port.TransactionStatusCaptured {
		t.Errorf("Status = %q, want captured", resp.Status)
	}
	if resp.Amount.Int64() != 1000 || resp.Amount.Currency() != shared.CurrencyJPY {
		t.Errorf("Amount = %v %v, want 1000 JPY", resp.Amount.Int64(), resp.Amount.Currency())
	}
	if resp.PaymentMethodType != port.PaymentMethodTypeCreditCard {
		t.Errorf("PaymentMethodType = %q, want credit_card", resp.PaymentMethodType)
	}
	if resp.PaymentMethodID != "cust_001/card_001" {
		t.Errorf("PaymentMethodID = %q, want composite cust_001/card_001", resp.PaymentMethodID)
	}
	// Created is JST 2026/06/01 12:00:00 → 03:00 UTC.
	wantCreated := time.Date(2026, 6, 1, 3, 0, 0, 0, time.UTC)
	if !resp.CreatedAt.Equal(wantCreated) {
		t.Errorf("CreatedAt = %v, want %v (JST parsed to UTC)", resp.CreatedAt, wantCreated)
	}
}

func TestGateway_Charge_StoredCardByPaymentMethodID(t *testing.T) {
	gw, fake, cleanup := setupGateway(t)
	defer cleanup()

	_, err := gw.Charge(context.Background(), &port.ChargeRequest{
		Amount:          jpy(1000),
		PaymentMethodID: strPtr("cust_001/card_001"),
	})
	if err != nil {
		t.Fatalf("Charge: %v", err)
	}
	if fake.lastExecute.CustomerID != "cust_001" || fake.lastExecute.CardID != "card_001" {
		t.Errorf("execute customer/card = %q/%q, want cust_001/card_001",
			fake.lastExecute.CustomerID, fake.lastExecute.CardID)
	}
	if fake.lastExecute.Token != "" {
		t.Errorf("token should be empty, got %q", fake.lastExecute.Token)
	}
}

func TestGateway_Charge_CustomerDefaultCard(t *testing.T) {
	gw, fake, cleanup := setupGateway(t)
	defer cleanup()

	_, err := gw.Charge(context.Background(), &port.ChargeRequest{
		Amount:     jpy(1000),
		CustomerID: "cust_001",
	})
	if err != nil {
		t.Fatalf("Charge: %v", err)
	}
	if fake.lastExecute.CustomerID != "cust_001" || fake.lastExecute.CardID != "" {
		t.Errorf("execute customer/card = %q/%q, want cust_001/(empty = default card)",
			fake.lastExecute.CustomerID, fake.lastExecute.CardID)
	}
}

func TestGateway_Charge_RequiresPaymentSource(t *testing.T) {
	gw := unreachableGateway()
	_, err := gw.Charge(context.Background(), &port.ChargeRequest{Amount: jpy(100)})
	if err == nil {
		t.Fatal("expected validation error")
	}
	var ve *ValidationError
	if !errors.As(err, &ve) {
		t.Fatalf("expected *ValidationError, got %T: %v", err, err)
	}
	if !errors.Is(err, ErrValidation) {
		t.Error("expected errors.Is(err, ErrValidation)")
	}
}

func TestGateway_Charge_RejectsNonJPY(t *testing.T) {
	gw := unreachableGateway()
	_, err := gw.Charge(context.Background(), &port.ChargeRequest{
		Amount: shared.NewMoney(big.NewRat(100, 1), shared.CurrencyUSD),
		Token:  strPtr("tok"),
	})
	var ge *port.GatewayError
	if !errors.As(err, &ge) {
		t.Fatalf("expected *port.GatewayError, got %T: %v", err, err)
	}
	if ge.Code != port.ErrorCodeCurrencyNotSupported {
		t.Errorf("Code = %q, want currency_not_supported", ge.Code)
	}
}

func TestGateway_Charge_RejectsZeroAmount(t *testing.T) {
	gw := unreachableGateway()
	_, err := gw.Charge(context.Background(), &port.ChargeRequest{
		Amount: jpy(0),
		Token:  strPtr("tok"),
	})
	var ve *ValidationError
	if !errors.As(err, &ve) {
		t.Fatalf("expected *ValidationError, got %T: %v", err, err)
	}
}

func TestGateway_Charge_Rejects3DS(t *testing.T) {
	gw := unreachableGateway()
	_, err := gw.Charge(context.Background(), &port.ChargeRequest{
		Amount:       jpy(100),
		Token:        strPtr("tok"),
		ThreeDSecure: &port.ThreeDSecureRequest{Required: true},
	})
	var ge *port.GatewayError
	if !errors.As(err, &ge) || ge.Code != port.ErrorCodeMethodNotSupported {
		t.Fatalf("expected method_not_supported GatewayError, got %v", err)
	}
}

// Amounts outside int64 must be rejected, not silently wrapped: big.Rat →
// int64 conversion without a range check would turn 2^64+100 yen into a
// 100 yen charge.
func TestGateway_Charge_RejectsAmountBeyondInt64(t *testing.T) {
	gw := unreachableGateway()
	huge := new(big.Int).Add(new(big.Int).Lsh(big.NewInt(1), 64), big.NewInt(100)) // 2^64 + 100
	// Sanity: this is exactly the silent-wrap hazard.
	if got := new(big.Int).SetInt64(huge.Int64()); got.Cmp(big.NewInt(100)) != 0 {
		t.Fatalf("test premise: (2^64+100).Int64() = %v, want wrapped 100", got)
	}
	amount := shared.NewMoney(new(big.Rat).SetInt(huge), shared.CurrencyJPY)

	_, err := gw.Charge(context.Background(), &port.ChargeRequest{
		Amount: amount,
		Token:  strPtr("tok"),
	})
	var ve *ValidationError
	if !errors.As(err, &ve) {
		t.Fatalf("expected *ValidationError, got %T: %v", err, err)
	}
}

// The overflow guard applies to every amount path, including the ones that
// read the amount after a successful retrieve (Capture, Refund).
func TestGateway_OverflowAmountRejectedOnAllPaths(t *testing.T) {
	gw, fake, cleanup := setupGateway(t)
	defer cleanup()
	fake.currentStatus = StatusAuthorized
	fake.currentTotal = 1000

	huge := shared.NewMoney(
		new(big.Rat).SetInt(new(big.Int).Add(new(big.Int).Lsh(big.NewInt(1), 64), big.NewInt(100))),
		shared.CurrencyJPY)

	var ve *ValidationError

	_, err := gw.Authorize(context.Background(), &port.AuthorizeRequest{Amount: huge, Token: strPtr("tok")})
	if !errors.As(err, &ve) {
		t.Errorf("Authorize: expected *ValidationError, got %v", err)
	}
	_, err = gw.Capture(context.Background(), &port.CaptureRequest{
		AuthorizationID: "o_gw_order_001", Amount: &huge,
	})
	if !errors.As(err, &ve) {
		t.Errorf("Capture: expected *ValidationError, got %v", err)
	}
	_, err = gw.Refund(context.Background(), &port.RefundRequest{
		TransactionID: "o_gw_order_001", Amount: &huge,
	})
	if !errors.As(err, &ve) {
		t.Errorf("Refund: expected *ValidationError, got %v", err)
	}
	if fake.captureCalled || fake.changeCalled || fake.cancelCalled {
		t.Error("no mutating fincode call may be made with an overflowing amount")
	}
}

// --- Idempotency (order ID derivation) ---

func TestDeriveOrderID(t *testing.T) {
	id := deriveOrderID("idem-uuid-abc")
	if len(id) != 30 {
		t.Errorf("len = %d, want 30", len(id))
	}
	for _, r := range id {
		if (r < 'a' || r > 'z') && (r < '0' || r > '9') {
			t.Errorf("unexpected character %q in derived order ID %q", r, id)
		}
	}
	if id != deriveOrderID("idem-uuid-abc") {
		t.Error("derivation must be deterministic")
	}
	if id == deriveOrderID("idem-uuid-abd") {
		t.Error("distinct keys must derive distinct order IDs")
	}
	if deriveOrderID("") != "" {
		t.Error("empty key must not derive an order ID (fincode assigns it)")
	}
}

func TestDeriveIdempotencyKey(t *testing.T) {
	k := deriveIdempotencyKey("idem-cap", "change")
	if len(k) != 30 {
		t.Errorf("len = %d, want 30", len(k))
	}
	if k != deriveIdempotencyKey("idem-cap", "change") {
		t.Error("derivation must be deterministic")
	}
	if k == deriveIdempotencyKey("idem-cap", "capture") {
		t.Error("distinct ops must derive distinct keys")
	}
	if k == deriveIdempotencyKey("idem-cab", "change") {
		t.Error("distinct base keys must derive distinct keys")
	}
	if k == "idem-cap" {
		t.Error("derived key must differ from the caller's key")
	}
	if deriveIdempotencyKey("", "change") != "" {
		t.Error("empty key must derive an empty key (header omitted)")
	}
}

// A caller-supplied IdempotencyKey must fix the fincode order ID so a retried
// Charge re-registers the same order (permanent dedup, beyond the 30-minute
// idempotent_key header window).
func TestGateway_Charge_IdempotencyKeyFixesOrderID(t *testing.T) {
	gw, fake, cleanup := setupGateway(t)
	defer cleanup()

	_, err := gw.Charge(context.Background(), &port.ChargeRequest{
		Amount:         jpy(1000),
		Token:          strPtr("tok"),
		IdempotencyKey: "idem-uuid-abc",
	})
	if err != nil {
		t.Fatalf("Charge: %v", err)
	}
	if fake.lastCreate.ID != deriveOrderID("idem-uuid-abc") {
		t.Errorf("register body id = %q, want derived %q", fake.lastCreate.ID, deriveOrderID("idem-uuid-abc"))
	}
	// The header-based short-window idempotency is still forwarded.
	if fake.idempotencyHeader != "idem-uuid-abc" {
		t.Errorf("idempotent_key = %q, want idem-uuid-abc", fake.idempotencyHeader)
	}
}

func TestGateway_Charge_NoIdempotencyKey_FincodeAssignsOrderID(t *testing.T) {
	gw, fake, cleanup := setupGateway(t)
	defer cleanup()

	_, err := gw.Charge(context.Background(), &port.ChargeRequest{
		Amount: jpy(1000),
		Token:  strPtr("tok"),
	})
	if err != nil {
		t.Fatalf("Charge: %v", err)
	}
	if fake.lastCreate.ID != "" {
		t.Errorf("register body id = %q, want empty (fincode-assigned)", fake.lastCreate.ID)
	}
}

// Capture / Void / Cancel / Refund must forward req.IdempotencyKey as the
// fincode idempotent_key header instead of discarding it.
func TestGateway_MutatingCalls_ForwardIdempotencyKey(t *testing.T) {
	// Capture issues up to two mutating calls from one request; each must get
	// its own derived idempotent_key (fincode's replay scope across endpoints
	// is unconfirmed, and replaying the /change response on /capture would
	// skip the real capture).
	t.Run("capture", func(t *testing.T) {
		gw, fake, cleanup := setupGateway(t)
		defer cleanup()
		fake.currentStatus = StatusAuthorized
		fake.currentTotal = 1000

		amount := jpy(600)
		_, err := gw.Capture(context.Background(), &port.CaptureRequest{
			AuthorizationID: "o_gw_order_001",
			Amount:          &amount,
			IdempotencyKey:  "idem-cap",
		})
		if err != nil {
			t.Fatalf("Capture: %v", err)
		}
		if want := deriveIdempotencyKey("idem-cap", "change"); fake.changeIdempotencyHeader != want {
			t.Errorf("change idempotent_key = %q, want derived %q", fake.changeIdempotencyHeader, want)
		}
		if want := deriveIdempotencyKey("idem-cap", "capture"); fake.captureIdempotencyHeader != want {
			t.Errorf("capture idempotent_key = %q, want derived %q", fake.captureIdempotencyHeader, want)
		}
		if fake.changeIdempotencyHeader == "" || fake.captureIdempotencyHeader == "" {
			t.Error("both calls must carry an idempotent_key")
		}
		if fake.changeIdempotencyHeader == fake.captureIdempotencyHeader {
			t.Error("/change and /capture must NOT share the same idempotent_key")
		}
	})

	t.Run("void", func(t *testing.T) {
		gw, fake, cleanup := setupGateway(t)
		defer cleanup()
		fake.currentStatus = StatusAuthorized

		_, err := gw.Void(context.Background(), &port.VoidRequest{
			AuthorizationID: "o_gw_order_001",
			IdempotencyKey:  "idem-void",
		})
		if err != nil {
			t.Fatalf("Void: %v", err)
		}
		if fake.cancelIdempotencyHeader != "idem-void" {
			t.Errorf("cancel idempotent_key = %q, want idem-void", fake.cancelIdempotencyHeader)
		}
	})

	t.Run("cancel", func(t *testing.T) {
		gw, fake, cleanup := setupGateway(t)
		defer cleanup()

		_, err := gw.Cancel(context.Background(), &port.CancelRequest{
			TransactionID:  "o_gw_order_001",
			IdempotencyKey: "idem-cancel",
		})
		if err != nil {
			t.Fatalf("Cancel: %v", err)
		}
		if fake.cancelIdempotencyHeader != "idem-cancel" {
			t.Errorf("cancel idempotent_key = %q, want idem-cancel", fake.cancelIdempotencyHeader)
		}
	})

	t.Run("refund full", func(t *testing.T) {
		gw, fake, cleanup := setupGateway(t)
		defer cleanup()
		fake.currentTotal = 1000

		_, err := gw.Refund(context.Background(), &port.RefundRequest{
			TransactionID:  "o_gw_order_001",
			IdempotencyKey: "idem-refund",
		})
		if err != nil {
			t.Fatalf("Refund: %v", err)
		}
		if fake.cancelIdempotencyHeader != "idem-refund" {
			t.Errorf("cancel idempotent_key = %q, want idem-refund", fake.cancelIdempotencyHeader)
		}
	})

	t.Run("refund partial", func(t *testing.T) {
		gw, fake, cleanup := setupGateway(t)
		defer cleanup()
		fake.currentTotal = 1000

		amount := jpy(300)
		_, err := gw.Refund(context.Background(), &port.RefundRequest{
			TransactionID:  "o_gw_order_001",
			Amount:         &amount,
			IdempotencyKey: "idem-refund-p",
		})
		if err != nil {
			t.Fatalf("Refund: %v", err)
		}
		if fake.changeIdempotencyHeader != "idem-refund-p" {
			t.Errorf("change idempotent_key = %q, want idem-refund-p", fake.changeIdempotencyHeader)
		}
	})
}

// --- Lost-response recovery (CompleteCharge / CompleteAuthorize) ---

// If the original execute succeeded but its response was lost, the payment is
// already CAPTURED: CompleteCharge must return success from the retrieved
// state WITHOUT re-executing (re-execute would fail and misreport a
// successful charge as failed).
func TestGateway_CompleteCharge_AlreadyCaptured_SkipsExecute(t *testing.T) {
	gw, fake, cleanup := setupGateway(t)
	defer cleanup()
	fake.currentStatus = StatusCaptured
	fake.currentTotal = 1000

	resp, err := gw.CompleteCharge(context.Background(), "o_gw_order_001", "a_gw_access_001",
		&port.ChargeRequest{Amount: jpy(1000), Token: strPtr("tok")})
	if err != nil {
		t.Fatalf("CompleteCharge: %v", err)
	}
	if !fake.retrieveCalled {
		t.Error("expected retrieve before deciding to re-execute")
	}
	if fake.executeCalled {
		t.Error("execute must NOT be called when the payment is already captured")
	}
	if resp.Status != port.TransactionStatusCaptured {
		t.Errorf("Status = %q, want captured", resp.Status)
	}
	if resp.TransactionID != "o_gw_order_001" {
		t.Errorf("TransactionID = %q", resp.TransactionID)
	}
}

// An actually-unprocessed payment still goes through the execute retry.
func TestGateway_CompleteCharge_Unprocessed_Executes(t *testing.T) {
	gw, fake, cleanup := setupGateway(t)
	defer cleanup()
	fake.currentStatus = StatusUnprocessed

	resp, err := gw.CompleteCharge(context.Background(), "o_gw_order_001", "a_gw_access_001",
		&port.ChargeRequest{Amount: jpy(1000), Token: strPtr("tok")})
	if err != nil {
		t.Fatalf("CompleteCharge: %v", err)
	}
	if !fake.executeCalled {
		t.Error("expected execute for an unprocessed payment")
	}
	if resp.Status != port.TransactionStatusCaptured {
		t.Errorf("Status = %q, want captured", resp.Status)
	}
}

func TestGateway_CompleteAuthorize_AlreadyAuthorized_SkipsExecute(t *testing.T) {
	gw, fake, cleanup := setupGateway(t)
	defer cleanup()
	fake.currentStatus = StatusAuthorized
	fake.currentTotal = 2000

	resp, err := gw.CompleteAuthorize(context.Background(), "o_gw_order_001", "a_gw_access_001",
		&port.AuthorizeRequest{Amount: jpy(2000), Token: strPtr("tok")})
	if err != nil {
		t.Fatalf("CompleteAuthorize: %v", err)
	}
	if fake.executeCalled {
		t.Error("execute must NOT be called when the payment is already authorized")
	}
	if resp.Status != port.TransactionStatusAuthorized {
		t.Errorf("Status = %q, want authorized", resp.Status)
	}
}

func TestGateway_CompleteAuthorize_Unprocessed_Executes(t *testing.T) {
	gw, fake, cleanup := setupGateway(t)
	defer cleanup()
	fake.currentStatus = StatusUnprocessed

	_, err := gw.CompleteAuthorize(context.Background(), "o_gw_order_001", "a_gw_access_001",
		&port.AuthorizeRequest{Amount: jpy(2000), Token: strPtr("tok")})
	if err != nil {
		t.Fatalf("CompleteAuthorize: %v", err)
	}
	if !fake.executeCalled {
		t.Error("expected execute for an unprocessed payment")
	}
}

// Partial failure (register succeeded, execute failed) surfaces
// *PartialAuthorizeError with the registered IDs; CompleteCharge recovers.
func TestGateway_Charge_PartialFailure_ThenCompleteCharge(t *testing.T) {
	executeShouldFail := true
	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		w.Header().Set("Content-Type", "application/json")
		if r.Method == http.MethodPost && r.URL.Path == "/v1/payments" {
			_ = json.NewEncoder(w).Encode(PaymentResponse{
				ID: "o_partial_001", AccessID: "a_partial_001", Status: StatusUnprocessed,
			})
			return
		}
		if r.Method == http.MethodGet {
			// CompleteCharge checks the current state first; report the
			// payment as still unprocessed so the execute retry path runs.
			_ = json.NewEncoder(w).Encode(PaymentResponse{
				ID: "o_partial_001", AccessID: "a_partial_001", Status: StatusUnprocessed,
			})
			return
		}
		if executeShouldFail {
			w.WriteHeader(http.StatusBadRequest)
			_ = json.NewEncoder(w).Encode(ErrorResponse{
				Errors: []APIError{{ErrorCode: "E0200001", ErrorMessage: "bad token"}},
			})
			return
		}
		_ = json.NewEncoder(w).Encode(PaymentResponse{
			ID: "o_partial_001", AccessID: "a_partial_001", Status: StatusCaptured,
			Amount: 1000, TotalAmount: 1000,
		})
	}))
	defer srv.Close()

	client := NewClient(Config{APIKey: "sk", BaseURL: srv.URL})
	gw := NewGateway(client, WithClock(shared.FixedClock{FixedTime: gwFixedTime}))

	req := &port.ChargeRequest{Amount: jpy(1000), Token: strPtr("tok_retry")}
	_, err := gw.Charge(context.Background(), req)
	if err == nil {
		t.Fatal("expected error")
	}
	var pae *PartialAuthorizeError
	if !errors.As(err, &pae) {
		t.Fatalf("expected *PartialAuthorizeError, got %T: %v", err, err)
	}
	if pae.OrderID != "o_partial_001" || pae.AccessID != "a_partial_001" {
		t.Errorf("PartialAuthorizeError ids = %q/%q", pae.OrderID, pae.AccessID)
	}
	// The cause chain keeps the typed HTTP error.
	var he *HTTPError
	if !errors.As(err, &he) || he.StatusCode != 400 {
		t.Errorf("cause should unwrap to *HTTPError 400, got %v", err)
	}

	// Recovery: execute-only retry.
	executeShouldFail = false
	resp, err := gw.CompleteCharge(context.Background(), pae.OrderID, pae.AccessID, req)
	if err != nil {
		t.Fatalf("CompleteCharge: %v", err)
	}
	if resp.Status != port.TransactionStatusCaptured {
		t.Errorf("Status = %q, want captured", resp.Status)
	}
	if resp.TransactionID != "o_partial_001" {
		t.Errorf("TransactionID = %q, want o_partial_001", resp.TransactionID)
	}
}

// Register failure is NOT partial — no order exists yet.
func TestGateway_Charge_RegisterFailure_IsPlainGatewayError(t *testing.T) {
	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		w.Header().Set("Content-Type", "application/json")
		w.WriteHeader(http.StatusBadRequest)
		_ = json.NewEncoder(w).Encode(ErrorResponse{
			Errors: []APIError{{ErrorCode: "E0100001", ErrorMessage: "amount is required"}},
		})
	}))
	defer srv.Close()

	client := NewClient(Config{APIKey: "sk", BaseURL: srv.URL})
	gw := NewGateway(client)

	_, err := gw.Charge(context.Background(), &port.ChargeRequest{
		Amount: jpy(100), Token: strPtr("tok"),
	})
	var pae *PartialAuthorizeError
	if errors.As(err, &pae) {
		t.Error("register failure must not produce PartialAuthorizeError")
	}
	var ge *port.GatewayError
	if !errors.As(err, &ge) {
		t.Fatalf("expected *port.GatewayError, got %T: %v", err, err)
	}
	if ge.Code != port.ErrorCodeProcessingError || ge.Retryable {
		t.Errorf("4xx should map to non-retryable processing_error, got %+v", ge)
	}
	if ge.DeclineCode != "E0100001" {
		t.Errorf("DeclineCode = %q, want fincode error code E0100001", ge.DeclineCode)
	}
}

// --- Idempotent retry recovery (duplicate order ID / lost execute response) ---

// registerFailFincode simulates a fincode where the register step always
// fails (as it does for a duplicate derived order ID once the idempotent_key
// header TTL has passed) and GET /v1/payments/{id} reports a configurable
// order state.
type registerFailFincode struct {
	getStatus    PaymentStatus // "" → GET answers 404 (order does not exist)
	getCalls     int
	executeCalls int
}

func (f *registerFailFincode) server() *httptest.Server {
	return httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		w.Header().Set("Content-Type", "application/json")
		switch {
		case r.Method == http.MethodPost && r.URL.Path == "/v1/payments":
			w.WriteHeader(http.StatusBadRequest)
			_ = json.NewEncoder(w).Encode(ErrorResponse{
				Errors: []APIError{{ErrorCode: "E_DUP", ErrorMessage: "order id already exists"}},
			})
		case r.Method == http.MethodGet && strings.HasPrefix(r.URL.Path, "/v1/payments/"):
			f.getCalls++
			if f.getStatus == "" {
				w.WriteHeader(http.StatusNotFound)
				_ = json.NewEncoder(w).Encode(ErrorResponse{
					Errors: []APIError{{ErrorCode: "E_NF", ErrorMessage: "order not found"}},
				})
				return
			}
			_ = json.NewEncoder(w).Encode(PaymentResponse{
				ID:       strings.TrimPrefix(r.URL.Path, "/v1/payments/"),
				AccessID: "a_recover_001",
				Status:   f.getStatus,
				Amount:   1000, TotalAmount: 1000,
			})
		default:
			f.executeCalls++
			w.WriteHeader(http.StatusBadRequest)
			_ = json.NewEncoder(w).Encode(ErrorResponse{
				Errors: []APIError{{ErrorCode: "E9999", ErrorMessage: "unexpected call"}},
			})
		}
	}))
}

func recoveryGateway(t *testing.T, fake *registerFailFincode) (*Gateway, func()) {
	t.Helper()
	srv := fake.server()
	client := NewClient(Config{APIKey: "sk", BaseURL: srv.URL})
	return NewGateway(client, WithClock(shared.FixedClock{FixedTime: gwFixedTime})), srv.Close
}

// After the 30-minute idempotent_key TTL, retrying a Charge with the same
// IdempotencyKey makes fincode reject the derived order ID as a duplicate.
// If that order was already captured, the retry must replay the success — not
// misreport a completed payment as a permanent failure.
func TestGateway_Charge_RegisterDuplicate_AlreadyCaptured_ReplaysSuccess(t *testing.T) {
	fake := &registerFailFincode{getStatus: StatusCaptured}
	gw, cleanup := recoveryGateway(t, fake)
	defer cleanup()

	resp, err := gw.Charge(context.Background(), &port.ChargeRequest{
		Amount: jpy(1000), Token: strPtr("tok"), IdempotencyKey: "idem-recover",
	})
	if err != nil {
		t.Fatalf("Charge: %v", err)
	}
	if resp.Status != port.TransactionStatusCaptured {
		t.Errorf("Status = %q, want captured", resp.Status)
	}
	if want := deriveOrderID("idem-recover"); resp.TransactionID != want {
		t.Errorf("TransactionID = %q, want derived order %q", resp.TransactionID, want)
	}
	if fake.executeCalls != 0 {
		t.Error("execute must NOT be called when replaying an existing captured order")
	}
}

// A duplicate rejection for an order that exists but never completed is
// recoverable: surface *PartialAuthorizeError with the derived order ID and
// the access_id obtained from the retrieve, so CompleteCharge can finish it.
func TestGateway_Charge_RegisterDuplicate_Incomplete_IsPartialAuthorizeError(t *testing.T) {
	for _, status := range []PaymentStatus{StatusUnprocessed, StatusAuthorized} {
		t.Run(string(status), func(t *testing.T) {
			fake := &registerFailFincode{getStatus: status}
			gw, cleanup := recoveryGateway(t, fake)
			defer cleanup()

			_, err := gw.Charge(context.Background(), &port.ChargeRequest{
				Amount: jpy(1000), Token: strPtr("tok"), IdempotencyKey: "idem-recover",
			})
			var pae *PartialAuthorizeError
			if !errors.As(err, &pae) {
				t.Fatalf("expected *PartialAuthorizeError, got %T: %v", err, err)
			}
			if want := deriveOrderID("idem-recover"); pae.OrderID != want {
				t.Errorf("OrderID = %q, want derived %q", pae.OrderID, want)
			}
			if pae.AccessID != "a_recover_001" {
				t.Errorf("AccessID = %q, want a_recover_001 (from retrieve)", pae.AccessID)
			}
			if pae.Cause == nil {
				t.Error("Cause must carry the register error")
			}
		})
	}
}

func TestGateway_Authorize_RegisterDuplicate_AlreadyAuthorized_ReplaysSuccess(t *testing.T) {
	fake := &registerFailFincode{getStatus: StatusAuthorized}
	gw, cleanup := recoveryGateway(t, fake)
	defer cleanup()

	resp, err := gw.Authorize(context.Background(), &port.AuthorizeRequest{
		Amount: jpy(1000), Token: strPtr("tok"), IdempotencyKey: "idem-recover-auth",
	})
	if err != nil {
		t.Fatalf("Authorize: %v", err)
	}
	if resp.Status != port.TransactionStatusAuthorized {
		t.Errorf("Status = %q, want authorized", resp.Status)
	}
	if want := deriveOrderID("idem-recover-auth"); resp.AuthorizationID != want {
		t.Errorf("AuthorizationID = %q, want derived order %q", resp.AuthorizationID, want)
	}
}

func TestGateway_Authorize_RegisterDuplicate_Unprocessed_IsPartialAuthorizeError(t *testing.T) {
	fake := &registerFailFincode{getStatus: StatusUnprocessed}
	gw, cleanup := recoveryGateway(t, fake)
	defer cleanup()

	_, err := gw.Authorize(context.Background(), &port.AuthorizeRequest{
		Amount: jpy(1000), Token: strPtr("tok"), IdempotencyKey: "idem-recover-auth",
	})
	var pae *PartialAuthorizeError
	if !errors.As(err, &pae) {
		t.Fatalf("expected *PartialAuthorizeError, got %T: %v", err, err)
	}
	if want := deriveOrderID("idem-recover-auth"); pae.OrderID != want {
		t.Errorf("OrderID = %q, want derived %q", pae.OrderID, want)
	}
}

// A genuine register failure (the derived order does not exist at fincode)
// must surface the ORIGINAL register error.
func TestGateway_Charge_RegisterFailure_OrderNotFound_ReturnsRegisterError(t *testing.T) {
	fake := &registerFailFincode{getStatus: ""} // GET → 404
	gw, cleanup := recoveryGateway(t, fake)
	defer cleanup()

	_, err := gw.Charge(context.Background(), &port.ChargeRequest{
		Amount: jpy(1000), Token: strPtr("tok"), IdempotencyKey: "idem-recover",
	})
	var pae *PartialAuthorizeError
	if errors.As(err, &pae) {
		t.Error("an unconfirmed order must not produce PartialAuthorizeError")
	}
	var ge *port.GatewayError
	if !errors.As(err, &ge) {
		t.Fatalf("expected *port.GatewayError, got %T: %v", err, err)
	}
	if ge.DeclineCode != "E_DUP" {
		t.Errorf("DeclineCode = %q, want the register error code E_DUP", ge.DeclineCode)
	}
	if fake.getCalls != 1 {
		t.Errorf("retrieve calls = %d, want 1", fake.getCalls)
	}
}

// Without an IdempotencyKey there is no deterministic order ID to look up:
// a register failure must not trigger any retrieve.
func TestGateway_Charge_RegisterFailure_NoKey_SkipsRetrieve(t *testing.T) {
	fake := &registerFailFincode{getStatus: StatusCaptured}
	gw, cleanup := recoveryGateway(t, fake)
	defer cleanup()

	_, err := gw.Charge(context.Background(), &port.ChargeRequest{
		Amount: jpy(1000), Token: strPtr("tok"),
	})
	var ge *port.GatewayError
	if !errors.As(err, &ge) {
		t.Fatalf("expected *port.GatewayError, got %T: %v", err, err)
	}
	if fake.getCalls != 0 {
		t.Errorf("retrieve calls = %d, want 0 (no key, no lookup)", fake.getCalls)
	}
}

// executeFailServer simulates a lost execute response: register succeeds,
// execute always fails at the HTTP layer, and GET reports a configurable
// (possibly already completed) state.
func executeFailServer(getStatus PaymentStatus) *httptest.Server {
	return httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		w.Header().Set("Content-Type", "application/json")
		switch {
		case r.Method == http.MethodPost && r.URL.Path == "/v1/payments":
			_ = json.NewEncoder(w).Encode(PaymentResponse{
				ID: "o_lost_001", AccessID: "a_lost_001", Status: StatusUnprocessed,
			})
		case r.Method == http.MethodGet:
			_ = json.NewEncoder(w).Encode(PaymentResponse{
				ID: "o_lost_001", AccessID: "a_lost_001", Status: getStatus,
				Amount: 1000, TotalAmount: 1000,
			})
		default: // PUT execute
			w.WriteHeader(http.StatusGatewayTimeout)
			_ = json.NewEncoder(w).Encode(ErrorResponse{
				Errors: []APIError{{ErrorCode: "E_TIMEOUT", ErrorMessage: "timeout"}},
			})
		}
	}))
}

// If the execute response was lost but fincode actually captured the payment,
// Charge must confirm the final state and return success — not a
// PartialAuthorizeError for a charge that went through.
func TestGateway_Charge_ExecuteFailure_AlreadyCaptured_ReplaysSuccess(t *testing.T) {
	srv := executeFailServer(StatusCaptured)
	defer srv.Close()
	gw := NewGateway(NewClient(Config{APIKey: "sk", BaseURL: srv.URL}),
		WithClock(shared.FixedClock{FixedTime: gwFixedTime}))

	resp, err := gw.Charge(context.Background(), &port.ChargeRequest{
		Amount: jpy(1000), Token: strPtr("tok"),
	})
	if err != nil {
		t.Fatalf("Charge: %v", err)
	}
	if resp.Status != port.TransactionStatusCaptured {
		t.Errorf("Status = %q, want captured", resp.Status)
	}
	if resp.TransactionID != "o_lost_001" {
		t.Errorf("TransactionID = %q, want o_lost_001", resp.TransactionID)
	}
}

func TestGateway_Authorize_ExecuteFailure_AlreadyAuthorized_ReplaysSuccess(t *testing.T) {
	srv := executeFailServer(StatusAuthorized)
	defer srv.Close()
	gw := NewGateway(NewClient(Config{APIKey: "sk", BaseURL: srv.URL}),
		WithClock(shared.FixedClock{FixedTime: gwFixedTime}))

	resp, err := gw.Authorize(context.Background(), &port.AuthorizeRequest{
		Amount: jpy(1000), Token: strPtr("tok"),
	})
	if err != nil {
		t.Fatalf("Authorize: %v", err)
	}
	if resp.Status != port.TransactionStatusAuthorized {
		t.Errorf("Status = %q, want authorized", resp.Status)
	}
}

// An execute failure whose retrieve still reports the order as UNPROCESSED
// keeps the PartialAuthorizeError contract (see also
// TestGateway_Charge_PartialFailure_ThenCompleteCharge, whose retrieve
// answers UNPROCESSED through the same path).
func TestGateway_Charge_ExecuteFailure_Unprocessed_IsPartialAuthorizeError(t *testing.T) {
	srv := executeFailServer(StatusUnprocessed)
	defer srv.Close()
	gw := NewGateway(NewClient(Config{APIKey: "sk", BaseURL: srv.URL}),
		WithClock(shared.FixedClock{FixedTime: gwFixedTime}))

	_, err := gw.Charge(context.Background(), &port.ChargeRequest{
		Amount: jpy(1000), Token: strPtr("tok"),
	})
	var pae *PartialAuthorizeError
	if !errors.As(err, &pae) {
		t.Fatalf("expected *PartialAuthorizeError, got %T: %v", err, err)
	}
	if pae.OrderID != "o_lost_001" || pae.AccessID != "a_lost_001" {
		t.Errorf("ids = %q/%q, want o_lost_001/a_lost_001", pae.OrderID, pae.AccessID)
	}
}

// --- Authorize ---

func TestGateway_Authorize_AuthOnly(t *testing.T) {
	gw, fake, cleanup := setupGateway(t)
	defer cleanup()

	resp, err := gw.Authorize(context.Background(), &port.AuthorizeRequest{
		Amount:     jpy(2000),
		CustomerID: "cust_001",
	})
	if err != nil {
		t.Fatalf("Authorize: %v", err)
	}
	if fake.lastCreateJobCode != JobCodeAuth {
		t.Errorf("job_code = %q, want AUTH", fake.lastCreateJobCode)
	}
	if resp.Status != port.TransactionStatusAuthorized {
		t.Errorf("Status = %q, want authorized", resp.Status)
	}
	if resp.AuthorizationID != "o_gw_order_001" || resp.TransactionID != "o_gw_order_001" {
		t.Errorf("ids = %q/%q, want fincode order id for both", resp.AuthorizationID, resp.TransactionID)
	}
	if resp.ExpiresAt == nil {
		t.Error("ExpiresAt should be parsed from auth_max_date")
	} else {
		// 2026/08/30 JST midnight → 2026/08/29 15:00 UTC.
		want := time.Date(2026, 8, 29, 15, 0, 0, 0, time.UTC)
		if !resp.ExpiresAt.Equal(want) {
			t.Errorf("ExpiresAt = %v, want %v", *resp.ExpiresAt, want)
		}
	}
}

// --- Capture ---

func TestGateway_Capture_FullAmount(t *testing.T) {
	gw, fake, cleanup := setupGateway(t)
	defer cleanup()
	fake.currentStatus = StatusAuthorized

	resp, err := gw.Capture(context.Background(), &port.CaptureRequest{
		AuthorizationID: "o_gw_order_001",
	})
	if err != nil {
		t.Fatalf("Capture: %v", err)
	}
	if !fake.retrieveCalled {
		t.Error("expected retrieve to fetch access_id")
	}
	if !fake.captureCalled {
		t.Error("expected capture call")
	}
	if fake.changeCalled {
		t.Error("full capture must not call /change")
	}
	if resp.Status != port.TransactionStatusCaptured {
		t.Errorf("Status = %q, want captured", resp.Status)
	}
	if resp.TransactionID != "o_gw_order_001" || resp.AuthorizationID != "o_gw_order_001" {
		t.Errorf("ids = %q/%q", resp.TransactionID, resp.AuthorizationID)
	}
}

func TestGateway_Capture_PartialAmount_ChangesFirst(t *testing.T) {
	gw, fake, cleanup := setupGateway(t)
	defer cleanup()
	fake.currentStatus = StatusAuthorized
	fake.currentTotal = 1000

	amount := jpy(600)
	_, err := gw.Capture(context.Background(), &port.CaptureRequest{
		AuthorizationID: "o_gw_order_001",
		Amount:          &amount,
	})
	if err != nil {
		t.Fatalf("Capture: %v", err)
	}
	if !fake.changeCalled {
		t.Error("expected /change before capture for partial amount")
	}
	if fake.lastChange.JobCode != JobCodeAuth {
		t.Errorf("change job_code = %q, want AUTH", fake.lastChange.JobCode)
	}
	if fake.lastChange.Amount != "600" {
		t.Errorf("change amount = %q, want 600", fake.lastChange.Amount)
	}
	if !fake.captureCalled {
		t.Error("expected capture call after change")
	}
}

// --- Void ---

func TestGateway_Void_Authorized(t *testing.T) {
	gw, fake, cleanup := setupGateway(t)
	defer cleanup()
	fake.currentStatus = StatusAuthorized

	resp, err := gw.Void(context.Background(), &port.VoidRequest{
		AuthorizationID: "o_gw_order_001",
	})
	if err != nil {
		t.Fatalf("Void: %v", err)
	}
	if !fake.cancelCalled {
		t.Error("expected cancel call")
	}
	if resp.Status != port.TransactionStatusCanceled {
		t.Errorf("Status = %q, want canceled", resp.Status)
	}
}

func TestGateway_Void_CapturedIsRejected(t *testing.T) {
	gw, fake, cleanup := setupGateway(t)
	defer cleanup()
	fake.currentStatus = StatusCaptured

	_, err := gw.Void(context.Background(), &port.VoidRequest{
		AuthorizationID: "o_gw_order_001",
	})
	var ge *port.GatewayError
	if !errors.As(err, &ge) || ge.Code != port.ErrorCodeProcessingError {
		t.Fatalf("expected processing_error GatewayError, got %v", err)
	}
	if fake.cancelCalled {
		t.Error("cancel must NOT be called when voiding a captured payment")
	}
}

// --- Cancel ---

func TestGateway_Cancel(t *testing.T) {
	gw, fake, cleanup := setupGateway(t)
	defer cleanup()

	resp, err := gw.Cancel(context.Background(), &port.CancelRequest{
		TransactionID: "o_gw_order_001",
	})
	if err != nil {
		t.Fatalf("Cancel: %v", err)
	}
	if !fake.cancelCalled {
		t.Error("expected cancel call")
	}
	if resp.Status != port.TransactionStatusCanceled {
		t.Errorf("Status = %q, want canceled", resp.Status)
	}
}

// --- Refund ---

func TestGateway_Refund_FullAmount_UsesCancel(t *testing.T) {
	gw, fake, cleanup := setupGateway(t)
	defer cleanup()
	fake.currentTotal = 1000

	amount := jpy(1000)
	resp, err := gw.Refund(context.Background(), &port.RefundRequest{
		TransactionID: "o_gw_order_001",
		Amount:        &amount,
		Reason:        port.RefundReasonRequestedByCustomer,
	})
	if err != nil {
		t.Fatalf("Refund (full): %v", err)
	}
	if !fake.retrieveCalled || !fake.cancelCalled {
		t.Error("expected retrieve + cancel for full refund")
	}
	if fake.changeCalled {
		t.Error("/change should NOT be called for full refund")
	}
	if resp.Status != port.RefundStatusSucceeded {
		t.Errorf("Status = %q, want succeeded", resp.Status)
	}
	if resp.Amount.Int64() != 1000 {
		t.Errorf("Amount = %d, want 1000", resp.Amount.Int64())
	}
	if resp.Reason != port.RefundReasonRequestedByCustomer {
		t.Errorf("Reason = %q, want requested_by_customer", resp.Reason)
	}
}

func TestGateway_Refund_NilAmount_MeansFullRefund(t *testing.T) {
	gw, fake, cleanup := setupGateway(t)
	defer cleanup()
	fake.currentTotal = 800

	resp, err := gw.Refund(context.Background(), &port.RefundRequest{
		TransactionID: "o_gw_order_001",
	})
	if err != nil {
		t.Fatalf("Refund (nil amount): %v", err)
	}
	if !fake.cancelCalled || fake.changeCalled {
		t.Error("nil amount must route to full refund (cancel)")
	}
	if resp.Amount.Int64() != 800 {
		t.Errorf("Amount = %d, want 800 (current total)", resp.Amount.Int64())
	}
}

func TestGateway_Refund_PartialAmount_UsesChange(t *testing.T) {
	gw, fake, cleanup := setupGateway(t)
	defer cleanup()
	fake.currentTotal = 1000

	amount := jpy(300)
	resp, err := gw.Refund(context.Background(), &port.RefundRequest{
		TransactionID: "o_gw_order_001",
		Amount:        &amount,
	})
	if err != nil {
		t.Fatalf("Refund (partial): %v", err)
	}
	if !fake.changeCalled {
		t.Error("expected /change for partial refund")
	}
	if fake.cancelCalled {
		t.Error("/cancel should NOT be called for partial refund")
	}
	if fake.lastChange.Amount != "700" {
		t.Errorf("new amount = %q, want 700 (1000 - 300)", fake.lastChange.Amount)
	}
	if fake.lastChange.JobCode != JobCodeCapture {
		t.Errorf("change job_code = %q, want CAPTURE", fake.lastChange.JobCode)
	}
	if resp.Amount.Int64() != 300 {
		t.Errorf("refund Amount = %d, want 300", resp.Amount.Int64())
	}
}

func TestGateway_Refund_AmountExceedsTotal(t *testing.T) {
	gw, fake, cleanup := setupGateway(t)
	defer cleanup()
	fake.currentTotal = 500

	amount := jpy(1000)
	_, err := gw.Refund(context.Background(), &port.RefundRequest{
		TransactionID: "o_gw_order_001",
		Amount:        &amount,
	})
	var ve *ValidationError
	if !errors.As(err, &ve) {
		t.Fatalf("expected *ValidationError, got %T: %v", err, err)
	}
	if fake.changeCalled || fake.cancelCalled {
		t.Error("no mutating call should have been made")
	}
}

func TestGateway_Refund_RequiresTransactionID(t *testing.T) {
	gw := unreachableGateway()
	_, err := gw.Refund(context.Background(), &port.RefundRequest{})
	var ve *ValidationError
	if !errors.As(err, &ve) {
		t.Fatalf("expected *ValidationError, got %T: %v", err, err)
	}
}

// --- GetTransaction ---

func TestGateway_GetTransaction(t *testing.T) {
	gw, fake, cleanup := setupGateway(t)
	defer cleanup()
	fake.currentStatus = StatusCaptured
	fake.currentTotal = 1000

	txn, err := gw.GetTransaction(context.Background(), "o_gw_order_001")
	if err != nil {
		t.Fatalf("GetTransaction: %v", err)
	}
	if txn.ID != "o_gw_order_001" {
		t.Errorf("ID = %q", txn.ID)
	}
	if txn.GatewayID != "fincode" {
		t.Errorf("GatewayID = %q, want fincode", txn.GatewayID)
	}
	if txn.Status != port.TransactionStatusCaptured {
		t.Errorf("Status = %q, want captured", txn.Status)
	}
	if txn.Amount.Int64() != 1000 || txn.Amount.Currency() != shared.CurrencyJPY {
		t.Errorf("Amount = %v %v", txn.Amount.Int64(), txn.Amount.Currency())
	}
	if txn.CustomerID != "cust_001" {
		t.Errorf("CustomerID = %q", txn.CustomerID)
	}
	if txn.PaymentMethodID != "cust_001/card_001" {
		t.Errorf("PaymentMethodID = %q, want composite", txn.PaymentMethodID)
	}
	if txn.Metadata["fincode_status"] != "CAPTURED" {
		t.Errorf("Metadata[fincode_status] = %q", txn.Metadata["fincode_status"])
	}
	if txn.AuthorizationID == nil || *txn.AuthorizationID != "o_gw_order_001" {
		t.Errorf("AuthorizationID = %v", txn.AuthorizationID)
	}
}

// --- Payment methods ---

func TestGateway_RegisterPaymentMethod(t *testing.T) {
	gw, fake, cleanup := setupGateway(t)
	defer cleanup()

	detail, err := gw.RegisterPaymentMethod(context.Background(), &port.RegisterPaymentMethodRequest{
		CustomerID:   "cust_001",
		Type:         port.PaymentMethodTypeCreditCard,
		Token:        "tok_card",
		SetAsDefault: true,
	})
	if err != nil {
		t.Fatalf("RegisterPaymentMethod: %v", err)
	}
	if !fake.cardCreateCalled {
		t.Error("expected card create call")
	}
	if fake.lastCardCreate.Token != "tok_card" || fake.lastCardCreate.DefaultFlag != "1" {
		t.Errorf("card create body = %+v", fake.lastCardCreate)
	}
	if detail.ID != "cust_001/card_001" {
		t.Errorf("ID = %q, want composite cust_001/card_001", detail.ID)
	}
	if detail.Type != port.PaymentMethodTypeCreditCard || !detail.IsDefault {
		t.Errorf("detail = %+v", detail)
	}
	if detail.Card == nil {
		t.Fatal("Card details missing")
	}
	if detail.Card.Brand != port.CardBrandVisa {
		t.Errorf("Brand = %q, want visa", detail.Card.Brand)
	}
	if detail.Card.Last4 != "1234" {
		t.Errorf("Last4 = %q, want 1234", detail.Card.Last4)
	}
	if detail.Card.ExpYear != 2029 || detail.Card.ExpMonth != 7 {
		t.Errorf("Exp = %d/%d, want 2029/7", detail.Card.ExpYear, detail.Card.ExpMonth)
	}
}

func TestGateway_RegisterPaymentMethod_RejectsNonCard(t *testing.T) {
	gw := unreachableGateway()
	_, err := gw.RegisterPaymentMethod(context.Background(), &port.RegisterPaymentMethodRequest{
		CustomerID: "cust_001",
		Type:       port.PaymentMethodTypeBankTransfer,
		Token:      "tok",
	})
	var ge *port.GatewayError
	if !errors.As(err, &ge) || ge.Code != port.ErrorCodeMethodNotSupported {
		t.Fatalf("expected method_not_supported, got %v", err)
	}
}

func TestGateway_GetPaymentMethod(t *testing.T) {
	gw, fake, cleanup := setupGateway(t)
	defer cleanup()

	detail, err := gw.GetPaymentMethod(context.Background(), "cust_001/card_001")
	if err != nil {
		t.Fatalf("GetPaymentMethod: %v", err)
	}
	if !fake.cardGetCalled {
		t.Error("expected card get call")
	}
	if detail.Card == nil || detail.Card.Brand != port.CardBrandMastercard {
		t.Errorf("detail = %+v", detail)
	}
}

func TestGateway_GetPaymentMethod_RejectsMalformedID(t *testing.T) {
	gw := unreachableGateway()
	for _, id := range []string{"", "no-slash", "/card", "cust/"} {
		if _, err := gw.GetPaymentMethod(context.Background(), id); err == nil {
			t.Errorf("expected error for malformed id %q", id)
		}
	}
}

func TestGateway_DeletePaymentMethod(t *testing.T) {
	gw, fake, cleanup := setupGateway(t)
	defer cleanup()

	if err := gw.DeletePaymentMethod(context.Background(), "cust_001/card_001"); err != nil {
		t.Fatalf("DeletePaymentMethod: %v", err)
	}
	if !fake.cardDeleteCalled {
		t.Error("expected card delete call")
	}
}

func TestGateway_ListPaymentMethods(t *testing.T) {
	gw, fake, cleanup := setupGateway(t)
	defer cleanup()

	list, err := gw.ListPaymentMethods(context.Background(), "cust_001")
	if err != nil {
		t.Fatalf("ListPaymentMethods: %v", err)
	}
	if !fake.cardListCalled {
		t.Error("expected card list call")
	}
	if len(list) != 2 {
		t.Fatalf("got %d methods, want 2", len(list))
	}
	if list[0].ID != "cust_001/card_001" || list[1].ID != "cust_001/card_002" {
		t.Errorf("ids = %q, %q", list[0].ID, list[1].ID)
	}
}

// --- Error mapping ---

func TestGateway_ErrorMapping_429IsRetryableRateLimit(t *testing.T) {
	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		w.Header().Set("Content-Type", "application/json")
		w.WriteHeader(http.StatusTooManyRequests)
		_ = json.NewEncoder(w).Encode(ErrorResponse{
			Errors: []APIError{{ErrorCode: "E9999", ErrorMessage: "rate limited"}},
		})
	}))
	defer srv.Close()

	gw := NewGateway(NewClient(Config{APIKey: "sk", BaseURL: srv.URL}))
	_, err := gw.GetTransaction(context.Background(), "o1")
	var ge *port.GatewayError
	if !errors.As(err, &ge) {
		t.Fatalf("expected *port.GatewayError, got %T: %v", err, err)
	}
	if ge.Code != port.ErrorCodeRateLimitExceeded || !ge.Retryable {
		t.Errorf("got %+v, want retryable rate_limit_exceeded", ge)
	}
	// The typed HTTP error must remain reachable through the chain.
	var he *HTTPError
	if !errors.As(err, &he) || he.StatusCode != 429 {
		t.Error("expected errors.As to reach *HTTPError with 429")
	}
}

func TestGateway_ErrorMapping_5xxIsRetryableUnavailable(t *testing.T) {
	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		w.WriteHeader(http.StatusBadGateway)
		_, _ = w.Write([]byte("<html>bad gateway</html>"))
	}))
	defer srv.Close()

	gw := NewGateway(NewClient(Config{APIKey: "sk", BaseURL: srv.URL}))
	_, err := gw.GetTransaction(context.Background(), "o1")
	var ge *port.GatewayError
	if !errors.As(err, &ge) {
		t.Fatalf("expected *port.GatewayError, got %T: %v", err, err)
	}
	if ge.Code != port.ErrorCodeGatewayUnavailable || !ge.Retryable {
		t.Errorf("got %+v, want retryable gateway_unavailable", ge)
	}
}

func TestGateway_ErrorMapping_NetworkErrorIsRetryable(t *testing.T) {
	gw := unreachableGateway()
	_, err := gw.GetTransaction(context.Background(), "o1")
	var ge *port.GatewayError
	if !errors.As(err, &ge) {
		t.Fatalf("expected *port.GatewayError, got %T: %v", err, err)
	}
	if ge.Code != port.ErrorCodeGatewayUnavailable || !ge.Retryable {
		t.Errorf("got %+v, want retryable gateway_unavailable", ge)
	}
}

// --- helpers ---

func TestSplitPaymentMethodID_SplitsAtLastSlash(t *testing.T) {
	customerID, cardID, err := splitPaymentMethodID("tenant/cust-42/card_abc")
	if err != nil {
		t.Fatal(err)
	}
	if customerID != "tenant/cust-42" || cardID != "card_abc" {
		t.Errorf("split = %q / %q", customerID, cardID)
	}
}

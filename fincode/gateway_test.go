package fincode

import (
	"context"
	"encoding/json"
	"errors"
	"math/big"
	"net/http"
	"net/http/httptest"
	"strconv"
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

	idempotencyHeader string
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
			_ = json.NewEncoder(w).Encode(f.paymentJSON(StatusCaptured, 1000))

		case r.Method == http.MethodPut && r.URL.Path == "/v1/payments/o_gw_order_001/cancel":
			f.cancelCalled = true
			_ = json.NewEncoder(w).Encode(f.paymentJSON(StatusCanceled, 0))

		case r.Method == http.MethodPut && r.URL.Path == "/v1/payments/o_gw_order_001/change":
			f.changeCalled = true
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

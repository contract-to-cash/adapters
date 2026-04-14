package fincode

import (
	"context"
	"encoding/json"
	"errors"
	"math/big"
	"net/http"
	"net/http/httptest"
	"testing"

	"github.com/contract-to-cash/core/domain/payment"
	"github.com/contract-to-cash/core/domain/shared"
)

func jpy(amount int64) shared.Money {
	return shared.NewMoney(new(big.Rat).SetInt64(amount), "JPY")
}

// fakeFincode is a test helper that implements a minimal fincode-like API server.
type fakeFincode struct {
	t             *testing.T
	createCalled  bool
	executeCalled bool
	captureCalled bool
	cancelCalled  bool
	changeCalled  bool
	retrieveCalled bool

	// lastJobCode records the JobCode sent in the last register call.
	lastCreateJobCode JobCode
	// lastExecute records the body of the last execute call.
	lastExecute ExecutePaymentRequest
	// lastChangeAmount records the last change request.
	lastChange ChangeAmountRequest
	// currentTotal is what GET /v1/payments/{id} will report.
	currentTotal int64
	// idempotencyHeader captured from the last register call.
	idempotencyHeader string
}

func (f *fakeFincode) handler() http.Handler {
	return http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		w.Header().Set("Content-Type", "application/json")

		switch {
		case r.Method == http.MethodPost && r.URL.Path == "/v1/payments":
			f.createCalled = true
			f.idempotencyHeader = r.Header.Get("idempotent_key")
			var req CreatePaymentRequest
			_ = json.NewDecoder(r.Body).Decode(&req)
			f.lastCreateJobCode = req.JobCode

			_ = json.NewEncoder(w).Encode(PaymentResponse{
				ID:       "o_gw_order_001",
				AccessID: "a_gw_access_001",
				Amount:   1000,
				Status:   StatusUnprocessed,
				PayType:  PayTypeCard,
				JobCode:  req.JobCode,
			})

		case r.Method == http.MethodPut && r.URL.Path == "/v1/payments/o_gw_order_001":
			f.executeCalled = true
			_ = json.NewDecoder(r.Body).Decode(&f.lastExecute)

			// Reflect the prior JobCode into the post-execute status so
			// that AUTH → AUTHORIZED and CAPTURE → CAPTURED.
			status := StatusCaptured
			if f.lastCreateJobCode == JobCodeAuth {
				status = StatusAuthorized
			}

			_ = json.NewEncoder(w).Encode(PaymentResponse{
				ID:            "o_gw_order_001",
				AccessID:      "a_gw_access_001",
				Amount:        1000,
				TotalAmount:   1000,
				Status:        status,
				TransactionID: "txn_gw_001",
				PayType:       PayTypeCard,
			})

		case r.Method == http.MethodGet && r.URL.Path == "/v1/payments/o_gw_order_001":
			f.retrieveCalled = true
			total := f.currentTotal
			if total == 0 {
				total = 1000
			}
			_ = json.NewEncoder(w).Encode(PaymentResponse{
				ID: "o_gw_order_001", AccessID: "a_gw_access_001",
				Amount: total, TotalAmount: total, Status: StatusCaptured,
				PayType: PayTypeCard,
			})

		case r.Method == http.MethodPut && r.URL.Path == "/v1/payments/o_gw_order_001/capture":
			f.captureCalled = true
			_ = json.NewEncoder(w).Encode(PaymentResponse{
				ID: "o_gw_order_001", AccessID: "a_gw_access_001",
				Amount: 1000, TotalAmount: 1000, Status: StatusCaptured,
				TransactionID: "txn_gw_cap_001", PayType: PayTypeCard,
			})

		case r.Method == http.MethodPut && r.URL.Path == "/v1/payments/o_gw_order_001/cancel":
			f.cancelCalled = true
			_ = json.NewEncoder(w).Encode(PaymentResponse{
				ID: "o_gw_order_001", AccessID: "a_gw_access_001",
				Amount: 0, TotalAmount: 0, Status: StatusCanceled,
				PayType: PayTypeCard,
			})

		case r.Method == http.MethodPut && r.URL.Path == "/v1/payments/o_gw_order_001/change":
			f.changeCalled = true
			_ = json.NewDecoder(r.Body).Decode(&f.lastChange)
			// Echo the new amount back as the current total.
			newTotal := int64(0)
			_, _ = fmtScanInt(f.lastChange.Amount, &newTotal)
			_ = json.NewEncoder(w).Encode(PaymentResponse{
				ID: "o_gw_order_001", AccessID: "a_gw_access_001",
				Amount: newTotal, TotalAmount: newTotal, Status: StatusCaptured,
				PayType: PayTypeCard,
			})

		default:
			w.WriteHeader(http.StatusNotFound)
			_ = json.NewEncoder(w).Encode(ErrorResponse{
				Errors: []APIError{{ErrorCode: "E9999", ErrorMessage: "not found: " + r.Method + " " + r.URL.Path}},
			})
		}
	})
}

// fmtScanInt is a tiny strconv shim that lets the fake handler echo numeric
// strings back without pulling strconv into the test namespace where the
// production code already uses it.
func fmtScanInt(s string, out *int64) (int, error) {
	var n int64
	var neg bool
	i := 0
	if len(s) > 0 && s[0] == '-' {
		neg = true
		i = 1
	}
	for ; i < len(s); i++ {
		c := s[i]
		if c < '0' || c > '9' {
			break
		}
		n = n*10 + int64(c-'0')
	}
	if neg {
		n = -n
	}
	*out = n
	return i, nil
}

func setupGateway(t *testing.T) (*Gateway, *fakeFincode, func()) {
	t.Helper()
	fake := &fakeFincode{t: t}
	srv := httptest.NewServer(fake.handler())
	client := NewClient(Config{APIKey: "sk_test_gw", BaseURL: srv.URL})
	gw := NewGateway(client)
	return gw, fake, srv.Close
}

// --- Authorize ---

func TestGateway_Authorize_ImmediateCapture(t *testing.T) {
	gw, fake, cleanup := setupGateway(t)
	defer cleanup()

	resp, err := gw.Authorize(context.Background(), payment.AuthorizeRequest{
		Amount:  jpy(1000),
		Token:   "tok_test_001",
		Capture: true,
	})
	if err != nil {
		t.Fatalf("Authorize: %v", err)
	}
	if !fake.createCalled || !fake.executeCalled {
		t.Error("expected both create and execute to be called")
	}
	if fake.lastCreateJobCode != JobCodeCapture {
		t.Errorf("job_code sent = %q, want CAPTURE", fake.lastCreateJobCode)
	}
	if resp.Status != payment.GatewayStatusCaptured {
		t.Errorf("Status = %q, want CAPTURED", resp.Status)
	}
	if resp.OrderID != "o_gw_order_001" {
		t.Errorf("OrderID = %q, want o_gw_order_001", resp.OrderID)
	}
	if resp.TransactionID != "txn_gw_001" {
		t.Errorf("TransactionID = %q, want txn_gw_001", resp.TransactionID)
	}
	if len(resp.RawResponse) == 0 {
		t.Error("RawResponse should contain the raw response body")
	}
}

// M6 fix: AuthOnly test actually asserts AUTHORIZED status, not the same
// CAPTURED as the immediate-capture test. Uses job_code=AUTH.
func TestGateway_Authorize_AuthOnly(t *testing.T) {
	gw, fake, cleanup := setupGateway(t)
	defer cleanup()

	resp, err := gw.Authorize(context.Background(), payment.AuthorizeRequest{
		Amount:     jpy(2000),
		CustomerID: "cust_001",
		CardID:     "card_001",
		Capture:    false, // AUTH
	})
	if err != nil {
		t.Fatalf("Authorize: %v", err)
	}
	if fake.lastCreateJobCode != JobCodeAuth {
		t.Errorf("job_code sent = %q, want AUTH", fake.lastCreateJobCode)
	}
	if resp.Status != payment.GatewayStatusAuthorized {
		t.Errorf("Status = %q, want AUTHORIZED (auth-only flow)", resp.Status)
	}
}

func TestGateway_Authorize_WithToken(t *testing.T) {
	gw, fake, cleanup := setupGateway(t)
	defer cleanup()

	_, err := gw.Authorize(context.Background(), payment.AuthorizeRequest{
		Amount:  jpy(500),
		Token:   "tok_from_fincodejs",
		Capture: true,
	})
	if err != nil {
		t.Fatalf("Authorize: %v", err)
	}
	if fake.lastExecute.Token != "tok_from_fincodejs" {
		t.Errorf("token = %q, want tok_from_fincodejs", fake.lastExecute.Token)
	}
}

// C3: IdempotencyKey is forwarded as the idempotent_key header.
func TestGateway_Authorize_IdempotencyKeyForwarded(t *testing.T) {
	gw, fake, cleanup := setupGateway(t)
	defer cleanup()

	_, err := gw.Authorize(context.Background(), payment.AuthorizeRequest{
		Amount:         jpy(100),
		Token:          "tok",
		Capture:        true,
		IdempotencyKey: "idem-uuid-abc",
	})
	if err != nil {
		t.Fatalf("Authorize: %v", err)
	}
	if fake.idempotencyHeader != "idem-uuid-abc" {
		t.Errorf("idempotent_key = %q, want idem-uuid-abc", fake.idempotencyHeader)
	}
}

// H4: Authorize requires Token or CustomerID.
func TestGateway_Authorize_RequiresTokenOrCustomerID(t *testing.T) {
	// No network server required — validation happens client-side.
	client := NewClient(Config{APIKey: "sk", BaseURL: "http://unreachable.invalid"})
	gw := NewGateway(client)

	_, err := gw.Authorize(context.Background(), payment.AuthorizeRequest{
		Amount:  jpy(100),
		Capture: true,
	})
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

// C2: partial failure (register succeeded, execute failed) returns
// *PartialAuthorizeError carrying OrderID and AccessID.
func TestGateway_Authorize_PartialFailure_ReturnsPartialError(t *testing.T) {
	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		w.Header().Set("Content-Type", "application/json")
		if r.Method == http.MethodPost && r.URL.Path == "/v1/payments" {
			_ = json.NewEncoder(w).Encode(PaymentResponse{
				ID: "o_partial_001", AccessID: "a_partial_001", Status: StatusUnprocessed,
			})
			return
		}
		// Execute always fails.
		w.WriteHeader(http.StatusBadRequest)
		_ = json.NewEncoder(w).Encode(ErrorResponse{
			Errors: []APIError{{ErrorCode: "E0200001", ErrorMessage: "bad token"}},
		})
	}))
	defer srv.Close()

	client := NewClient(Config{APIKey: "sk", BaseURL: srv.URL})
	gw := NewGateway(client)

	_, err := gw.Authorize(context.Background(), payment.AuthorizeRequest{
		Amount: jpy(1000), Token: "bad_token", Capture: true,
	})
	if err == nil {
		t.Fatal("expected error")
	}

	var pae *PartialAuthorizeError
	if !errors.As(err, &pae) {
		t.Fatalf("expected *PartialAuthorizeError, got %T: %v", err, err)
	}
	if pae.OrderID != "o_partial_001" {
		t.Errorf("OrderID = %q, want o_partial_001", pae.OrderID)
	}
	if pae.AccessID != "a_partial_001" {
		t.Errorf("AccessID = %q, want a_partial_001", pae.AccessID)
	}
	// The cause should unwrap to an HTTPError.
	var he *HTTPError
	if !errors.As(err, &he) {
		t.Error("cause should unwrap to *HTTPError")
	} else if he.StatusCode != 400 {
		t.Errorf("cause HTTPError.StatusCode = %d, want 400", he.StatusCode)
	}
}

// C2: ExecuteAuthorize completes a previously registered order.
func TestGateway_ExecuteAuthorize_CompletesFromPartial(t *testing.T) {
	var registerCallCount int
	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		w.Header().Set("Content-Type", "application/json")
		if r.Method == http.MethodPost && r.URL.Path == "/v1/payments" {
			registerCallCount++
			w.WriteHeader(http.StatusBadRequest)
			_ = json.NewEncoder(w).Encode(ErrorResponse{
				Errors: []APIError{{ErrorCode: "E", ErrorMessage: "should not be called"}},
			})
			return
		}
		// PUT /v1/payments/{id} succeeds on retry.
		_ = json.NewEncoder(w).Encode(PaymentResponse{
			ID: "o_partial_001", AccessID: "a_partial_001", Status: StatusCaptured,
			TransactionID: "txn_retry_001",
		})
	}))
	defer srv.Close()

	client := NewClient(Config{APIKey: "sk", BaseURL: srv.URL})
	gw := NewGateway(client)

	resp, err := gw.ExecuteAuthorize(context.Background(), "o_partial_001", "a_partial_001",
		payment.AuthorizeRequest{
			Token: "tok_retry", Capture: true,
		})
	if err != nil {
		t.Fatalf("ExecuteAuthorize: %v", err)
	}
	if registerCallCount != 0 {
		t.Error("ExecuteAuthorize must not call /v1/payments POST")
	}
	if resp.Status != payment.GatewayStatusCaptured {
		t.Errorf("Status = %q, want CAPTURED", resp.Status)
	}
	if resp.TransactionID != "txn_retry_001" {
		t.Errorf("TransactionID = %q, want txn_retry_001", resp.TransactionID)
	}
}

func TestGateway_ExecuteAuthorize_ValidatesIDs(t *testing.T) {
	client := NewClient(Config{APIKey: "sk", BaseURL: "http://unreachable.invalid"})
	gw := NewGateway(client)

	_, err := gw.ExecuteAuthorize(context.Background(), "", "a1",
		payment.AuthorizeRequest{Token: "t"})
	if err == nil {
		t.Error("expected error for empty orderID")
	}
	_, err = gw.ExecuteAuthorize(context.Background(), "o1", "",
		payment.AuthorizeRequest{Token: "t"})
	if err == nil {
		t.Error("expected error for empty accessID")
	}
}

// --- Capture / Cancel ---

func TestGateway_Capture(t *testing.T) {
	gw, fake, cleanup := setupGateway(t)
	defer cleanup()

	resp, err := gw.Capture(context.Background(), payment.CaptureRequest{
		OrderID:  "o_gw_order_001",
		AccessID: "a_gw_access_001",
		Amount:   jpy(1000),
	})
	if err != nil {
		t.Fatalf("Capture: %v", err)
	}
	if !fake.captureCalled {
		t.Error("expected CapturePayment to be called")
	}
	if resp.Status != payment.GatewayStatusCaptured {
		t.Errorf("Status = %q, want CAPTURED", resp.Status)
	}
	if resp.TransactionID != "txn_gw_cap_001" {
		t.Errorf("TransactionID = %q, want txn_gw_cap_001", resp.TransactionID)
	}
}

func TestGateway_Cancel(t *testing.T) {
	gw, fake, cleanup := setupGateway(t)
	defer cleanup()

	resp, err := gw.Cancel(context.Background(), payment.CancelRequest{
		OrderID:  "o_gw_order_001",
		AccessID: "a_gw_access_001",
	})
	if err != nil {
		t.Fatalf("Cancel: %v", err)
	}
	if !fake.cancelCalled {
		t.Error("expected CancelPayment to be called")
	}
	if resp.Status != payment.GatewayStatusCanceled {
		t.Errorf("Status = %q, want CANCELED", resp.Status)
	}
}

// --- Refund (C1 fix) ---

// C1: full refund routes to /cancel.
func TestGateway_Refund_FullAmount_UsesCancel(t *testing.T) {
	gw, fake, cleanup := setupGateway(t)
	defer cleanup()
	fake.currentTotal = 1000

	resp, err := gw.Refund(context.Background(), payment.RefundRequest{
		OrderID:  "o_gw_order_001",
		AccessID: "a_gw_access_001",
		Amount:   jpy(1000),
	})
	if err != nil {
		t.Fatalf("Refund (full): %v", err)
	}
	if !fake.retrieveCalled {
		t.Error("expected retrieve to be called to determine current total")
	}
	if !fake.cancelCalled {
		t.Error("expected /cancel to be called for full refund")
	}
	if fake.changeCalled {
		t.Error("/change should NOT be called for full refund")
	}
	if resp.Status != payment.GatewayStatusCanceled {
		t.Errorf("Status = %q, want CANCELED", resp.Status)
	}
}

// C1: partial refund routes to /change with new_amount = total - refund.
func TestGateway_Refund_PartialAmount_UsesChange(t *testing.T) {
	gw, fake, cleanup := setupGateway(t)
	defer cleanup()
	fake.currentTotal = 1000

	resp, err := gw.Refund(context.Background(), payment.RefundRequest{
		OrderID:  "o_gw_order_001",
		AccessID: "a_gw_access_001",
		Amount:   jpy(300), // refund 300 out of 1000
	})
	if err != nil {
		t.Fatalf("Refund (partial): %v", err)
	}
	if !fake.retrieveCalled {
		t.Error("expected retrieve to be called to determine current total")
	}
	if !fake.changeCalled {
		t.Error("expected /change to be called for partial refund")
	}
	if fake.cancelCalled {
		t.Error("/cancel should NOT be called for partial refund")
	}
	if fake.lastChange.Amount != "700" {
		t.Errorf("new amount = %q, want 700 (1000 - 300)", fake.lastChange.Amount)
	}
	if fake.lastChange.JobCode != JobCodeCapture {
		t.Errorf("job_code = %q, want CAPTURE", fake.lastChange.JobCode)
	}
	if resp.Status != payment.GatewayStatusCaptured {
		// After /change, the payment remains CAPTURED with a lower amount.
		t.Errorf("Status = %q, want CAPTURED", resp.Status)
	}
}

// C1: refund amount > current total is a validation error.
func TestGateway_Refund_AmountExceedsTotal(t *testing.T) {
	gw, fake, cleanup := setupGateway(t)
	defer cleanup()
	fake.currentTotal = 500

	_, err := gw.Refund(context.Background(), payment.RefundRequest{
		OrderID:  "o_gw_order_001",
		AccessID: "a_gw_access_001",
		Amount:   jpy(1000),
	})
	if err == nil {
		t.Fatal("expected validation error")
	}
	var ve *ValidationError
	if !errors.As(err, &ve) {
		t.Fatalf("expected *ValidationError, got %T: %v", err, err)
	}
	if fake.changeCalled || fake.cancelCalled {
		t.Error("no mutating call should have been made")
	}
}

func TestGateway_Refund_NegativeOrZeroAmount(t *testing.T) {
	client := NewClient(Config{APIKey: "sk", BaseURL: "http://unreachable.invalid"})
	gw := NewGateway(client)

	_, err := gw.Refund(context.Background(), payment.RefundRequest{
		OrderID: "o1", AccessID: "a1", Amount: jpy(0),
	})
	if err == nil {
		t.Error("expected validation error for zero amount")
	}
	var ve *ValidationError
	if !errors.As(err, &ve) {
		t.Errorf("expected *ValidationError, got %T", err)
	}
}

func TestGateway_Refund_RequiresIDs(t *testing.T) {
	client := NewClient(Config{APIKey: "sk", BaseURL: "http://unreachable.invalid"})
	gw := NewGateway(client)

	_, err := gw.Refund(context.Background(), payment.RefundRequest{
		OrderID: "", AccessID: "a1", Amount: jpy(100),
	})
	if err == nil {
		t.Error("expected validation error for empty OrderID")
	}
}

// --- Error handling ---

func TestGateway_Authorize_APIErrorOnRegister(t *testing.T) {
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

	_, err := gw.Authorize(context.Background(), payment.AuthorizeRequest{
		Amount: jpy(0), Token: "tok", Capture: true,
	})
	if err == nil {
		t.Fatal("expected error")
	}
	// Register failure is NOT a partial — no order was created.
	var pae *PartialAuthorizeError
	if errors.As(err, &pae) {
		t.Error("register failure should not produce PartialAuthorizeError")
	}
	// But it should still be a typed HTTPError.
	var he *HTTPError
	if !errors.As(err, &he) {
		t.Errorf("expected *HTTPError, got %T", err)
	}
}

func TestGateway_Capture_APIError(t *testing.T) {
	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		w.Header().Set("Content-Type", "application/json")
		w.WriteHeader(http.StatusBadRequest)
		_ = json.NewEncoder(w).Encode(ErrorResponse{
			Errors: []APIError{{ErrorCode: "E0200001", ErrorMessage: "invalid access_id"}},
		})
	}))
	defer srv.Close()

	client := NewClient(Config{APIKey: "sk", BaseURL: srv.URL})
	gw := NewGateway(client)

	_, err := gw.Capture(context.Background(), payment.CaptureRequest{
		OrderID: "o_invalid", AccessID: "a_invalid",
	})
	var he *HTTPError
	if !errors.As(err, &he) {
		t.Fatalf("expected *HTTPError, got %T: %v", err, err)
	}
}

func TestGateway_Cancel_APIError(t *testing.T) {
	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		w.Header().Set("Content-Type", "application/json")
		w.WriteHeader(http.StatusBadRequest)
		_ = json.NewEncoder(w).Encode(ErrorResponse{
			Errors: []APIError{{ErrorCode: "E0300001", ErrorMessage: "already canceled"}},
		})
	}))
	defer srv.Close()

	client := NewClient(Config{APIKey: "sk", BaseURL: srv.URL})
	gw := NewGateway(client)

	_, err := gw.Cancel(context.Background(), payment.CancelRequest{
		OrderID: "o1", AccessID: "a1",
	})
	var he *HTTPError
	if !errors.As(err, &he) {
		t.Fatalf("expected *HTTPError, got %T", err)
	}
}

// --- RawResponse preserves server bytes ---

func TestGateway_RawResponse_PreservesServerJSON(t *testing.T) {
	const specificValue = "a_server_value_abc"
	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		w.Header().Set("Content-Type", "application/json")
		if r.Method == http.MethodPost {
			_, _ = w.Write([]byte(`{"id":"o1","access_id":"` + specificValue + `","status":"UNPROCESSED"}`))
			return
		}
		_, _ = w.Write([]byte(`{"id":"o1","access_id":"` + specificValue + `","status":"CAPTURED","transaction_id":"txn1","amount":100,"total_amount":100}`))
	}))
	defer srv.Close()

	client := NewClient(Config{APIKey: "sk", BaseURL: srv.URL})
	gw := NewGateway(client)

	resp, err := gw.Authorize(context.Background(), payment.AuthorizeRequest{
		Amount: jpy(100), Token: "t", Capture: true,
	})
	if err != nil {
		t.Fatalf("Authorize: %v", err)
	}

	// RawResponse should be the exact bytes from the execute response.
	var raw map[string]any
	if err := json.Unmarshal(resp.RawResponse, &raw); err != nil {
		t.Fatalf("unmarshal RawResponse: %v", err)
	}
	if raw["access_id"] != specificValue {
		t.Errorf("access_id in RawResponse = %v, want %s", raw["access_id"], specificValue)
	}
	if raw["transaction_id"] != "txn1" {
		t.Errorf("transaction_id in RawResponse = %v, want txn1", raw["transaction_id"])
	}
}

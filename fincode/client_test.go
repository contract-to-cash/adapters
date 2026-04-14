package fincode

import (
	"context"
	"encoding/json"
	"errors"
	"net/http"
	"net/http/httptest"
	"testing"
	"time"
)

func TestClient_CreatePayment(t *testing.T) {
	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		if r.Method != http.MethodPost {
			t.Errorf("method = %s, want POST", r.Method)
		}
		if r.URL.Path != "/v1/payments" {
			t.Errorf("path = %s, want /v1/payments", r.URL.Path)
		}
		if got := r.Header.Get("Authorization"); got != "Bearer sk_test_123" {
			t.Errorf("Authorization = %q, want Bearer sk_test_123", got)
		}
		if ct := r.Header.Get("Content-Type"); ct != "application/json;charset=UTF-8" {
			t.Errorf("Content-Type = %q, want application/json;charset=UTF-8", ct)
		}

		var req CreatePaymentRequest
		if err := json.NewDecoder(r.Body).Decode(&req); err != nil {
			t.Fatalf("decode request: %v", err)
		}
		if req.PayType != PayTypeCard {
			t.Errorf("pay_type = %q, want Card", req.PayType)
		}
		if req.JobCode != JobCodeCapture {
			t.Errorf("job_code = %q, want CAPTURE", req.JobCode)
		}
		if req.Amount != "1000" {
			t.Errorf("amount = %q, want 1000", req.Amount)
		}

		w.Header().Set("Content-Type", "application/json")
		_ = json.NewEncoder(w).Encode(PaymentResponse{
			ID:       "o_test_order_001",
			AccessID: "a_test_access_001",
			Amount:   1000,
			Status:   StatusUnprocessed,
			PayType:  PayTypeCard,
			JobCode:  JobCodeCapture,
		})
	}))
	defer srv.Close()

	client := NewClient(Config{APIKey: "sk_test_123", BaseURL: srv.URL})
	resp, err := client.CreatePayment(context.Background(), &CreatePaymentRequest{
		PayType: PayTypeCard,
		JobCode: JobCodeCapture,
		Amount:  "1000",
	}, "")
	if err != nil {
		t.Fatalf("CreatePayment: %v", err)
	}
	if resp.ID != "o_test_order_001" {
		t.Errorf("ID = %q, want o_test_order_001", resp.ID)
	}
	if resp.AccessID != "a_test_access_001" {
		t.Errorf("AccessID = %q, want a_test_access_001", resp.AccessID)
	}
}

// C3: idempotent_key header must be forwarded when provided.
func TestClient_CreatePayment_ForwardsIdempotencyKey(t *testing.T) {
	var got string
	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		got = r.Header.Get("idempotent_key")
		w.Header().Set("Content-Type", "application/json")
		_ = json.NewEncoder(w).Encode(PaymentResponse{ID: "o1", AccessID: "a1"})
	}))
	defer srv.Close()

	client := NewClient(Config{APIKey: "sk", BaseURL: srv.URL})
	_, err := client.CreatePayment(context.Background(), &CreatePaymentRequest{
		PayType: PayTypeCard, JobCode: JobCodeCapture, Amount: "100",
	}, "uuid-v4-abc-123")
	if err != nil {
		t.Fatalf("CreatePayment: %v", err)
	}
	if got != "uuid-v4-abc-123" {
		t.Errorf("idempotent_key header = %q, want uuid-v4-abc-123", got)
	}
}

func TestClient_CreatePayment_NoIdempotencyKeyWhenEmpty(t *testing.T) {
	var got string
	var hasHeader bool
	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		got = r.Header.Get("idempotent_key")
		_, hasHeader = r.Header["Idempotent_key"]
		w.Header().Set("Content-Type", "application/json")
		_ = json.NewEncoder(w).Encode(PaymentResponse{ID: "o1"})
	}))
	defer srv.Close()

	client := NewClient(Config{APIKey: "sk", BaseURL: srv.URL})
	_, _ = client.CreatePayment(context.Background(), &CreatePaymentRequest{
		PayType: PayTypeCard, JobCode: JobCodeCapture, Amount: "100",
	}, "")
	if got != "" || hasHeader {
		t.Errorf("idempotent_key should not be set when empty; got=%q hasHeader=%v", got, hasHeader)
	}
}

func TestClient_ExecutePayment(t *testing.T) {
	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		if r.Method != http.MethodPut {
			t.Errorf("method = %s, want PUT", r.Method)
		}
		if r.URL.Path != "/v1/payments/o_test_001" {
			t.Errorf("path = %s, want /v1/payments/o_test_001", r.URL.Path)
		}

		var req ExecutePaymentRequest
		_ = json.NewDecoder(r.Body).Decode(&req)
		if req.AccessID != "a_test_001" {
			t.Errorf("access_id = %q, want a_test_001", req.AccessID)
		}

		w.Header().Set("Content-Type", "application/json")
		_ = json.NewEncoder(w).Encode(PaymentResponse{
			ID:            "o_test_001",
			AccessID:      "a_test_001",
			Amount:        1000,
			Status:        StatusCaptured,
			TransactionID: "txn_001",
		})
	}))
	defer srv.Close()

	client := NewClient(Config{APIKey: "sk_test_123", BaseURL: srv.URL})
	resp, err := client.ExecutePayment(context.Background(), "o_test_001", &ExecutePaymentRequest{
		PayType:    PayTypeCard,
		AccessID:   "a_test_001",
		CustomerID: "cust_001",
	})
	if err != nil {
		t.Fatalf("ExecutePayment: %v", err)
	}
	if resp.Status != StatusCaptured {
		t.Errorf("Status = %q, want CAPTURED", resp.Status)
	}
}

func TestClient_CapturePayment(t *testing.T) {
	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		if r.URL.Path != "/v1/payments/o_test_001/capture" {
			t.Errorf("path = %s, want /v1/payments/o_test_001/capture", r.URL.Path)
		}
		w.Header().Set("Content-Type", "application/json")
		_ = json.NewEncoder(w).Encode(PaymentResponse{
			ID: "o_test_001", AccessID: "a_test_001", Status: StatusCaptured,
			TransactionID: "txn_cap_001",
		})
	}))
	defer srv.Close()

	client := NewClient(Config{APIKey: "sk", BaseURL: srv.URL})
	resp, err := client.CapturePayment(context.Background(), "o_test_001", &CapturePaymentRequest{
		PayType: PayTypeCard, AccessID: "a_test_001",
	})
	if err != nil {
		t.Fatalf("CapturePayment: %v", err)
	}
	if resp.Status != StatusCaptured {
		t.Errorf("Status = %q, want CAPTURED", resp.Status)
	}
}

func TestClient_CancelPayment(t *testing.T) {
	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		if r.URL.Path != "/v1/payments/o_test_001/cancel" {
			t.Errorf("path = %s, want /v1/payments/o_test_001/cancel", r.URL.Path)
		}
		w.Header().Set("Content-Type", "application/json")
		_ = json.NewEncoder(w).Encode(PaymentResponse{
			ID: "o_test_001", AccessID: "a_test_001", Status: StatusCanceled,
		})
	}))
	defer srv.Close()

	client := NewClient(Config{APIKey: "sk", BaseURL: srv.URL})
	resp, err := client.CancelPayment(context.Background(), "o_test_001", &CancelPaymentRequest{
		PayType: PayTypeCard, AccessID: "a_test_001",
	})
	if err != nil {
		t.Fatalf("CancelPayment: %v", err)
	}
	if resp.Status != StatusCanceled {
		t.Errorf("Status = %q, want CANCELED", resp.Status)
	}
}

// C1: /change endpoint must be reachable.
func TestClient_ChangeAmount(t *testing.T) {
	var gotReq ChangeAmountRequest
	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		if r.Method != http.MethodPut {
			t.Errorf("method = %s, want PUT", r.Method)
		}
		if r.URL.Path != "/v1/payments/o_test_001/change" {
			t.Errorf("path = %s, want /v1/payments/o_test_001/change", r.URL.Path)
		}
		_ = json.NewDecoder(r.Body).Decode(&gotReq)
		w.Header().Set("Content-Type", "application/json")
		_ = json.NewEncoder(w).Encode(PaymentResponse{
			ID: "o_test_001", AccessID: "a_test_001",
			Amount: 700, TotalAmount: 700, Status: StatusCaptured,
		})
	}))
	defer srv.Close()

	client := NewClient(Config{APIKey: "sk", BaseURL: srv.URL})
	resp, err := client.ChangeAmount(context.Background(), "o_test_001", &ChangeAmountRequest{
		PayType: PayTypeCard, AccessID: "a_test_001",
		JobCode: JobCodeCapture, Amount: "700",
	})
	if err != nil {
		t.Fatalf("ChangeAmount: %v", err)
	}
	if resp.TotalAmount != 700 {
		t.Errorf("TotalAmount = %d, want 700", resp.TotalAmount)
	}
	if gotReq.JobCode != JobCodeCapture {
		t.Errorf("job_code = %q, want CAPTURE", gotReq.JobCode)
	}
	if gotReq.Amount != "700" {
		t.Errorf("amount = %q, want 700", gotReq.Amount)
	}
}

func TestClient_RetrievePayment(t *testing.T) {
	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		if r.Method != http.MethodGet {
			t.Errorf("method = %s, want GET", r.Method)
		}
		if got := r.URL.Query().Get("pay_type"); got != "Card" {
			t.Errorf("pay_type = %q, want Card", got)
		}
		w.Header().Set("Content-Type", "application/json")
		_ = json.NewEncoder(w).Encode(PaymentResponse{
			ID: "o_test_001", Amount: 1000, TotalAmount: 1000, Status: StatusCaptured,
		})
	}))
	defer srv.Close()

	client := NewClient(Config{APIKey: "sk", BaseURL: srv.URL})
	resp, err := client.RetrievePayment(context.Background(), "o_test_001", PayTypeCard)
	if err != nil {
		t.Fatalf("RetrievePayment: %v", err)
	}
	if resp.TotalAmount != 1000 {
		t.Errorf("TotalAmount = %d, want 1000", resp.TotalAmount)
	}
}

// H1: errors from API are returned as *HTTPError with status and APIError.
func TestClient_HTTPError_4xx_JSONBody(t *testing.T) {
	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		w.Header().Set("Content-Type", "application/json")
		w.WriteHeader(http.StatusBadRequest)
		_ = json.NewEncoder(w).Encode(ErrorResponse{
			Errors: []APIError{{ErrorCode: "E0100001", ErrorMessage: "Invalid request"}},
		})
	}))
	defer srv.Close()

	client := NewClient(Config{APIKey: "sk", BaseURL: srv.URL})
	_, err := client.CreatePayment(context.Background(), &CreatePaymentRequest{
		PayType: PayTypeCard, JobCode: JobCodeCapture, Amount: "1000",
	}, "")
	if err == nil {
		t.Fatal("expected error")
	}

	var he *HTTPError
	if !errors.As(err, &he) {
		t.Fatalf("expected *HTTPError, got %T: %v", err, err)
	}
	if he.StatusCode != 400 {
		t.Errorf("StatusCode = %d, want 400", he.StatusCode)
	}
	if he.APIError == nil || len(he.APIError.Errors) == 0 {
		t.Fatalf("APIError not populated: %+v", he)
	}
	if he.APIError.Errors[0].ErrorCode != "E0100001" {
		t.Errorf("ErrorCode = %q, want E0100001", he.APIError.Errors[0].ErrorCode)
	}

	var erResp *ErrorResponse
	if !errors.As(err, &erResp) {
		t.Error("expected errors.As to unwrap *ErrorResponse")
	}
}

// H1: 429 preserved so callers can drive retries.
func TestClient_HTTPError_Preserves429(t *testing.T) {
	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		w.Header().Set("Content-Type", "application/json")
		w.WriteHeader(http.StatusTooManyRequests)
		_ = json.NewEncoder(w).Encode(ErrorResponse{
			Errors: []APIError{{ErrorCode: "E9999", ErrorMessage: "rate limited"}},
		})
	}))
	defer srv.Close()

	client := NewClient(Config{APIKey: "sk", BaseURL: srv.URL})
	_, err := client.CancelPayment(context.Background(), "o1", &CancelPaymentRequest{
		PayType: PayTypeCard, AccessID: "a1",
	})
	var he *HTTPError
	if !errors.As(err, &he) {
		t.Fatalf("expected *HTTPError, got %T", err)
	}
	if he.StatusCode != 429 {
		t.Errorf("StatusCode = %d, want 429", he.StatusCode)
	}
}

// H1: non-JSON error body (e.g., 502 HTML page) still returns HTTPError.
func TestClient_HTTPError_NonJSONBody(t *testing.T) {
	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		w.Header().Set("Content-Type", "text/html")
		w.WriteHeader(http.StatusBadGateway)
		_, _ = w.Write([]byte("<html>gateway error</html>"))
	}))
	defer srv.Close()

	client := NewClient(Config{APIKey: "sk", BaseURL: srv.URL})
	_, err := client.CancelPayment(context.Background(), "o1", &CancelPaymentRequest{
		PayType: PayTypeCard, AccessID: "a1",
	})
	var he *HTTPError
	if !errors.As(err, &he) {
		t.Fatalf("expected *HTTPError, got %T: %v", err, err)
	}
	if he.StatusCode != 502 {
		t.Errorf("StatusCode = %d, want 502", he.StatusCode)
	}
	if he.APIError != nil {
		t.Errorf("APIError should be nil for non-JSON body, got %+v", he.APIError)
	}
	if len(he.Body) == 0 {
		t.Error("Body should be preserved for debugging")
	}
}

// H1: all errors are surfaced in ErrorResponse.Error(), not just the first.
func TestErrorResponse_MultipleErrors(t *testing.T) {
	er := &ErrorResponse{
		Errors: []APIError{
			{ErrorCode: "E001", ErrorMessage: "first"},
			{ErrorCode: "E002", ErrorMessage: "second"},
		},
	}
	msg := er.Error()
	if !contains(msg, "E001") || !contains(msg, "E002") {
		t.Errorf("Error() = %q, expected both E001 and E002", msg)
	}
}

func contains(s, sub string) bool {
	for i := 0; i+len(sub) <= len(s); i++ {
		if s[i:i+len(sub)] == sub {
			return true
		}
	}
	return false
}

func TestClient_DefaultSandboxURL(t *testing.T) {
	client := NewClient(Config{APIKey: "sk_test"})
	if client.baseURL != SandboxBaseURL {
		t.Errorf("baseURL = %q, want %q", client.baseURL, SandboxBaseURL)
	}
}

// M1: default http.Client has a timeout set.
func TestClient_DefaultTimeout(t *testing.T) {
	client := NewClient(Config{APIKey: "sk"})
	if client.httpClient == nil {
		t.Fatal("httpClient must be non-nil")
	}
	if client.httpClient.Timeout != DefaultTimeout {
		t.Errorf("Timeout = %v, want %v", client.httpClient.Timeout, DefaultTimeout)
	}
}

func TestClient_CustomHTTPClient(t *testing.T) {
	custom := &http.Client{Timeout: 5 * time.Second}
	client := NewClient(Config{APIKey: "sk"}, WithHTTPClient(custom))
	if client.httpClient != custom {
		t.Error("expected custom http client")
	}
	if client.httpClient.Timeout != 5*time.Second {
		t.Errorf("Timeout = %v, want 5s", client.httpClient.Timeout)
	}
}

// M8: path segments are escaped.
func TestClient_PathEscape(t *testing.T) {
	var gotPath string
	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		gotPath = r.URL.Path
		w.Header().Set("Content-Type", "application/json")
		_ = json.NewEncoder(w).Encode(PaymentResponse{ID: "x"})
	}))
	defer srv.Close()

	client := NewClient(Config{APIKey: "sk", BaseURL: srv.URL})
	// orderID with a slash — must be escaped, not path-routed.
	_, _ = client.CapturePayment(context.Background(), "a/b", &CapturePaymentRequest{
		PayType: PayTypeCard, AccessID: "a1",
	})
	// r.URL.Path is already unescaped by net/http on the server side, but
	// the path must still contain the raw order-id segment including the
	// slash — i.e., the server sees /v1/payments/a/b/capture. That's
	// actually what we want to avoid: on the client, the slash must be
	// encoded as %2F so the path routes as one segment. On the server
	// side after unescape we get the original string back in a single
	// segment only if r.URL.RawPath is inspected; otherwise stdlib
	// collapses %2F back to /. We test the outgoing URL via a custom
	// transport instead.
	_ = gotPath

	// Use a RoundTripper to capture the raw URL before it's sent.
	var captured string
	rt := roundTripFunc(func(r *http.Request) (*http.Response, error) {
		captured = r.URL.EscapedPath()
		return &http.Response{
			StatusCode: 200,
			Body:       http.NoBody,
			Header:     http.Header{"Content-Type": []string{"application/json"}},
		}, nil
	})
	c2 := NewClient(Config{APIKey: "sk", BaseURL: "http://example.test"},
		WithHTTPClient(&http.Client{Transport: rt}))
	_, _ = c2.CapturePayment(context.Background(), "a/b", &CapturePaymentRequest{
		PayType: PayTypeCard, AccessID: "a",
	})
	if captured != "/v1/payments/a%2Fb/capture" {
		t.Errorf("escaped path = %q, want /v1/payments/a%%2Fb/capture", captured)
	}
}

type roundTripFunc func(*http.Request) (*http.Response, error)

func (f roundTripFunc) RoundTrip(r *http.Request) (*http.Response, error) {
	return f(r)
}

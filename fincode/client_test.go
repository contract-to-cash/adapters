package fincode

import (
	"context"
	"encoding/json"
	"net/http"
	"net/http/httptest"
	"testing"
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
		json.NewEncoder(w).Encode(PaymentResponse{
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
	})
	if err != nil {
		t.Fatalf("CreatePayment: %v", err)
	}
	if resp.ID != "o_test_order_001" {
		t.Errorf("ID = %q, want o_test_order_001", resp.ID)
	}
	if resp.AccessID != "a_test_access_001" {
		t.Errorf("AccessID = %q, want a_test_access_001", resp.AccessID)
	}
	if resp.Status != StatusUnprocessed {
		t.Errorf("Status = %q, want UNPROCESSED", resp.Status)
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
		json.NewDecoder(r.Body).Decode(&req)
		if req.AccessID != "a_test_001" {
			t.Errorf("access_id = %q, want a_test_001", req.AccessID)
		}
		if req.CustomerID != "cust_001" {
			t.Errorf("customer_id = %q, want cust_001", req.CustomerID)
		}

		w.Header().Set("Content-Type", "application/json")
		json.NewEncoder(w).Encode(PaymentResponse{
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
		CardID:     "card_001",
		Method:     "1",
	})
	if err != nil {
		t.Fatalf("ExecutePayment: %v", err)
	}
	if resp.Status != StatusCaptured {
		t.Errorf("Status = %q, want CAPTURED", resp.Status)
	}
	if resp.TransactionID != "txn_001" {
		t.Errorf("TransactionID = %q, want txn_001", resp.TransactionID)
	}
}

func TestClient_CapturePayment(t *testing.T) {
	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		if r.Method != http.MethodPut {
			t.Errorf("method = %s, want PUT", r.Method)
		}
		if r.URL.Path != "/v1/payments/o_test_001/capture" {
			t.Errorf("path = %s, want /v1/payments/o_test_001/capture", r.URL.Path)
		}

		w.Header().Set("Content-Type", "application/json")
		json.NewEncoder(w).Encode(PaymentResponse{
			ID:            "o_test_001",
			AccessID:      "a_test_001",
			Status:        StatusCaptured,
			TransactionID: "txn_cap_001",
		})
	}))
	defer srv.Close()

	client := NewClient(Config{APIKey: "sk_test_123", BaseURL: srv.URL})
	resp, err := client.CapturePayment(context.Background(), "o_test_001", &CapturePaymentRequest{
		PayType:  PayTypeCard,
		AccessID: "a_test_001",
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
		if r.Method != http.MethodPut {
			t.Errorf("method = %s, want PUT", r.Method)
		}
		if r.URL.Path != "/v1/payments/o_test_001/cancel" {
			t.Errorf("path = %s, want /v1/payments/o_test_001/cancel", r.URL.Path)
		}

		w.Header().Set("Content-Type", "application/json")
		json.NewEncoder(w).Encode(PaymentResponse{
			ID:       "o_test_001",
			AccessID: "a_test_001",
			Status:   StatusCanceled,
		})
	}))
	defer srv.Close()

	client := NewClient(Config{APIKey: "sk_test_123", BaseURL: srv.URL})
	resp, err := client.CancelPayment(context.Background(), "o_test_001", &CancelPaymentRequest{
		PayType:  PayTypeCard,
		AccessID: "a_test_001",
	})
	if err != nil {
		t.Fatalf("CancelPayment: %v", err)
	}
	if resp.Status != StatusCanceled {
		t.Errorf("Status = %q, want CANCELED", resp.Status)
	}
}

func TestClient_RetrievePayment(t *testing.T) {
	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		if r.Method != http.MethodGet {
			t.Errorf("method = %s, want GET", r.Method)
		}
		if r.URL.Path != "/v1/payments/o_test_001" {
			t.Errorf("path = %s, want /v1/payments/o_test_001", r.URL.Path)
		}
		if got := r.URL.Query().Get("pay_type"); got != "Card" {
			t.Errorf("pay_type = %q, want Card", got)
		}

		w.Header().Set("Content-Type", "application/json")
		json.NewEncoder(w).Encode(PaymentResponse{
			ID:       "o_test_001",
			AccessID: "a_test_001",
			Amount:   1000,
			Status:   StatusCaptured,
			PayType:  PayTypeCard,
		})
	}))
	defer srv.Close()

	client := NewClient(Config{APIKey: "sk_test_123", BaseURL: srv.URL})
	resp, err := client.RetrievePayment(context.Background(), "o_test_001", PayTypeCard)
	if err != nil {
		t.Fatalf("RetrievePayment: %v", err)
	}
	if resp.Amount != 1000 {
		t.Errorf("Amount = %d, want 1000", resp.Amount)
	}
}

func TestClient_ErrorResponse(t *testing.T) {
	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		w.WriteHeader(http.StatusBadRequest)
		w.Header().Set("Content-Type", "application/json")
		json.NewEncoder(w).Encode(ErrorResponse{
			Errors: []APIError{
				{ErrorCode: "E0100001", ErrorMessage: "Invalid request"},
			},
		})
	}))
	defer srv.Close()

	client := NewClient(Config{APIKey: "sk_test_123", BaseURL: srv.URL})
	_, err := client.CreatePayment(context.Background(), &CreatePaymentRequest{
		PayType: PayTypeCard,
		JobCode: JobCodeCapture,
		Amount:  "1000",
	})
	if err == nil {
		t.Fatal("expected error")
	}

	errResp, ok := err.(*ErrorResponse)
	if !ok {
		t.Fatalf("expected *ErrorResponse, got %T", err)
	}
	if errResp.Errors[0].ErrorCode != "E0100001" {
		t.Errorf("ErrorCode = %q, want E0100001", errResp.Errors[0].ErrorCode)
	}
}

func TestClient_DefaultSandboxURL(t *testing.T) {
	client := NewClient(Config{APIKey: "sk_test"})
	if client.baseURL != SandboxBaseURL {
		t.Errorf("baseURL = %q, want %q", client.baseURL, SandboxBaseURL)
	}
}

func TestClient_CustomHTTPClient(t *testing.T) {
	custom := &http.Client{}
	client := NewClient(Config{APIKey: "sk_test"}, WithHTTPClient(custom))
	if client.httpClient != custom {
		t.Error("expected custom http client")
	}
}

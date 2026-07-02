package fincode

import (
	"bytes"
	"context"
	"encoding/json"
	"fmt"
	"io"
	"net/http"
	"net/url"
	"time"
)

const (
	ProductionBaseURL = "https://api.fincode.jp"
	SandboxBaseURL    = "https://api.test.fincode.jp"

	// DefaultTimeout is the default per-request timeout applied when the
	// caller does not supply their own http.Client. Payment endpoints should
	// not exceed this under normal conditions; callers needing longer
	// timeouts should supply a custom http.Client via WithHTTPClient.
	DefaultTimeout = 30 * time.Second
)

// Config holds fincode client configuration.
type Config struct {
	APIKey  string
	BaseURL string // defaults to SandboxBaseURL if empty
}

func (c Config) baseURL() string {
	if c.BaseURL != "" {
		return c.BaseURL
	}
	return SandboxBaseURL
}

// Client is a low-level HTTP client for the fincode API.
type Client struct {
	apiKey     string
	baseURL    string
	httpClient *http.Client
}

// NewClient creates a new fincode API client.
//
// By default, a new http.Client with a DefaultTimeout is created. Use
// WithHTTPClient to override this (e.g., to share a client or configure
// transport-level retries).
func NewClient(cfg Config, opts ...ClientOption) *Client {
	c := &Client{
		apiKey:  cfg.APIKey,
		baseURL: cfg.baseURL(),
		httpClient: &http.Client{
			Timeout: DefaultTimeout,
		},
	}
	for _, opt := range opts {
		opt(c)
	}
	return c
}

// ClientOption configures the Client.
type ClientOption func(*Client)

// WithHTTPClient sets a custom http.Client. When set, the caller is
// responsible for configuring timeouts.
func WithHTTPClient(hc *http.Client) ClientOption {
	return func(c *Client) { c.httpClient = hc }
}

// RequestOption configures a single HTTP request (e.g., for setting
// per-request headers like idempotent_key).
type RequestOption func(*http.Request)

// WithIdempotencyKey sets the fincode idempotent_key header. An empty key is
// a no-op. Per fincode docs: 30-minute TTL, the same key returns the same
// response instead of re-processing.
func WithIdempotencyKey(key string) RequestOption {
	return func(r *http.Request) {
		if key != "" {
			r.Header.Set("idempotent_key", key)
		}
	}
}

// CreatePayment registers a new payment (POST /v1/payments).
//
// If idempotencyKey is non-empty, it is forwarded as the fincode
// `idempotent_key` header so retries within 30 minutes return the same
// registered order rather than creating a duplicate.
func (c *Client) CreatePayment(ctx context.Context, req *CreatePaymentRequest, idempotencyKey string) (*PaymentResponse, error) {
	return doJSON[PaymentResponse](c, ctx, http.MethodPost, "/v1/payments", req, WithIdempotencyKey(idempotencyKey))
}

// ExecutePayment executes a registered payment (PUT /v1/payments/{id}).
func (c *Client) ExecutePayment(ctx context.Context, orderID string, req *ExecutePaymentRequest) (*PaymentResponse, error) {
	return doJSON[PaymentResponse](c, ctx, http.MethodPut, "/v1/payments/"+url.PathEscape(orderID), req)
}

// CapturePayment captures an authorized payment (PUT /v1/payments/{id}/capture).
// A non-empty idempotencyKey is forwarded as the fincode `idempotent_key`
// header (fincode accepts the header on POST/PUT endpoints; see CreatePayment).
func (c *Client) CapturePayment(ctx context.Context, orderID string, req *CapturePaymentRequest, opts ...RequestOption) (*PaymentResponse, error) {
	return doJSON[PaymentResponse](c, ctx, http.MethodPut, "/v1/payments/"+url.PathEscape(orderID)+"/capture", req, opts...)
}

// CancelPayment cancels a payment (PUT /v1/payments/{id}/cancel).
// For AUTHORIZED payments this voids the authorization; for CAPTURED card
// payments fincode attempts a reversal which may or may not complete
// depending on the acquirer's settlement state.
func (c *Client) CancelPayment(ctx context.Context, orderID string, req *CancelPaymentRequest, opts ...RequestOption) (*PaymentResponse, error) {
	return doJSON[PaymentResponse](c, ctx, http.MethodPut, "/v1/payments/"+url.PathEscape(orderID)+"/cancel", req, opts...)
}

// ChangeAmount modifies a payment's amount (PUT /v1/payments/{id}/change).
// The request's Amount is the NEW total amount, not a delta. Used to implement
// partial refunds by lowering the amount of a CAPTURED payment.
func (c *Client) ChangeAmount(ctx context.Context, orderID string, req *ChangeAmountRequest, opts ...RequestOption) (*PaymentResponse, error) {
	return doJSON[PaymentResponse](c, ctx, http.MethodPut, "/v1/payments/"+url.PathEscape(orderID)+"/change", req, opts...)
}

// RetrievePayment gets payment details (GET /v1/payments/{id}).
func (c *Client) RetrievePayment(ctx context.Context, orderID string, payType PayType) (*PaymentResponse, error) {
	q := url.Values{}
	q.Set("pay_type", string(payType))
	path := "/v1/payments/" + url.PathEscape(orderID) + "?" + q.Encode()
	return doJSON[PaymentResponse](c, ctx, http.MethodGet, path, nil)
}

// --- Customer card API (payment methods) ---

// CreateCard registers a tokenized card for a customer
// (POST /v1/customers/{customer_id}/cards).
func (c *Client) CreateCard(ctx context.Context, customerID string, req *CreateCardRequest) (*CardResponse, error) {
	return doJSON[CardResponse](c, ctx, http.MethodPost,
		"/v1/customers/"+url.PathEscape(customerID)+"/cards", req)
}

// RetrieveCard gets a stored card (GET /v1/customers/{customer_id}/cards/{id}).
func (c *Client) RetrieveCard(ctx context.Context, customerID, cardID string) (*CardResponse, error) {
	return doJSON[CardResponse](c, ctx, http.MethodGet,
		"/v1/customers/"+url.PathEscape(customerID)+"/cards/"+url.PathEscape(cardID), nil)
}

// ListCards lists a customer's stored cards (GET /v1/customers/{customer_id}/cards).
func (c *Client) ListCards(ctx context.Context, customerID string) (*CardListResponse, error) {
	return doJSON[CardListResponse](c, ctx, http.MethodGet,
		"/v1/customers/"+url.PathEscape(customerID)+"/cards", nil)
}

// DeleteCard removes a stored card (DELETE /v1/customers/{customer_id}/cards/{id}).
func (c *Client) DeleteCard(ctx context.Context, customerID, cardID string) (*DeleteCardResponse, error) {
	return doJSON[DeleteCardResponse](c, ctx, http.MethodDelete,
		"/v1/customers/"+url.PathEscape(customerID)+"/cards/"+url.PathEscape(cardID), nil)
}

// doJSON sends an HTTP request and decodes a JSON response.
//
// On success it returns a pointer to a freshly decoded T.
// On HTTP status >= 400 it returns a *HTTPError carrying the status, the
// raw body, and (if the body is a well-formed ErrorResponse) the parsed
// APIError chain.
//
// doJSON delegates to doJSONRaw and discards the raw response bytes. Nothing
// in this package currently consumes them; doJSONRaw exists so a future
// caller that needs the exact server bytes (audit / RawResponse) can get
// them without re-marshalling.
func doJSON[T any](c *Client, ctx context.Context, method, path string, body any, opts ...RequestOption) (*T, error) {
	result, _, err := doJSONRaw[T](c, ctx, method, path, body, opts...)
	return result, err
}

func doJSONRaw[T any](c *Client, ctx context.Context, method, path string, body any, opts ...RequestOption) (*T, json.RawMessage, error) {
	var reqBody io.Reader
	if body != nil {
		data, err := json.Marshal(body)
		if err != nil {
			return nil, nil, fmt.Errorf("fincode: marshal request: %w", err)
		}
		reqBody = bytes.NewReader(data)
	}

	req, err := http.NewRequestWithContext(ctx, method, c.baseURL+path, reqBody)
	if err != nil {
		return nil, nil, fmt.Errorf("fincode: create request: %w", err)
	}
	req.Header.Set("Authorization", "Bearer "+c.apiKey)
	if body != nil {
		req.Header.Set("Content-Type", "application/json;charset=UTF-8")
	}
	for _, opt := range opts {
		opt(req)
	}

	resp, err := c.httpClient.Do(req)
	if err != nil {
		return nil, nil, fmt.Errorf("fincode: send request: %w", err)
	}
	defer resp.Body.Close()

	respBody, err := io.ReadAll(resp.Body)
	if err != nil {
		return nil, nil, fmt.Errorf("fincode: read response: %w", err)
	}

	if resp.StatusCode >= 400 {
		he := &HTTPError{
			StatusCode: resp.StatusCode,
			Method:     method,
			Path:       path,
			Body:       respBody,
		}
		// Best-effort parse. If the body isn't a valid ErrorResponse, leave
		// APIError nil; callers can still inspect StatusCode and Body.
		var errResp ErrorResponse
		if len(respBody) > 0 && json.Unmarshal(respBody, &errResp) == nil && len(errResp.Errors) > 0 {
			he.APIError = &errResp
		}
		return nil, nil, he
	}

	var result T
	if err := json.Unmarshal(respBody, &result); err != nil {
		return nil, nil, fmt.Errorf("fincode: unmarshal response: %w", err)
	}
	return &result, respBody, nil
}

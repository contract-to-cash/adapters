package fincode

import (
	"bytes"
	"context"
	"encoding/json"
	"fmt"
	"io"
	"net/http"
)

const (
	ProductionBaseURL = "https://api.fincode.jp"
	SandboxBaseURL    = "https://api.test.fincode.jp"
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
func NewClient(cfg Config, opts ...ClientOption) *Client {
	c := &Client{
		apiKey:     cfg.APIKey,
		baseURL:    cfg.baseURL(),
		httpClient: http.DefaultClient,
	}
	for _, opt := range opts {
		opt(c)
	}
	return c
}

// ClientOption configures the Client.
type ClientOption func(*Client)

// WithHTTPClient sets a custom http.Client.
func WithHTTPClient(hc *http.Client) ClientOption {
	return func(c *Client) { c.httpClient = hc }
}

// CreatePayment registers a new payment (POST /v1/payments).
func (c *Client) CreatePayment(ctx context.Context, req *CreatePaymentRequest) (*PaymentResponse, error) {
	return doJSON[PaymentResponse](c, ctx, http.MethodPost, "/v1/payments", req)
}

// ExecutePayment executes a registered payment (PUT /v1/payments/{id}).
func (c *Client) ExecutePayment(ctx context.Context, orderID string, req *ExecutePaymentRequest) (*PaymentResponse, error) {
	return doJSON[PaymentResponse](c, ctx, http.MethodPut, "/v1/payments/"+orderID, req)
}

// CapturePayment captures an authorized payment (PUT /v1/payments/{id}/capture).
func (c *Client) CapturePayment(ctx context.Context, orderID string, req *CapturePaymentRequest) (*PaymentResponse, error) {
	return doJSON[PaymentResponse](c, ctx, http.MethodPut, "/v1/payments/"+orderID+"/capture", req)
}

// CancelPayment cancels a payment (PUT /v1/payments/{id}/cancel).
func (c *Client) CancelPayment(ctx context.Context, orderID string, req *CancelPaymentRequest) (*PaymentResponse, error) {
	return doJSON[PaymentResponse](c, ctx, http.MethodPut, "/v1/payments/"+orderID+"/cancel", req)
}

// RetrievePayment gets payment details (GET /v1/payments/{id}).
func (c *Client) RetrievePayment(ctx context.Context, orderID string, payType PayType) (*PaymentResponse, error) {
	path := fmt.Sprintf("/v1/payments/%s?pay_type=%s", orderID, payType)
	return doJSON[PaymentResponse](c, ctx, http.MethodGet, path, nil)
}

func doJSON[T any](c *Client, ctx context.Context, method, path string, body any) (*T, error) {
	var reqBody io.Reader
	if body != nil {
		data, err := json.Marshal(body)
		if err != nil {
			return nil, fmt.Errorf("fincode: marshal request: %w", err)
		}
		reqBody = bytes.NewReader(data)
	}

	req, err := http.NewRequestWithContext(ctx, method, c.baseURL+path, reqBody)
	if err != nil {
		return nil, fmt.Errorf("fincode: create request: %w", err)
	}
	req.Header.Set("Authorization", "Bearer "+c.apiKey)
	if body != nil {
		req.Header.Set("Content-Type", "application/json;charset=UTF-8")
	}

	resp, err := c.httpClient.Do(req)
	if err != nil {
		return nil, fmt.Errorf("fincode: send request: %w", err)
	}
	defer resp.Body.Close()

	respBody, err := io.ReadAll(resp.Body)
	if err != nil {
		return nil, fmt.Errorf("fincode: read response: %w", err)
	}

	if resp.StatusCode >= 400 {
		var errResp ErrorResponse
		if err := json.Unmarshal(respBody, &errResp); err != nil {
			return nil, fmt.Errorf("fincode: HTTP %d: %s", resp.StatusCode, string(respBody))
		}
		return nil, &errResp
	}

	var result T
	if err := json.Unmarshal(respBody, &result); err != nil {
		return nil, fmt.Errorf("fincode: unmarshal response: %w", err)
	}
	return &result, nil
}

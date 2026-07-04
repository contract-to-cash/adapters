package stripe

import (
	"net/http"

	stripego "github.com/stripe/stripe-go/v82"
	"github.com/stripe/stripe-go/v82/customer"
	"github.com/stripe/stripe-go/v82/paymentintent"
	"github.com/stripe/stripe-go/v82/paymentmethod"
	"github.com/stripe/stripe-go/v82/refund"
)

// Config holds Stripe client configuration.
type Config struct {
	// SecretKey is the Stripe secret API key ("sk_..."). Required.
	SecretKey string

	// APIBase overrides the Stripe API base URL. Empty uses Stripe's default
	// (https://api.stripe.com). Set this to point the SDK at a local test
	// server (see gateway_test.go); it does not select live vs test mode —
	// that is determined by the SecretKey.
	APIBase string
}

// Client bundles the Stripe resource clients the adapter uses, all sharing a
// single configured backend and API key. It is safe for concurrent use.
type Client struct {
	paymentIntents *paymentintent.Client
	refunds        *refund.Client
	paymentMethods *paymentmethod.Client
	customers      *customer.Client
}

// ClientOption configures the Client.
type ClientOption func(*clientConfig)

type clientConfig struct {
	httpClient *http.Client
}

// WithHTTPClient sets a custom http.Client for all Stripe API calls (e.g. to
// share a transport or tune timeouts). When unset, the SDK's default client
// is used.
func WithHTTPClient(hc *http.Client) ClientOption {
	return func(c *clientConfig) { c.httpClient = hc }
}

// NewClient creates a Stripe client. It returns a *ValidationError when the
// secret key is empty.
func NewClient(cfg Config, opts ...ClientOption) (*Client, error) {
	if cfg.SecretKey == "" {
		return nil, &ValidationError{Field: "SecretKey", Message: "stripe secret key must not be empty"}
	}

	cc := &clientConfig{}
	for _, opt := range opts {
		opt(cc)
	}

	bcfg := &stripego.BackendConfig{}
	if cfg.APIBase != "" {
		bcfg.URL = stripego.String(cfg.APIBase)
	}
	if cc.httpClient != nil {
		bcfg.HTTPClient = cc.httpClient
	}
	backend := stripego.GetBackendWithConfig(stripego.APIBackend, bcfg)

	return &Client{
		paymentIntents: &paymentintent.Client{B: backend, Key: cfg.SecretKey},
		refunds:        &refund.Client{B: backend, Key: cfg.SecretKey},
		paymentMethods: &paymentmethod.Client{B: backend, Key: cfg.SecretKey},
		customers:      &customer.Client{B: backend, Key: cfg.SecretKey},
	}, nil
}

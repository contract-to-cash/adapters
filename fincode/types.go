// Package fincode implements the core port.PaymentGateway and
// port.WebhookHandler interfaces on top of the fincode payment API.
//
// API Reference: https://docs.fincode.jp/api
package fincode

import (
	"errors"
	"fmt"
	"strings"
)

// PayType represents fincode payment types.
type PayType string

const (
	PayTypeCard PayType = "Card"
)

// JobCode represents fincode transaction types.
type JobCode string

const (
	JobCodeAuth    JobCode = "AUTH"
	JobCodeCapture JobCode = "CAPTURE"
	JobCodeCheck   JobCode = "CHECK"
)

// PayMethod represents card payment methods.
//
// Currently only lump-sum is used by the Gateway. Installment / revolving
// require a corresponding field in payment.AuthorizeRequest which is not yet
// defined in core; see TODO in gateway.go:Authorize.
type PayMethod string

const (
	PayMethodLumpSum     PayMethod = "1" // 一括払い
	PayMethodInstallment PayMethod = "2" // 分割払い (reserved; not wired)
	PayMethodRevolving   PayMethod = "5" // リボ払い (reserved; not wired)
)

// PaymentStatus represents fincode payment statuses.
type PaymentStatus string

const (
	StatusUnprocessed   PaymentStatus = "UNPROCESSED"
	StatusChecked       PaymentStatus = "CHECKED"
	StatusAuthorized    PaymentStatus = "AUTHORIZED"
	StatusCaptured      PaymentStatus = "CAPTURED"
	StatusCanceled      PaymentStatus = "CANCELED"
	StatusAuthenticated PaymentStatus = "AUTHENTICATED"
)

// --- Request types ---

// CreatePaymentRequest is the request body for POST /v1/payments.
type CreatePaymentRequest struct {
	ID           string  `json:"id,omitempty"`
	PayType      PayType `json:"pay_type"`
	JobCode      JobCode `json:"job_code"`
	Amount       string  `json:"amount,omitempty"`
	Tax          string  `json:"tax,omitempty"`
	ClientField1 string  `json:"client_field_1,omitempty"`
	ClientField2 string  `json:"client_field_2,omitempty"`
	ClientField3 string  `json:"client_field_3,omitempty"`
	TdsType      string  `json:"tds_type,omitempty"`
	Tds2Type     string  `json:"tds2_type,omitempty"`
	TdTenantName string  `json:"td_tenant_name,omitempty"`
}

// ExecutePaymentRequest is the request body for PUT /v1/payments/{id}.
type ExecutePaymentRequest struct {
	PayType    PayType `json:"pay_type"`
	AccessID   string  `json:"access_id"`
	Token      string  `json:"token,omitempty"`
	CustomerID string  `json:"customer_id,omitempty"`
	CardID     string  `json:"card_id,omitempty"`
	Method     string  `json:"method,omitempty"`
	PayTimes   string  `json:"pay_times,omitempty"`
}

// CapturePaymentRequest is the request body for PUT /v1/payments/{id}/capture.
type CapturePaymentRequest struct {
	PayType  PayType `json:"pay_type"`
	AccessID string  `json:"access_id"`
	Method   string  `json:"method,omitempty"`
	PayTimes string  `json:"pay_times,omitempty"`
}

// CancelPaymentRequest is the request body for PUT /v1/payments/{id}/cancel.
type CancelPaymentRequest struct {
	PayType  PayType `json:"pay_type"`
	AccessID string  `json:"access_id"`
}

// ChangeAmountRequest is the request body for PUT /v1/payments/{id}/change.
// Used to reduce the amount of a captured payment (partial refund).
// The amount field is the NEW total amount after change, not a delta.
type ChangeAmountRequest struct {
	PayType  PayType `json:"pay_type"`
	AccessID string  `json:"access_id"`
	JobCode  JobCode `json:"job_code"` // AUTH or CAPTURE
	Amount   string  `json:"amount"`
	Tax      string  `json:"tax,omitempty"`
}

// --- Response types ---

// PaymentResponse represents the common payment response from fincode.
type PaymentResponse struct {
	ShopID        string        `json:"shop_id"`
	ID            string        `json:"id"`
	AccessID      string        `json:"access_id"`
	Amount        int64         `json:"amount"`
	Tax           int64         `json:"tax"`
	TotalAmount   int64         `json:"total_amount"`
	ClientField1  string        `json:"client_field_1"`
	ClientField2  string        `json:"client_field_2"`
	ClientField3  string        `json:"client_field_3"`
	ProcessDate   string        `json:"process_date"`
	CustomerID    string        `json:"customer_id"`
	PayType       PayType       `json:"pay_type"`
	Status        PaymentStatus `json:"status"`
	JobCode       JobCode       `json:"job_code"`
	CardID        string        `json:"card_id"`
	Brand         string        `json:"brand"`
	CardNo        string        `json:"card_no"`
	Expire        string        `json:"expire"`
	HolderName    string        `json:"holder_name"`
	Method        string        `json:"method"`
	PayTimes      *int          `json:"pay_times"`
	Forward       string        `json:"forward"`
	Issuer        string        `json:"issuer"`
	TransactionID string        `json:"transaction_id"`
	Approve       string        `json:"approve"`
	AuthMaxDate   string        `json:"auth_max_date"`
	ErrorCode     string        `json:"error_code"`
	Created       string        `json:"created"`
	Updated       string        `json:"updated"`
}

// --- Customer card types (payment methods) ---

// CreateCardRequest is the request body for POST /v1/customers/{customer_id}/cards.
type CreateCardRequest struct {
	Token       string `json:"token"`
	DefaultFlag string `json:"default_flag,omitempty"` // "0" or "1"
}

// CardResponse represents a stored customer card.
type CardResponse struct {
	CustomerID  string `json:"customer_id"`
	ID          string `json:"id"`
	DefaultFlag string `json:"default_flag"`
	CardNo      string `json:"card_no"` // masked
	Expire      string `json:"expire"`  // yymm
	HolderName  string `json:"holder_name"`
	Type        string `json:"type"`
	Brand       string `json:"brand"`
	Created     string `json:"created"`
	Updated     string `json:"updated"`
}

// CardListResponse is the response body for GET /v1/customers/{customer_id}/cards.
type CardListResponse struct {
	List []CardResponse `json:"list"`
}

// DeleteCardResponse is the response body for DELETE /v1/customers/{customer_id}/cards/{id}.
type DeleteCardResponse struct {
	CustomerID string `json:"customer_id"`
	ID         string `json:"id"`
	DeleteFlag string `json:"delete_flag"`
}

// APIError represents a single error from fincode API.
type APIError struct {
	ErrorCode    string `json:"error_code"`
	ErrorMessage string `json:"error_message"`
}

// ErrorResponse represents the fincode API error response.
type ErrorResponse struct {
	Errors []APIError `json:"errors"`
}

func (e *ErrorResponse) Error() string {
	if len(e.Errors) == 0 {
		return "fincode: unknown error"
	}
	parts := make([]string, 0, len(e.Errors))
	for _, ae := range e.Errors {
		parts = append(parts, ae.ErrorCode+": "+ae.ErrorMessage)
	}
	return "fincode: " + strings.Join(parts, "; ")
}

// HTTPError represents an HTTP-level error from the fincode API, wrapping the
// status code and (if the body was JSON) the parsed ErrorResponse. The raw
// Body is preserved for logging and debugging.
//
// Callers can use errors.As to extract *HTTPError for status-based dispatch
// (retry 5xx/429, fail fast on 4xx user errors) and errors.As to extract
// *ErrorResponse for fincode error_code inspection.
type HTTPError struct {
	StatusCode int
	Method     string
	Path       string
	APIError   *ErrorResponse
	Body       []byte
}

func (e *HTTPError) Error() string {
	if e.APIError != nil {
		return fmt.Sprintf("fincode: %s %s: HTTP %d: %s", e.Method, e.Path, e.StatusCode, e.APIError.Error())
	}
	return fmt.Sprintf("fincode: %s %s: HTTP %d: %s", e.Method, e.Path, e.StatusCode, string(e.Body))
}

func (e *HTTPError) Unwrap() error {
	if e.APIError != nil {
		return e.APIError
	}
	return nil
}

// ValidationError is returned for client-side input validation failures,
// before any network call is made.
type ValidationError struct {
	Field   string
	Message string
}

func (e *ValidationError) Error() string {
	return fmt.Sprintf("fincode: validation: %s: %s", e.Field, e.Message)
}

// ErrValidation is a sentinel error that ValidationError wraps for errors.Is.
var ErrValidation = errors.New("fincode: validation failed")

func (e *ValidationError) Is(target error) bool {
	return target == ErrValidation
}

// PartialAuthorizeError is returned from Gateway.Charge / Gateway.Authorize
// when an order is registered at fincode (POST /v1/payments) but not executed
// to completion: either the execute step (PUT /v1/payments/{id}) failed, or a
// retried request found an order left in that state by an earlier attempt
// with the same IdempotencyKey. The OrderID and AccessID identify the
// registered-but-not-executed payment, so the caller can retry the execute
// step by calling Gateway.CompleteCharge / Gateway.CompleteAuthorize with
// those values. fincode does not bill until the execute step completes.
type PartialAuthorizeError struct {
	OrderID  string
	AccessID string
	Cause    error
}

func (e *PartialAuthorizeError) Error() string {
	return fmt.Sprintf("fincode: partial authorize: registered order=%s access=%s but execute failed: %v",
		e.OrderID, e.AccessID, e.Cause)
}

func (e *PartialAuthorizeError) Unwrap() error { return e.Cause }

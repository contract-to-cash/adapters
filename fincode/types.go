// Package fincode implements a payment.Gateway adapter for the fincode payment API.
//
// API Reference: https://docs.fincode.jp/api
package fincode

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
type PayMethod string

const (
	PayMethodLumpSum     PayMethod = "1" // 一括払い
	PayMethodInstallment PayMethod = "2" // 分割払い
	PayMethodRevolving   PayMethod = "5" // リボ払い
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
	return "fincode: " + e.Errors[0].ErrorCode + ": " + e.Errors[0].ErrorMessage
}

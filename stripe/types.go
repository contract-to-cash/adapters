// Package stripe implements the core port.PaymentGateway and
// port.WebhookHandler interfaces on top of the Stripe API, using the official
// stripe-go SDK (github.com/stripe/stripe-go/v82).
//
// API reference: https://docs.stripe.com/api
//
// # ID mapping conventions
//
//   - port TransactionID and AuthorizationID are both the Stripe PaymentIntent
//     ID ("pi_..."). Charge/Authorize create a PaymentIntent; Capture, Void,
//     Cancel and GetTransaction operate on it by that single ID (Stripe does
//     not need a separate access token, unlike some gateways).
//   - RefundRequest.TransactionID is the PaymentIntent ID; the returned
//     RefundResponse.RefundID is the Stripe refund ID ("re_...").
//   - Payment method IDs are the Stripe PaymentMethod ID ("pm_..."). They are
//     flat (no compositing) and global — Get/Delete take the ID directly,
//     List takes the customer ID.
//
// # Currency and amounts
//
// Stripe amounts are integers in the currency's smallest unit (cents for
// USD/EUR, whole yen for the zero-decimal JPY). The adapter converts
// shared.Money (a big.Rat) to those minor units using currencyExponent and
// rejects amounts that are not exactly representable, non-positive, or out of
// int64 range with a typed error. Only the currencies defined in
// domain/shared (JPY, USD, EUR) are supported; extend currencyExponent when
// core adds more. A Stripe response reporting an amount in any other currency
// is rejected rather than decoded at a guessed exponent.
//
// # Idempotency (24h window)
//
// An IdempotencyKey set on Charge, Authorize, Capture or Refund is forwarded
// verbatim as Stripe's Idempotency-Key header. Stripe only retains an
// idempotency key for about 24 hours, after which the same key is treated as a
// fresh request — so a retry beyond that window is NOT deduplicated by Stripe
// and can double-charge. This adapter therefore satisfies the core
// PaymentService's "gateway deduplicates on IdempotencyKey" contract only
// within 24h. For durable deduplication (e.g. a payment retried days later by
// a dunning batch), pair the gateway with the core port.IdempotencyStore so
// the outcome is remembered independently of Stripe's window.
package stripe

import (
	"errors"
	"fmt"
	"math/big"
	"strings"

	stripego "github.com/stripe/stripe-go/v82"

	"github.com/contract-to-cash/core/application/port"
	"github.com/contract-to-cash/core/domain/shared"
)

// GatewayID is the identifier returned by Gateway.ID().
const GatewayID = "stripe"

// currencyExponent maps a supported currency to the number of decimal places
// in its Stripe minor unit. JPY is a zero-decimal currency; USD and EUR use
// cents. Currencies absent from this map are rejected with
// port.ErrorCodeCurrencyNotSupported.
var currencyExponent = map[shared.Currency]int{
	shared.CurrencyJPY: 0,
	shared.CurrencyUSD: 2,
	shared.CurrencyEUR: 2,
}

// pow10 returns 10^n as a *big.Int (n >= 0).
func pow10(n int) *big.Int {
	return new(big.Int).Exp(big.NewInt(10), big.NewInt(int64(n)), nil)
}

// toMinorUnits converts a shared.Money to a Stripe integer amount in the
// currency's smallest unit plus the lowercase ISO currency code Stripe
// expects. It returns typed errors for unsupported currency
// (*port.GatewayError), and for fractional-minor-unit, non-positive, or
// out-of-range amounts (*ValidationError).
func toMinorUnits(field string, m shared.Money) (int64, string, error) {
	exp, ok := currencyExponent[m.Currency()]
	if !ok {
		return 0, "", &port.GatewayError{
			Code:    port.ErrorCodeCurrencyNotSupported,
			Message: fmt.Sprintf("stripe adapter supports %v only, got %q", supportedCurrencies(), m.Currency()),
		}
	}

	scaled := new(big.Rat).Mul(m.Amount(), new(big.Rat).SetInt(pow10(exp)))
	if !scaled.IsInt() {
		return 0, "", &ValidationError{
			Field:   field,
			Message: fmt.Sprintf("amount %s is not representable in %s minor units", m.Amount().RatString(), m.Currency()),
		}
	}
	// scaled is normalized (denominator 1), so Num() is the integer value.
	num := scaled.Num()
	if !num.IsInt64() {
		return 0, "", &ValidationError{Field: field, Message: "amount exceeds int64 range"}
	}
	v := num.Int64()
	if v <= 0 {
		return 0, "", &ValidationError{Field: field, Message: "amount must be positive"}
	}
	return v, strings.ToLower(string(m.Currency())), nil
}

// fromMinorUnits builds a shared.Money from a Stripe integer amount and its
// currency code (Stripe returns lowercase ISO codes). A currency absent from
// currencyExponent is rejected with a *port.GatewayError rather than being
// silently decoded at a zero exponent: guessing whole units would misreport a
// two-decimal amount by 100x (e.g. GBP 2599 minor units read back as 2599
// instead of 25.99). Callers propagate the error so a response in an
// unexpected currency surfaces loudly instead of corrupting the recorded
// amount.
func fromMinorUnits(amount int64, cur stripego.Currency) (shared.Money, error) {
	currency := shared.Currency(strings.ToUpper(string(cur)))
	exp, ok := currencyExponent[currency]
	if !ok {
		return shared.Money{}, &port.GatewayError{
			Code: port.ErrorCodeCurrencyNotSupported,
			Message: fmt.Sprintf(
				"stripe: cannot convert %d minor units: response currency %q is not one of %v",
				amount, cur, supportedCurrencies(),
			),
		}
	}
	r := new(big.Rat).SetFrac(big.NewInt(amount), pow10(exp))
	return shared.NewMoney(r, currency), nil
}

func supportedCurrencies() []shared.Currency {
	return []shared.Currency{shared.CurrencyJPY, shared.CurrencyUSD, shared.CurrencyEUR}
}

// cardBrandMap translates Stripe card brand strings to port.CardBrand.
var cardBrandMap = map[string]port.CardBrand{
	"visa":       port.CardBrandVisa,
	"mastercard": port.CardBrandMastercard,
	"amex":       port.CardBrandAmex,
	"jcb":        port.CardBrandJCB,
	"diners":     port.CardBrandDiners,
	"discover":   port.CardBrandDiscover,
}

func toCardBrand(brand string) port.CardBrand {
	if b, ok := cardBrandMap[strings.ToLower(brand)]; ok {
		return b
	}
	return port.CardBrandUnknown
}

// ValidationError is returned for client-side input validation failures,
// before any network call is made.
type ValidationError struct {
	Field   string
	Message string
}

func (e *ValidationError) Error() string {
	return fmt.Sprintf("stripe: validation: %s: %s", e.Field, e.Message)
}

// ErrValidation is a sentinel error that ValidationError wraps for errors.Is.
var ErrValidation = errors.New("stripe: validation failed")

func (e *ValidationError) Is(target error) bool { return target == ErrValidation }

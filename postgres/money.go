package postgres

import (
	"math/big"

	"github.com/contract-to-cash/core/domain/shared"
)

// moneyFromInt64 builds a shared.Money from a zero-decimal int64 column
// (the representation used by the BIGINT monetary columns in the schema).
//
// This is lossy for currencies with subunits (USD cents, EUR cents) — the
// rule is: zero-decimal currencies (JPY, KRW) are exact; anything else
// requires additional scaling, which the PG reference adapter does not
// attempt. Applications that need non-JPY precision should store money as
// JSONB via shared.Money's MarshalJSON/UnmarshalJSON, or add their own
// minor-unit conversion on top of this helper.
func moneyFromInt64(amount int64, currency shared.Currency) shared.Money {
	return shared.NewMoney(new(big.Rat).SetInt64(amount), currency)
}

package postgres

import (
	"math/big"

	"github.com/contract-to-cash/core/domain/shared"
)

// moneyFromInt64 builds a shared.Money from a BIGINT column value.
func moneyFromInt64(amount int64, currency shared.Currency) shared.Money {
	return shared.NewMoney(new(big.Rat).SetInt64(amount), currency)
}

// parseMoneyPayload extracts a monetary amount (as a whole-unit int64) and its
// currency from a value decoded out of an event's JSON payload.
//
// core marshals shared.Money as {"amount":"11000/1","currency":"JPY"} where the
// amount is a big.Rat RatString, so after json.Unmarshal into map[string]any the
// value arrives as a nested map. A bare JSON number (legacy/numeric payloads) is
// also accepted. Because the read-model amount columns are BIGINT, a fractional
// amount is truncated toward the same value shared.Money.Int64 would yield (the
// full-precision amount is preserved in the stored `data` JSON). ok is false when
// the value is absent or not a recognizable money shape, letting callers keep the
// column's zero/default.
func parseMoneyPayload(v any) (amount int64, currency string, ok bool) {
	switch t := v.(type) {
	case map[string]any:
		amountStr, _ := t["amount"].(string)
		cur, _ := t["currency"].(string)
		r := new(big.Rat)
		if _, parsed := r.SetString(amountStr); !parsed {
			return 0, cur, false
		}
		return new(big.Int).Div(r.Num(), r.Denom()).Int64(), cur, true
	case float64:
		return int64(t), "", true
	case string:
		r := new(big.Rat)
		if _, parsed := r.SetString(t); !parsed {
			return 0, "", false
		}
		return new(big.Int).Div(r.Num(), r.Denom()).Int64(), "", true
	default:
		return 0, "", false
	}
}

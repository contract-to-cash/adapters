package postgres

import (
	"fmt"
	"math/big"

	"github.com/contract-to-cash/core/domain/balance"
	"github.com/contract-to-cash/core/domain/shared"
)

// moneyFromInt64 builds a shared.Money from a BIGINT column value.
//
// This is a lossy, back-compat path: it is only used to reconstruct rows that
// predate the precise `state` JSON column (see issue #11). New rows carry the
// exact big.Rat amount in state JSON.
func moneyFromInt64(amount int64, currency shared.Currency) shared.Money {
	return shared.NewMoney(new(big.Rat).SetInt64(amount), currency)
}

// filterAvailableBalance keeps entries with a strictly positive remaining
// amount, mirroring the old `remaining_amount > 0` SQL predicate but on the
// precise (non-truncated) amount.
func filterAvailableBalance(entries []*balance.BalanceEntry) []*balance.BalanceEntry {
	var result []*balance.BalanceEntry
	for _, e := range entries {
		rem := e.RemainingAmount()
		if rem.IsZero() || rem.IsNegative() {
			continue
		}
		result = append(result, e)
	}
	return result
}

// sumRemaining adds up the remaining amounts of the given entries using exact
// Money arithmetic.
func sumRemaining(entries []*balance.BalanceEntry, currency shared.Currency) (shared.Money, error) {
	total := shared.Zero(currency)
	for _, e := range entries {
		var err error
		total, err = total.Add(e.RemainingAmount())
		if err != nil {
			return shared.Money{}, fmt.Errorf("sum balance remaining: %w", err)
		}
	}
	return total, nil
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

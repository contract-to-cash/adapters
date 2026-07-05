package mysql

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

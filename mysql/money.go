package mysql

import (
	"math/big"

	"github.com/contract-to-cash/core/domain/shared"
)

// moneyFromInt64 builds a shared.Money from a BIGINT column value.
func moneyFromInt64(amount int64, currency shared.Currency) shared.Money {
	return shared.NewMoney(new(big.Rat).SetInt64(amount), currency)
}

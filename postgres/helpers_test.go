package postgres_test

import (
	"errors"

	"github.com/contract-to-cash/core/domain/shared"
)

// isDomainError reports whether err wraps a *shared.DomainError with the
// given code. (core does not export such a helper.)
func isDomainError(err error, code shared.ErrorCode) bool {
	var de *shared.DomainError
	return errors.As(err, &de) && de.Code == code
}

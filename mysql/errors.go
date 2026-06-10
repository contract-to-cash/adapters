package mysql

import (
	"errors"

	"github.com/contract-to-cash/core/domain/shared"
	driver "github.com/go-sql-driver/mysql"
)

// mysqlErrDupEntry is MySQL's ER_DUP_ENTRY error number, returned when an INSERT
// violates a UNIQUE constraint (here: uq_stream_version / uq_event_id).
const mysqlErrDupEntry = 1062

// isDuplicateKey reports whether err is a MySQL duplicate-key (1062) error.
func isDuplicateKey(err error) bool {
	var me *driver.MySQLError
	return errors.As(err, &me) && me.Number == mysqlErrDupEntry
}

// versionConflict wraps a cause in the core's structured optimistic-concurrency
// error so callers can detect it via errors.As(*shared.DomainError) and Code.
func versionConflict(streamID string, cause error) error {
	return shared.NewDomainErrorWithCause(
		shared.ErrCodeVersionConflict,
		"event store: optimistic concurrency conflict for stream "+streamID,
		cause,
	)
}

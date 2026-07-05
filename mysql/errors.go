package mysql

import (
	"errors"
	"strings"

	"github.com/contract-to-cash/core/domain/shared"
	driver "github.com/go-sql-driver/mysql"
)

// mysqlErrDupEntry is MySQL's ER_DUP_ENTRY error number, returned when an INSERT
// violates a UNIQUE or PRIMARY KEY constraint. In the events table this can be
// either uq_stream_version (a genuine optimistic-concurrency conflict) or
// uq_event_id / PRIMARY (a duplicate event ID, i.e. a caller/infrastructure
// bug). MySQL reports the offending key name in the error message, e.g.
//
//	Duplicate entry 'stream-1-2' for key 'events.uq_stream_version'
//	Duplicate entry 'evt-abc'    for key 'events.uq_event_id'
//
// so the two cases can be told apart by matching the key name rather than the
// numeric code alone.
const mysqlErrDupEntry = 1062

// dupEntryOnKey reports whether err is a MySQL duplicate-key (1062) error whose
// message names the given constraint/index. The key name substring must be
// present; if MySQL ever omits it we conservatively return false so an
// unrecognised duplicate is never mistaken for the requested one.
func dupEntryOnKey(err error, keyName string) bool {
	var me *driver.MySQLError
	if !errors.As(err, &me) || me.Number != mysqlErrDupEntry {
		return false
	}
	return strings.Contains(me.Message, keyName)
}

// isStreamVersionConflict reports whether err is a duplicate-key error on the
// uq_stream_version UNIQUE constraint. Only that constraint represents a
// retryable optimistic-concurrency conflict.
func isStreamVersionConflict(err error) bool {
	return dupEntryOnKey(err, "uq_stream_version")
}

// isDuplicateEventID reports whether err is a duplicate-key error on the
// uq_event_id UNIQUE constraint, i.e. the same event ID was written twice. This
// is a non-retryable fault (an application or upstream bug), NOT a version
// conflict.
func isDuplicateEventID(err error) bool {
	return dupEntryOnKey(err, "uq_event_id")
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

// duplicateEventID wraps a cause in the core's structured conflict error. Unlike
// versionConflict this maps to ErrCodeConflict, which callers must treat as a
// non-retryable data fault: a duplicate event ID means the very same event was
// appended twice (an application or infrastructure bug), and retrying will
// never succeed.
func duplicateEventID(eventID string, cause error) error {
	return shared.NewDomainErrorWithCause(
		shared.ErrCodeConflict,
		"event store: duplicate event id "+eventID,
		cause,
	)
}

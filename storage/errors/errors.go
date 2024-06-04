// Copyright JAMF Software, LLC

package errors

import (
	"errors"

	"github.com/jamf/regatta/raft"
)

// IsSafeToRetry returns true for transient errors
// for operations that client could attempt to retry using the same arguments.
func IsSafeToRetry(err error) bool {
	return raft.IsTempError(err)
}

var (
	// ErrTableNotFound returned when the table is not found.
	ErrTableNotFound = errors.New("table not found")
	// ErrEmptyKey returned when the key is not provided.
	ErrEmptyKey = errors.New("key must not be empty")
	// ErrKeyLengthExceeded key length exceeded max allowed value.
	ErrKeyLengthExceeded = errors.New("key length exceeded max allowed value")
	// ErrValueLengthExceeded value length exceeded max allowed value.
	ErrValueLengthExceeded = errors.New("value length exceeded max allowed value")
	// ErrUnknownQueryType unknown type sent to the Lookup method.
	ErrUnknownQueryType = errors.New("unknown query type")
	// ErrInvalidNodeID invalid (negative or 0) node ID.
	ErrInvalidNodeID = errors.New("invalid node ID")
	// ErrInvalidClusterID invalid (negative or 0) cluster ID.
	ErrInvalidClusterID = errors.New("invalid cluster ID")
	// ErrStateMachineClosed state machine is closed.
	ErrStateMachineClosed = errors.New("calling action on closed state machine")
	// ErrNoResultFound FSM should have returned a result, but it didn't.
	ErrNoResultFound = errors.New("FSM returned no results")
	// ErrUnknownResultType FSM returned a result of the wrong type.
	ErrUnknownResultType = errors.New("unknown result type")

	ErrTableExists             = errors.New("table already exists")
	ErrManagerClosed           = errors.New("manager closed")
	ErrLeaseNotAcquired        = errors.New("lease not acquired")
	ErrNodeHostInfoUnavailable = errors.New("nodehost info unavailable")

	// ErrLogBehind the queried log is behind and contains only older indices.
	ErrLogBehind = errors.New("queried log is behind")
	// ErrLogAhead the queried log is ahead and contains only newer indices.
	ErrLogAhead = errors.New("queried log is ahead")
)

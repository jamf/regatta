package storage

import (
	"errors"
)

var (
	// ErrNotFound returned when the key is not found.
	ErrNotFound = errors.New("key not found")
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
)

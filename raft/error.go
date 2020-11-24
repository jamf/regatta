package raft

import "errors"

var (
	// ErrUnknownQueryType unknown type sent to the Lookup method.
	ErrUnknownQueryType = errors.New("unknown query type")
	// ErrInvalidNodeID invalid (negative or 0) node ID.
	ErrInvalidNodeID = errors.New("invalid node ID")
	// ErrInvalidClusterID invalid (negative or 0) cluster ID.
	ErrInvalidClusterID = errors.New("invalid cluster ID")
	// ErrIsNotDir requested file is not a directory.
	ErrIsNotDir = errors.New("is not a dir")
	// ErrStateMachineClosed state machine is closed.
	ErrStateMachineClosed = errors.New("calling action on closed state machine")
)

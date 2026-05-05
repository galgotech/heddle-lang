package core

import (
	"context"

	"github.com/apache/arrow/go/v18/arrow"
)

// Resource represents an external dependency or stateful object
// (e.g., HTTP server, database connection pool) initialized by the user.
type Resource interface {
	// Start starts the resource and is called after the resource struct is created.
	Start(ctx context.Context) error
}

// Table is the universal data interface for Heddle Lang.
// It wraps an Apache Arrow Record to provide zero-copy data passing.
type Table interface {
	// Native returns the underlying Arrow Record.
	Native() arrow.Record
	// ToBytes serializes the table to Arrow IPC format.
	ToBytes() ([]byte, error)
	// Release releases the underlying memory.
	Release()
	// WriteToHandle writes the table to a file handle.
	WriteToHandle(handle string) error
}

package core

import "context"

// Resource represents an external dependency or stateful object
// (e.g., HTTP server, database connection pool) initialized by the user.
type Resource interface {
	// Start starts the resource and is called after the resource struct is created.
	Start(ctx context.Context) error
}

package state

import (
	"context"
)

// Credentials carries authentication metadata.
type Credentials struct {
	Token string
}

// Metadata is a generic store for Heddle-specific metadata.
type Metadata struct {
	Values map[string]any
}

// HeddleContext is a specialized context carrier for execution-scoped state.
type HeddleContext struct {
	context.Context
	Credentials Credentials
	Metadata    Metadata
}

// NewHeddleContext initializes a new HeddleContext.
func NewHeddleContext(ctx context.Context, creds Credentials, meta Metadata) *HeddleContext {
	return &HeddleContext{
		Context:     ctx,
		Credentials: creds,
		Metadata:    meta,
	}
}

package plugin

import (
	"context"
)

// Resource represents an external dependency or stateful object
// (e.g., database connection pool, API client) initialized by the Heddle runtime.
type Resource interface {
	Start(ctx context.Context) error
}

// Config represents the configuration contract for a Heddle Step or Resource.
// It can optionally implement the Validate method to provide custom validation logic.
type Config struct {
	resource Resource
}

func (c *Config) HasResource() bool {
	return c.resource != nil
}

func (c *Config) GetResource() Resource {
	return c.resource
}

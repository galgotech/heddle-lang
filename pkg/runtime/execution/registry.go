package execution

import (
	"context"
	"fmt"
	"sync"

	"github.com/apache/arrow/go/v18/arrow"
)

// StepFunc is the signature for a Go-based imperative step.
// It takes a context and an Arrow RecordBatch, and returns a new RecordBatch and an error.
type StepFunc func(ctx context.Context, input arrow.Record) (arrow.Record, error)

// Registry manages a set of named Go steps.
type Registry struct {
	mu    sync.RWMutex
	steps map[string]StepFunc
}

// NewRegistry creates a new empty registry.
func NewRegistry() *Registry {
	return &Registry{
		steps: make(map[string]StepFunc),
	}
}

// Register adds a new step to the registry.
func (r *Registry) Register(module, name string, fn StepFunc) {
	r.mu.Lock()
	defer r.mu.Unlock()
	key := fmt.Sprintf("%s:%s", module, name)
	r.steps[key] = fn
}

// Get retrieves a step by its module and name.
func (r *Registry) Get(module, name string) (StepFunc, bool) {
	r.mu.RLock()
	defer r.mu.RUnlock()
	key := fmt.Sprintf("%s:%s", module, name)
	fn, ok := r.steps[key]
	return fn, ok
}

// GlobalRegistry is a default registry for the application.
var GlobalRegistry = NewRegistry()

package plugin

import (
	"context"
	"fmt"
	"reflect"

	"github.com/galgotech/heddle-lang/sdk/go/core"
)

// StepFunc is a strict signature for steps without resources.
type StepFunc[C any] func(ctx context.Context, config C, input *core.Table) (*core.Table, error)

// ResourceStepFunc is a strict signature for steps with resources.
type ResourceStepFunc[C any, R any] func(ctx context.Context, config C, resource R, input *core.Table) (*core.Table, error)

// ResourceFunc is a strict signature for resource initializers.
type ResourceFunc[C any, R any] func(ctx context.Context, config C) (R, error)

// ResourceRegistration stores metadata about a registered resource.
type ResourceRegistration struct {
	Name         string
	Fn           reflect.Value
	ConfigSchema string
}

// StepRegistration stores metadata about a registered step.
type StepRegistration struct {
	Name         string
	Fn           reflect.Value
	ResourceName string // Optional: if empty, no resource is required
	ConfigSchema string
}

// Registry stores registered resource and step functions.
type Registry struct {
	resources map[string]ResourceRegistration
	steps     map[string]StepRegistration
}

// NewRegistry creates a new Registry.
func NewRegistry() *Registry {
	return &Registry{
		resources: make(map[string]ResourceRegistration),
		steps:     make(map[string]StepRegistration),
	}
}

// StepOption configures step registration.
type StepOption func(*StepRegistration)

// WithResource specifies that a step requires a registered resource.
func WithResource(resourceName string) StepOption {
	return func(s *StepRegistration) {
		s.ResourceName = resourceName
	}
}

// RegisterResource registers a resource function.
func (r *Registry) RegisterResource(name string, fn interface{}) {
	val := reflect.ValueOf(fn)
	typ := val.Type()

	if typ.Kind() != reflect.Func {
		panic(fmt.Sprintf("resource %q must be a function", name))
	}

	if typ.NumIn() != 2 {
		panic(fmt.Sprintf("resource %q function must take exactly 2 arguments", name))
	}

	// Optional validation of argument types
	ctxType := reflect.TypeOf((*context.Context)(nil)).Elem()
	if !typ.In(0).Implements(ctxType) {
		panic(fmt.Sprintf("resource %q first argument must implement context.Context", name))
	}

	if typ.NumOut() != 2 {
		panic(fmt.Sprintf("resource %q function must return exactly 2 values", name))
	}

	errType := reflect.TypeOf((*error)(nil)).Elem()
	if !typ.Out(1).Implements(errType) {
		panic(fmt.Sprintf("resource %q second return value must implement error", name))
	}

	r.resources[name] = ResourceRegistration{
		Name:         name,
		Fn:           val,
		ConfigSchema: generateJSONSchema(typ.In(1)),
	}
}

// RegisterStep registers a step function.
func (r *Registry) RegisterStep(name string, fn interface{}, opts ...StepOption) {
	val := reflect.ValueOf(fn)
	typ := val.Type()

	if typ.Kind() != reflect.Func {
		panic(fmt.Sprintf("step %q must be a function", name))
	}

	reg := StepRegistration{
		Name:         name,
		Fn:           val,
		ConfigSchema: generateJSONSchema(typ.In(1)),
	}

	for _, opt := range opts {
		opt(&reg)
	}

	expectedArgs := 3
	if reg.ResourceName != "" {
		expectedArgs = 4
	}

	if typ.NumIn() != expectedArgs {
		panic(fmt.Sprintf("step %q function must take exactly %d arguments", name, expectedArgs))
	}

	ctxType := reflect.TypeOf((*context.Context)(nil)).Elem()
	if !typ.In(0).Implements(ctxType) {
		panic(fmt.Sprintf("step %q first argument must implement context.Context", name))
	}

	tableType := reflect.TypeOf((*core.Table)(nil))
	if typ.In(expectedArgs-1) != tableType {
		panic(fmt.Sprintf("step %q last argument must be *core.Table", name))
	}

	if typ.NumOut() != 2 {
		panic(fmt.Sprintf("step %q function must return exactly 2 values", name))
	}

	if typ.Out(0) != tableType {
		panic(fmt.Sprintf("step %q first return value must be *core.Table", name))
	}

	errType := reflect.TypeOf((*error)(nil)).Elem()
	if !typ.Out(1).Implements(errType) {
		panic(fmt.Sprintf("step %q second return value must implement error", name))
	}

	r.steps[name] = reg
}

// generateJSONSchema is a helper to extract a basic JSON schema from a type.
func generateJSONSchema(t reflect.Type) string {
	if t.Kind() == reflect.Ptr {
		t = t.Elem()
	}
	if t.Kind() != reflect.Struct {
		return fmt.Sprintf(`{"type": "%s"}`, t.Kind().String())
	}

	schema := `{"type": "object", "properties": {`
	for i := 0; i < t.NumField(); i++ {
		field := t.Field(i)
		if i > 0 {
			schema += ", "
		}
		schema += fmt.Sprintf(`"%s": {"type": "%s"}`, field.Name, field.Type.Kind().String())
	}
	schema += `}}`
	return schema
}

// GetResource returns a registered resource function.
func (r *Registry) GetResource(name string) (ResourceRegistration, bool) {
	reg, ok := r.resources[name]
	return reg, ok
}

// GetStep returns a registered step function.
func (r *Registry) GetStep(name string) (StepRegistration, bool) {
	reg, ok := r.steps[name]
	return reg, ok
}

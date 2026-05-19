package plugin

import (
	"context"
	"io"
)

type contextKey string

const (
	OutputWriterKey contextKey = "heddle-step-output-writer"
)

// GetOutputWriter retrieves the step output writer from the context,
// or returns nil if it is not set.
func GetOutputWriter(ctx context.Context) io.Writer {
	if ctx == nil {
		return nil
	}
	if w, ok := ctx.Value(OutputWriterKey).(io.Writer); ok {
		return w
	}
	return nil
}

// WithOutputWriter returns a new context containing the provided output writer.
func WithOutputWriter(ctx context.Context, w io.Writer) context.Context {
	return context.WithValue(ctx, OutputWriterKey, w)
}

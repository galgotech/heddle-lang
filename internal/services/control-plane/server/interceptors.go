package server

import (
	"context"
	"strings"

	"google.golang.org/grpc"
	"google.golang.org/grpc/metadata"

	"github.com/galgotech/heddle-lang/internal/services/control-plane/state"
)

const (
	// HeaderWorkerID identifies the unique identifier of the worker instance initiating the request.
	HeaderWorkerID = "x-heddle-worker-id"
	// HeaderToken carries the bearer token used for authenticating the worker or client against the control plane.
	HeaderToken = "x-heddle-token"
)

// UnaryWorkerInterceptor intercepts unary gRPC calls to enrich the request context with Heddle-specific
// identity and metadata extracted from the incoming transport headers.
func UnaryWorkerInterceptor(ctx context.Context, req any, info *grpc.UnaryServerInfo, handler grpc.UnaryHandler) (any, error) {
	newCtx := contextWithHeddleMetadata(ctx)
	return handler(newCtx, req)
}

// StreamWorkerInterceptor provides context enrichment for streaming gRPC calls (e.g., Flight DoExchange).
// It wraps the original stream to override the Context() method, returning a HeddleContext.
func StreamWorkerInterceptor(srv any, ss grpc.ServerStream, info *grpc.StreamServerInfo, handler grpc.StreamHandler) error {
	newCtx := contextWithHeddleMetadata(ss.Context())
	wrapped := &wrappedStream{ServerStream: ss, ctx: newCtx}
	return handler(srv, wrapped)
}

// contextWithHeddleMetadata extracts gRPC metadata and transforms it into a strictly-typed state.HeddleContext.
// It maps the "x-heddle-" header namespace into the Metadata store, excluding reserved keys used for credentials.
func contextWithHeddleMetadata(ctx context.Context) context.Context {
	md, ok := metadata.FromIncomingContext(ctx)
	if !ok {
		return ctx
	}

	creds := state.Credentials{}
	meta := state.Metadata{Values: make(map[string]any)}

	// Populate credentials from the reserved auth header.
	if tokens := md.Get(HeaderToken); len(tokens) > 0 {
		creds.Token = tokens[0]
	}

	// Map the worker identifier into the metadata store for downstream locality and state tracking.
	if ids := md.Get(HeaderWorkerID); len(ids) > 0 {
		meta.Values["worker_id"] = ids[0]
	}

	// Sanitize and map all other x-heddle prefixed headers into the generic metadata map.
	// This allows for dynamic extension of the protocol without breaking the interceptor logic.
	for k, v := range md {
		if strings.HasPrefix(k, "x-heddle-") && len(v) > 0 {
			key := strings.TrimPrefix(k, "x-heddle-")
			// Avoid duplicating data already handled in specialized fields.
			if key != "token" && key != "worker-id" {
				meta.Values[key] = v[0]
			}
		}
	}

	// Initialize the HeddleContext which acts as the primary carrier for execution-scoped state.
	hCtx := state.NewHeddleContext(ctx, creds, state.Lineage{}, meta)
	return hCtx
}

// wrappedStream is a decorator for grpc.ServerStream that allows injecting a modified context.
type wrappedStream struct {
	grpc.ServerStream
	ctx context.Context
}

// Context returns the enriched HeddleContext instead of the original gRPC stream context.
func (w *wrappedStream) Context() context.Context {
	return w.ctx
}

// GetWorkerID performs a type-safe extraction of the worker identifier from the context.
// It returns an empty string if the context is not a HeddleContext or if the ID is missing.
func GetWorkerID(ctx context.Context) string {
	if hCtx, ok := ctx.(*state.HeddleContext); ok {
		if id, ok := hCtx.Metadata.Values["worker_id"].(string); ok {
			return id
		}
	}
	return ""
}

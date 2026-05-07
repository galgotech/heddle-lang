package server

import (
	"context"
	"strings"

	"google.golang.org/grpc"
	"google.golang.org/grpc/metadata"

	"github.com/galgotech/heddle-lang/internal/services/control-plane/state"
)

const (
	// HeaderWorkerID defines the gRPC metadata key for identifying the originating worker.
	HeaderWorkerID = "x-heddle-worker-id"
	// HeaderToken defines the gRPC metadata key for authorization.
	HeaderToken = "x-heddle-token"
)

// UnaryWorkerInterceptor extracts Heddle-specific metadata from incoming gRPC headers
// and injects it into a HeddleContext for use in unary handlers.
func UnaryWorkerInterceptor(ctx context.Context, req interface{}, info *grpc.UnaryServerInfo, handler grpc.UnaryHandler) (interface{}, error) {
	newCtx := contextWithHeddleMetadata(ctx)
	return handler(newCtx, req)
}

// StreamWorkerInterceptor provides metadata extraction for bidirectional and unidirectional
// Flight streams, wrapping the server stream with a context-enriched HeddleContext.
func StreamWorkerInterceptor(srv interface{}, ss grpc.ServerStream, info *grpc.StreamServerInfo, handler grpc.StreamHandler) error {
	newCtx := contextWithHeddleMetadata(ss.Context())
	wrapped := &wrappedStream{ServerStream: ss, ctx: newCtx}
	return handler(srv, wrapped)
}

// contextWithHeddleMetadata parses gRPC metadata to initialize a HeddleContext.
// It maps standard and custom x-heddle-* headers to the context's credentials and metadata stores.
func contextWithHeddleMetadata(ctx context.Context) context.Context {
	md, ok := metadata.FromIncomingContext(ctx)
	if !ok {
		return ctx
	}

	creds := state.Credentials{}
	meta := state.Metadata{Values: make(map[string]interface{})}

	// Extract primary auth token.
	if tokens := md.Get(HeaderToken); len(tokens) > 0 {
		creds.Token = tokens[0]
	}

	// Extract authoritative worker identifier.
	if ids := md.Get(HeaderWorkerID); len(ids) > 0 {
		meta.Values["worker_id"] = ids[0]
	}

	// Iterate through all custom x-heddle- headers and populate the metadata map.
	for k, v := range md {
		if strings.HasPrefix(k, "x-heddle-") && len(v) > 0 {
			key := strings.TrimPrefix(k, "x-heddle-")
			if key != "token" && key != "worker-id" {
				meta.Values[key] = v[0]
			}
		}
	}

	// Construct and return the strictly-typed HeddleContext.
	hCtx := state.NewHeddleContext(ctx, creds, state.Lineage{}, meta)
	return hCtx
}

type wrappedStream struct {
	grpc.ServerStream
	ctx context.Context
}

func (w *wrappedStream) Context() context.Context {
	return w.ctx
}

// GetWorkerID safely retrieves the worker identifier from a HeddleContext.
// Returns an empty string if the context is not a HeddleContext or the ID is missing.
func GetWorkerID(ctx context.Context) string {
	if hCtx, ok := ctx.(*state.HeddleContext); ok {
		if id, ok := hCtx.Metadata.Values["worker_id"].(string); ok {
			return id
		}
	}
	return ""
}

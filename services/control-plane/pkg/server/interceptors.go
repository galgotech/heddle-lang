package server

import (
	"context"
	"strings"

	"google.golang.org/grpc"
	"google.golang.org/grpc/metadata"

	"github.com/galgotech/heddle-lang/services/control-plane/pkg/state"
)

const (
	HeaderWorkerID = "x-heddle-worker-id"
	HeaderToken    = "x-heddle-token"
)

// UnaryWorkerInterceptor extracts worker metadata from gRPC headers and populates HeddleContext.
func UnaryWorkerInterceptor(ctx context.Context, req interface{}, info *grpc.UnaryServerInfo, handler grpc.UnaryHandler) (interface{}, error) {
	newCtx := contextWithHeddleMetadata(ctx)
	return handler(newCtx, req)
}

// StreamWorkerInterceptor extracts worker metadata from gRPC headers for streaming calls.
func StreamWorkerInterceptor(srv interface{}, ss grpc.ServerStream, info *grpc.StreamServerInfo, handler grpc.StreamHandler) error {
	newCtx := contextWithHeddleMetadata(ss.Context())
	wrapped := &wrappedStream{ServerStream: ss, ctx: newCtx}
	return handler(srv, wrapped)
}

func contextWithHeddleMetadata(ctx context.Context) context.Context {
	md, ok := metadata.FromIncomingContext(ctx)
	if !ok {
		return ctx
	}

	creds := state.Credentials{}
	meta := state.Metadata{Values: make(map[string]interface{})}

	if tokens := md.Get(HeaderToken); len(tokens) > 0 {
		creds.Token = tokens[0]
	}

	if ids := md.Get(HeaderWorkerID); len(ids) > 0 {
		meta.Values["worker_id"] = ids[0]
	}

	// Also look for other x-heddle-* headers
	for k, v := range md {
		if strings.HasPrefix(k, "x-heddle-") && len(v) > 0 {
			key := strings.TrimPrefix(k, "x-heddle-")
			if key != "token" && key != "worker-id" {
				meta.Values[key] = v[0]
			}
		}
	}

	// Create HeddleContext
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

// GetWorkerID retrieves the worker ID from the context.
func GetWorkerID(ctx context.Context) string {
	if hCtx, ok := ctx.(*state.HeddleContext); ok {
		if id, ok := hCtx.Metadata.Values["worker_id"].(string); ok {
			return id
		}
	}
	return ""
}

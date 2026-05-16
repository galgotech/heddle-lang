package lsp

import (
	"context"
	"encoding/json"

	"go.lsp.dev/jsonrpc2"
	"go.lsp.dev/protocol"
	"go.uber.org/zap"
)

func HandleSetTrace(ctx context.Context, req jsonrpc2.Request, logger *zap.Logger) (protocol.TraceValue, error) {
	var params protocol.SetTraceParams
	if err := json.Unmarshal(req.Params(), &params); err == nil {
		logger.Info("Trace level set", zap.String("level", string(params.Value)))
		return params.Value, nil
	}
	return "", nil
}

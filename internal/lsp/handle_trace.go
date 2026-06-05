package lsp

import (
	"context"
	"encoding/json"

	"go.lsp.dev/jsonrpc2"
	"go.lsp.dev/protocol"

	"github.com/galgotech/heddle-lang/pkg/logger"
)

func HandleSetTrace(ctx context.Context, req jsonrpc2.Request, log logger.Logger) (protocol.TraceValue, error) {
	var params protocol.SetTraceParams
	if err := json.Unmarshal(req.Params(), &params); err == nil {
		log.Info("Trace level set", logger.String("level", string(params.Value)))
		return params.Value, nil
	}
	return "", nil
}

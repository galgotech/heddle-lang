package main

import (
	"go.uber.org/zap"

	"github.com/galgotech/heddle-lang/pkg/logger"
	"github.com/galgotech/heddle-lang/sdk/go/plugin"
)

func main() {
	namespace := "std"
	p := plugin.New(namespace)

	logger.L().Info("Go Plugin [%s] starting...", zap.String("namespace", namespace))
	if err := p.Start(); err != nil {
		logger.L().Fatal("plugin server failed", zap.Error(err))
	}
}

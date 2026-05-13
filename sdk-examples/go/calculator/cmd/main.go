package main

import (
	"go.uber.org/zap"

	"github.com/galgotech/heddle-lang/pkg/logger"
	"github.com/galgotech/heddle-lang/sdk/go/plugin"

	"github.com/galgotech/heddle-lang/sdk-examples/go/calculator/step"
)

func main() {
	p := plugin.New("calculator")

	p.RegisterStep("add", step.Add)
	p.RegisterStep("subtract", step.Subtract)
	p.RegisterStep("multiply", step.Multiply)
	p.RegisterStep("divide", step.Divide)

	logger.L().Info("Calculator plugin starting...")
	if err := p.Start(); err != nil {
		logger.L().Fatal("Plugin failed", zap.Error(err))
	}
}

package stdplugin

import (
	"go.uber.org/zap"

	"github.com/galgotech/heddle-lang/pkg/logger"
	"github.com/galgotech/heddle-lang/sdk/go/plugin"
	"github.com/galgotech/heddle-lang/sdk/go/std"
)

func Register() <-chan struct{} {
	ready := make(chan struct{})

	// Then, start the "std/io" plugin
	pIo := plugin.New("std/io")
	err := pIo.RegisterStep("print", std.PrintStep)
	if err != nil {
		logger.L().Fatal("Standard library plugin (io) failed to register step print: %v", zap.Error(err))
	}

	go func() {
		if err := pIo.Start(); err != nil {
			logger.L().Info("Standard library plugin (io) failed: %v", zap.Error(err))
		}
	}()

	go func() {
		<-pIo.Ready
		close(ready)
	}()

	return ready
}

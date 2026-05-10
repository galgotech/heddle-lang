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
	pIo.RegisterStep("print", std.PrintStep)

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

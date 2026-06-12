package local

import (
	"context"
	"fmt"
	"os"
	"os/signal"
	"strings"
	"syscall"
	"time"

	"github.com/spf13/cobra"

	"github.com/galgotech/heddle-lang/internal/controlplane"
	"github.com/galgotech/heddle-lang/internal/controlplane/registry"
	"github.com/galgotech/heddle-lang/internal/worker"
	"github.com/galgotech/heddle-lang/pkg/logger"
	"github.com/galgotech/heddle-lang/pkg/runtime"
	"github.com/galgotech/heddle-lang/pkg/runtime/locality"
	"github.com/galgotech/heddle-lang/pkg/transport"
)

// LocalCmd is the command to start the engine locally.
var LocalCmd = &cobra.Command{
	Use:   "local",
	Short: "Starts Control Plane and Worker",
	Run: func(cmd *cobra.Command, args []string) {
		logger.L().Info("startup: starting Heddle services in local mode", logger.Component("local"))
		defer logger.Sync()

		// Set up signal handling for graceful shutdown
		logger.L().Debug("startup: registering signal handlers for graceful shutdown", logger.Component("local"))
		ctx, stop := signal.NotifyContext(cmd.Context(), os.Interrupt, syscall.SIGTERM)
		defer stop()

		if err := startLocalServices(ctx); err != nil {
			logger.L().Error("startup: failed to start local services", logger.Component("local"), logger.Error(err))
			return
		}

		logger.L().Info("startup: heddle services are running, press Ctrl+C to stop", logger.Component("local"))
		fmt.Println("Heddle services are running. Press Ctrl+C to stop...")

		<-ctx.Done()
		logger.L().Info("shutdown: context cancelled, initiating graceful shutdown", logger.Component("local"))
	},
}

func startLocalServices(ctx context.Context) error {
	workerSocket := runtime.WorkerUDSPath
	logger.L().Debug("startup: resolving worker socket path", logger.Component("local"), logger.String("socket_path", workerSocket))

	// 1. Start Control Plane
	logger.L().Debug("startup: starting node registry and registry sweeper", logger.Component("local"))
	workerRegistry := registry.NewNodeRegistry()
	workerRegistry.StartSweeper(ctx, 5*time.Second, 15*time.Second)

	logger.L().Debug("startup: creating control plane server", logger.Component("local"))
	cp := controlplane.NewControlPlaneServer(workerRegistry)

	logger.L().Debug("startup: establishing in-memory transport", logger.Component("local"))
	inMemory := transport.NewInMemory(cp)

	// 2. Start Worker
	// Warn if the worker socket file already exists
	socketFilePath := workerSocket
	if after, ok := strings.CutPrefix(socketFilePath, "unix://"); ok {
		socketFilePath = after
	}
	if _, err := os.Stat(socketFilePath); err == nil {
		logger.L().Warn("startup: worker socket file already exists, it will be cleaned up on worker start",
			logger.Component("local"),
			logger.String("socket_path", socketFilePath),
		)
	}

	logger.L().Debug("startup: initializing data locality registry and worker plugins", logger.Component("local"))
	registry := locality.NewDataLocalityRegistry()
	nativePlugins := worker.NewNativePlugins()
	pluginServer := worker.NewPluginServer(registry, nativePlugins, workerSocket)

	logger.L().Debug("startup: creating worker daemon", logger.Component("local"))
	worker, err := worker.NewWorker(inMemory, pluginServer)
	if err != nil {
		logger.L().Error("startup: failed to create worker node", logger.Component("local"), logger.Error(err))
		return fmt.Errorf("failed to create worker: %w", err)
	}

	logger.L().Debug("startup: starting worker daemon", logger.Component("local"))
	if err := worker.Start(ctx); err != nil {
		logger.L().Error("startup: failed to start worker node", logger.Component("local"), logger.Error(err))
		return fmt.Errorf("failed to start worker: %w", err)
	}

	logger.L().Info("startup: control plane and worker initialized successfully", logger.Component("local"))

	logger.L().Debug("startup: starting local in-memory transport", logger.Component("local"))
	go func() {
		if err := inMemory.Start(ctx); err != nil {
			logger.L().Error("startup: failed to start local in-memory transport", logger.Component("local"), logger.Error(err))
		}
	}()

	return nil
}

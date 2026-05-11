package local

import (
	"context"
	"os"
	"strings"
	"time"

	"github.com/spf13/cobra"
	"go.uber.org/zap"

	"github.com/galgotech/heddle-lang/internal/services/client"
	controlplane "github.com/galgotech/heddle-lang/internal/services/control-plane"
	"github.com/galgotech/heddle-lang/internal/services/worker"
	"github.com/galgotech/heddle-lang/pkg/logger"
	"github.com/galgotech/heddle-lang/sdk/go/stdplugin"
)

// LocalCmd is the root command for running Heddle in local standalone mode.
var LocalCmd = &cobra.Command{
	Use:   "local [file.he]",
	Short: "Run Heddle in local standalone mode",
	Long: `In local mode, Heddle starts an internal Control Plane and Worker using Unix Domain Sockets.
If a file is provided, it will be executed immediately.`,
	Args: cobra.MaximumNArgs(1),
	Run:  runStandalone,
}

func runStandalone(cmd *cobra.Command, args []string) {
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

	cpSocket := "unix:///tmp/heddle-cp.sock"
	workerSocket := "/tmp/heddle-worker.sock"

	// 1. Start Control Plane
	cp := controlplane.NewControlPlaneServer()
	go func() {
		if err := cp.Listen(cpSocket); err != nil {
			logger.L().Fatal("Control Plane failed", zap.Error(err))
		}
	}()
	<-cp.Ready

	// 2. Start Worker
	w, err := worker.NewWorker(cpSocket, workerSocket)
	if err != nil {
		logger.L().Fatal("Failed to create worker", zap.Error(err))
	}
	go func() {
		if err := w.Start(ctx); err != nil {
			logger.L().Fatal("Worker failed", zap.Error(err))
		}
	}()
	<-w.Ready

	// 3. Start Standard Library Plugins (std and std/io)
	<-stdplugin.Register()

	// wait a little for plugins to register and update capabilities
	time.Sleep(1 * time.Second)

	// 4. Submit file if provided
	if len(args) > 0 {
		filePath := args[0]
		content, err := os.ReadFile(filePath)
		if err != nil {
			logger.L().Fatal("Failed to read file", zap.String("file", filePath), zap.Error(err))
		}

		c, err := client.NewControlPlaneClient(cpSocket)
		if err != nil {
			logger.L().Fatal("Failed to create client", zap.Error(err))
		}

		res, err := c.SubmitWorkflow(ctx, string(content))
		if err != nil {
			logger.L().Fatal("Submission failed", zap.Error(err))
		}

		logger.L().Info("Workflow submitted", zap.String("result", res))

		taskID := strings.TrimPrefix(res, "QUEUED: ")

		// Wait for execution
		select {
		case err := <-cp.WaitForWorkflow(taskID):
			if err != nil {
				logger.L().Fatal("Workflow failed", zap.Error(err))
			}
			logger.L().Info("Workflow completed successfully (including SHM purge)")
		case <-ctx.Done():
			logger.L().Info("Shutdown requested")
		}
	} else {
		logger.L().Info("Heddle is running in local mode. Press Ctrl+C to exit.")
		select {}
	}
}

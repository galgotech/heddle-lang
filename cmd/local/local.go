package local

import (
	"context"
	"os"
	"time"

	"github.com/spf13/cobra"
	"go.uber.org/zap"

	"github.com/galgotech/heddle-lang/internal/services/client"
	controlplane "github.com/galgotech/heddle-lang/internal/services/control-plane"
	"github.com/galgotech/heddle-lang/internal/services/worker"
	"github.com/galgotech/heddle-lang/pkg/logger"
	sdk "github.com/galgotech/heddle-lang/sdk/go/plugin"
	std "github.com/galgotech/heddle-lang/sdk/go/std"
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

	// 2. Start Worker
	w, err := worker.NewWorker(cpSocket)
	if err != nil {
		logger.L().Fatal("Failed to create worker", zap.Error(err))
	}
	w.SocketPath = workerSocket
	go func() {
		if err := w.Start(ctx); err != nil {
			logger.L().Fatal("Worker failed", zap.Error(err))
		}
	}()

	// 3. Start Standard Library Plugin (std:io)
	go func() {
		p := sdk.New("std/io")
		p.RegisterStep("print", std.PrintStep)
		// Give some time for worker to start
		time.Sleep(500 * time.Millisecond)
		if err := p.Start(); err != nil {
			logger.L().Info("Standard library plugin failed: %v", zap.Error(err))
		}
	}()

	// Give some time for everything to connect
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

		// Wait for execution (simulated for now since we don't have a wait endpoint)
		time.Sleep(2 * time.Second)
	} else {
		logger.L().Info("Heddle is running in local mode. Press Ctrl+C to exit.")
		select {}
	}
}

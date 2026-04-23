package main

import (
	"context"
	"fmt"
	"os"
	"os/signal"
	"syscall"

	"github.com/spf13/cobra"
	"go.uber.org/zap"

	"github.com/galgotech/heddle-lang/pkg/execution"
	"github.com/galgotech/heddle-lang/pkg/logger"
	_ "github.com/galgotech/heddle-lang/pkg/stdlib/io"
)

var (
	workerID string
	cpAddr   string
)

var rootCmd = &cobra.Command{
	Use:   "heddle-worker",
	Short: "Heddle Worker executes tasks assigned by the control plane",
	Run: func(cmd *cobra.Command, args []string) {
		// Initialize logger
		if err := logger.Init(logger.Config{Development: true}); err != nil {
			panic(err)
		}
		defer logger.Sync()

		worker, err := execution.NewWorker(workerID, cpAddr)
		if err != nil {
			logger.L().Fatal("Failed to create worker", zap.Error(err))
		}

		ctx, cancel := context.WithCancel(context.Background())
		defer cancel()

		// Handle signals for graceful shutdown
		sigChan := make(chan os.Signal, 1)
		signal.Notify(sigChan, syscall.SIGINT, syscall.SIGTERM)
		go func() {
			<-sigChan
			cancel()
		}()

		if err := worker.Register(ctx); err != nil {
			logger.L().Fatal("Failed to register worker", zap.Error(err))
		}

		go worker.StartHeartbeat(ctx)
		go worker.StartExecutionLoop(ctx)

		logger.L().Info("Worker is running", zap.String("workerID", workerID))
		<-ctx.Done()
		logger.L().Info("Worker shutting down", zap.String("workerID", workerID))
	},
}

func init() {
	rootCmd.Flags().StringVar(&workerID, "id", "worker-1", "Unique ID for this worker")
	rootCmd.Flags().StringVar(&cpAddr, "cp", "localhost:50051", "Address of the control plane")
}

func main() {
	if err := rootCmd.Execute(); err != nil {
		fmt.Fprintln(os.Stderr, err)
		os.Exit(1)
	}
}

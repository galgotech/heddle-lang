package main

import (
	"context"
	"os"
	"os/signal"
	"syscall"
	"time"

	"github.com/spf13/cobra"
	"github.com/spf13/viper"
	"go.uber.org/zap"
	"google.golang.org/grpc"
	"google.golang.org/grpc/credentials/insecure"

	"github.com/galgotech/heddle-lang/pkg/config"
	"github.com/galgotech/heddle-lang/pkg/logger"
	"github.com/galgotech/heddle-lang/pkg/runtime/data"
	"github.com/galgotech/heddle-lang/pkg/runtime/execution"
	"github.com/galgotech/heddle-lang/pkg/runtime/transport"
)

var workerCfgFile string

var workerCmd = &cobra.Command{
	Use:   "worker",
	Short: "Start a Heddle Worker",
	PersistentPreRunE: func(cmd *cobra.Command, args []string) error {
		return config.Init("HEDDLE_WORKER", workerCfgFile)
	},
	Run: func(cmd *cobra.Command, args []string) {
		if err := logger.Init(logger.Config{Development: true}); err != nil {
			panic(err)
		}
		defer logger.Sync()

		workerID := viper.GetString("id")
		cpAddr := viper.GetString("cp")
		batchSize := viper.GetInt("batch-size")
		batchWindow := viper.GetDuration("batch-window")

		conn, err := grpc.NewClient(cpAddr, grpc.WithTransportCredentials(insecure.NewCredentials()))
		if err != nil {
			logger.L().Fatal("failed to connect to CP", zap.Error(err))
		}

		trans := transport.NewFlightTransport(conn)
		alloc := data.NewOSMemoryAllocator("/dev/shm/heddle")
		dataMgr := data.NewLocalMmapManager(alloc, 1<<30)

		worker := execution.NewWorker(workerID, trans, dataMgr, batchSize, batchWindow)

		ctx, cancel := context.WithCancel(context.Background())
		defer cancel()

		sigChan := make(chan os.Signal, 1)
		signal.Notify(sigChan, syscall.SIGINT, syscall.SIGTERM)
		go func() {
			<-sigChan
			logger.L().Info("Termination signal received, initiating shutdown...")
			cancel()
		}()

		if err := worker.Register(ctx); err != nil {
			logger.L().Fatal("Failed to register worker", zap.Error(err))
		}

		go worker.StartHeartbeat(ctx)
		go worker.StartExecutionLoop(ctx)

		logger.L().Info("Worker is operational and awaiting tasks", zap.String("workerID", workerID))

		<-ctx.Done()
		logger.L().Info("Worker shutdown complete", zap.String("workerID", workerID))
	},
}

func init() {
	workerCmd.Flags().StringVar(&workerCfgFile, "config", "", "config file (default is ./heddle-worker.yaml)")
	workerCmd.Flags().String("id", "worker-1", "Unique identifier for this worker instance")
	workerCmd.Flags().String("cp", "localhost:50051", "gRPC address of the Heddle Control Plane")
	workerCmd.Flags().Int("batch-size", 10, "Maximum number of tasks to aggregate per batch")
	workerCmd.Flags().Duration("batch-window", 50*time.Millisecond, "Time window to wait for batch convergence")

	viper.BindPFlag("id", workerCmd.Flags().Lookup("id"))
	viper.BindPFlag("cp", workerCmd.Flags().Lookup("cp"))
	viper.BindPFlag("batch-size", workerCmd.Flags().Lookup("batch-size"))
	viper.BindPFlag("batch-window", workerCmd.Flags().Lookup("batch-window"))
}

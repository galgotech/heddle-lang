package cluster

import (
	"context"
	"time"

	"github.com/spf13/cobra"
	"github.com/spf13/viper"
	"go.uber.org/zap"

	"github.com/galgotech/heddle-lang/internal/services/worker"
	"github.com/galgotech/heddle-lang/pkg/config"
	"github.com/galgotech/heddle-lang/pkg/logger"
)

var workerCfgFile string

var workerCmd = &cobra.Command{
	Use:   "worker",
	Short: "Start a Heddle Worker",
	PersistentPreRunE: func(cmd *cobra.Command, args []string) error {
		return config.Init("HEDDLE_WORKER", workerCfgFile)
	},
	Run: func(cmd *cobra.Command, args []string) {
		ctx := context.Background()
		cpAddr := viper.GetString("cp")
		socket := viper.GetString("socket")
		id := viper.GetString("id")

		w, err := worker.NewWorker(cpAddr, socket)
		if err != nil {
			logger.L().Fatal("Failed to initialize worker", zap.Error(err))
		}

		w.ID = id
		logger.L().Info("Starting worker", zap.String("id", id), zap.String("cp", cpAddr))
		if err := w.Start(ctx); err != nil {
			logger.L().Fatal("Worker exited with error", zap.Error(err))
		}
	},
}

func init() {
	workerCmd.Flags().StringVar(&workerCfgFile, "config", "", "config file (default is ./heddle-worker.yaml)")
	workerCmd.Flags().String("id", "worker-1", "Unique identifier for this worker instance")
	workerCmd.Flags().String("cp", "localhost:50051", "gRPC address of the Heddle Control Plane")
	workerCmd.Flags().String("socket", "/tmp/heddle-worker.sock", "Path to the Unix Domain Socket for plugins")
	workerCmd.Flags().Int("batch-size", 10, "Maximum number of tasks to aggregate per batch")
	workerCmd.Flags().Duration("batch-window", 50*time.Millisecond, "Time window to wait for batch convergence")

	viper.BindPFlag("id", workerCmd.Flags().Lookup("id"))
	viper.BindPFlag("cp", workerCmd.Flags().Lookup("cp"))
	viper.BindPFlag("socket", workerCmd.Flags().Lookup("socket"))
	viper.BindPFlag("batch-size", workerCmd.Flags().Lookup("batch-size"))
	viper.BindPFlag("batch-window", workerCmd.Flags().Lookup("batch-window"))
}

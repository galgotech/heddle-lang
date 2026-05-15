package cluster

import (
	"context"
	"fmt"
	"time"

	"github.com/spf13/cobra"
	"github.com/spf13/viper"
	"go.uber.org/zap"

	"github.com/galgotech/heddle-lang/internal/worker"
	"github.com/galgotech/heddle-lang/pkg/config"
	"github.com/galgotech/heddle-lang/pkg/logger"
)

var workerGroupCmd = &cobra.Command{
	Use:   "worker",
	Short: "Worker Management in the cluster",
}

var workerRunCmd = &cobra.Command{
	Use:   "run",
	Short: "Starts the Worker binary, connecting it to a Control Plane",
	PersistentPreRunE: func(cmd *cobra.Command, args []string) error {
		cfgFile, _ := cmd.Flags().GetString("config")
		return config.Init("HEDDLE_WORKER", cfgFile)
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

var workerLogsCmd = &cobra.Command{
	Use:   "logs",
	Short: "Streams the logs Worker Logs",
	Run: func(cmd *cobra.Command, args []string) {
		fmt.Println("Streaming Worker logs (Mock)...")
		for i := 0; i < 5; i++ {
			fmt.Printf("2024-05-11 17:43:%02d [DEBUG] worker-local processing task task-%d\n", i*3, i+100)
			time.Sleep(500 * time.Millisecond)
		}
		fmt.Println("... use Ctrl+C to stop streaming")
	},
}

var workerHealthCmd = &cobra.Command{
	Use:   "health",
	Short: "Checks the health and current load of the Workers",
	Run: func(cmd *cobra.Command, args []string) {
		fmt.Println("Mock: Worker Health Check")
		fmt.Println("Status: ONLINE")
		fmt.Println("Load: 12% (1/8 slots used)")
		fmt.Println("Uptime: 14h 55m")
	},
}

func init() {
	workerRunCmd.Flags().String("config", "", "config file (default is ./heddle-worker.yaml)")
	workerRunCmd.Flags().String("id", "worker-1", "Unique identifier for this worker instance")
	workerRunCmd.Flags().String("cp", "localhost:50051", "gRPC address of the Heddle Control Plane")
	workerRunCmd.Flags().String("socket", "/tmp/heddle-worker.sock", "Path to the Unix Domain Socket for plugins")
	workerRunCmd.Flags().Int("batch-size", 10, "Maximum number of tasks to aggregate per batch")
	workerRunCmd.Flags().Duration("batch-window", 50*time.Millisecond, "Time window to wait for batch convergence")

	viper.BindPFlag("id", workerRunCmd.Flags().Lookup("id"))
	viper.BindPFlag("cp", workerRunCmd.Flags().Lookup("cp"))
	viper.BindPFlag("socket", workerRunCmd.Flags().Lookup("socket"))
	viper.BindPFlag("batch-size", workerRunCmd.Flags().Lookup("batch-size"))
	viper.BindPFlag("batch-window", workerRunCmd.Flags().Lookup("batch-window"))

	workerGroupCmd.AddCommand(workerRunCmd)
	workerGroupCmd.AddCommand(workerLogsCmd)
	workerGroupCmd.AddCommand(workerHealthCmd)
}

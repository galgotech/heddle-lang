package main

import (
	"context"
	"fmt"
	"os"
	"os/signal"
	"syscall"

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


var (
	// cfgFile holds the path to the configuration file provided via CLI flags.
	cfgFile string
)

// rootCmd defines the primary entry point for the Heddle Worker.
// It orchestrates the worker's lifecycle, including configuration loading,
// logger initialization, and the startup of core execution routines.
var rootCmd = &cobra.Command{
	Use:   "heddle-worker",
	Short: "Heddle Worker executes tasks assigned by the control plane",
	PersistentPreRunE: func(cmd *cobra.Command, args []string) error {
		// Bootstrap configuration before executing the main logic.
		return initializeConfig()
	},
	Run: func(cmd *cobra.Command, args []string) {
		// Initialize the global logger with a development-friendly configuration.
		if err := logger.Init(logger.Config{Development: true}); err != nil {
			panic(fmt.Errorf("failed to initialize logger: %w", err))
		}
		defer logger.Sync()

		// Retrieve worker identity and control plane coordinates from the configuration registry.
		workerID := viper.GetString("id")
		cpAddr := viper.GetString("cp")

		// Establish a gRPC connection to the Control Plane.
		conn, err := grpc.NewClient(cpAddr, grpc.WithTransportCredentials(insecure.NewCredentials()))
		if err != nil {
			logger.L().Fatal("failed to connect to CP", zap.Error(err))
		}
		
		// Initialize the network transport abstraction.
		trans := transport.NewFlightTransport(conn)

		// Configure the OS memory allocator for zero-copy mapping.
		alloc := data.NewOSMemoryAllocator("/dev/shm/heddle")
		dataMgr := data.NewLocalMmapManager(alloc, 1<<30) // 1GB limit

		// Initialize the execution worker (the "Muscle").
		worker := execution.NewWorker(workerID, trans, dataMgr)


		// Create a root context to manage the lifecycle of all background goroutines.
		ctx, cancel := context.WithCancel(context.Background())
		defer cancel()

		// Establish signal handlers for SIGINT and SIGTERM to ensure a graceful shutdown.
		// This allows the worker to notify the control plane before exiting, minimizing DAG disruption.
		sigChan := make(chan os.Signal, 1)
		signal.Notify(sigChan, syscall.SIGINT, syscall.SIGTERM)
		go func() {
			<-sigChan
			logger.L().Info("Termination signal received, initiating shutdown...")
			cancel()
		}()

		// Register the worker with the "Smart Control Plane" (the Brain).
		// This announces the worker's availability and capabilities to the scheduler.
		if err := worker.Register(ctx); err != nil {
			logger.L().Fatal("Failed to register worker", zap.Error(err))
		}

		// Start the heartbeat routine to maintain the worker's "Alive" state in the global registry.
		go worker.StartHeartbeat(ctx)

		// Launch the main execution loop to consume and process tasks dispatched by the control plane.
		go worker.StartExecutionLoop(ctx)

		logger.L().Info("Worker is operational and awaiting tasks", zap.String("workerID", workerID))

		// Block until the context is canceled by a signal or an unrecoverable error.
		<-ctx.Done()
		logger.L().Info("Worker shutdown complete", zap.String("workerID", workerID))
	},
}

// initializeConfig loads environment variables and configuration files into the global viper instance.
func initializeConfig() error {
	return config.Init("HEDDLE_WORKER", cfgFile)
}

func init() {
	// Define persistent flags available to all commands.
	rootCmd.PersistentFlags().StringVar(&cfgFile, "config", "", "config file (default is ./heddle-worker.yaml)")

	// Define local flags for identity and networking.
	rootCmd.Flags().String("id", "worker-1", "Unique identifier for this worker instance")
	rootCmd.Flags().String("cp", "localhost:50051", "gRPC address of the Heddle Control Plane")

	// Bind CLI flags to Viper keys for unified configuration access.
	viper.BindPFlag("id", rootCmd.Flags().Lookup("id"))
	viper.BindPFlag("cp", rootCmd.Flags().Lookup("cp"))
}

func main() {
	// Execute the root command and exit with a non-zero status on failure.
	if err := rootCmd.Execute(); err != nil {
		fmt.Fprintln(os.Stderr, err)
		os.Exit(1)
	}
}

package local

import (
	"context"
	"fmt"
	"os"

	"github.com/spf13/cobra"
	"go.uber.org/zap"

	controlplane "github.com/galgotech/heddle-lang/internal/services/control-plane"
	"github.com/galgotech/heddle-lang/internal/services/worker"
	"github.com/galgotech/heddle-lang/pkg/logger"
	"github.com/galgotech/heddle-lang/sdk/go/stdplugin"
)

const pidFile = "/tmp/heddle.pid"

// LocalCmd is the group command for local engine management.
var LocalCmd = &cobra.Command{
	Use:   "local",
	Short: "Engine lifecycle management on the LOCAL MACHINE",
}

var startCmd = &cobra.Command{
	Use:   "start",
	Short: "Starts the local Control Plane and Worker",
	Run: func(cmd *cobra.Command, args []string) {
		daemon, _ := cmd.Flags().GetBool("daemon")
		if daemon {
			fmt.Println("Simulating starting local processes in background...")
			if err := os.WriteFile(pidFile, []byte("12345"), 0644); err != nil {
				fmt.Printf("Error writing PID file: %v\n", err)
				return
			}
			fmt.Printf("Mock: Heddle daemon started. PID: 12345 (saved to %s)\n", pidFile)
			return
		}

		fmt.Println("Starting Heddle local services in foreground...")
		ctx, cancel := context.WithCancel(cmd.Context())
		defer cancel()
		if err := StartLocalServices(ctx); err != nil {
			logger.L().Error("Failed to start local services", zap.Error(err))
			return
		}
	},
}

var statusCmd = &cobra.Command{
	Use:   "status",
	Short: "Displays the current status and health of the daemon local",
	Run: func(cmd *cobra.Command, args []string) {
		if _, err := os.Stat(pidFile); err == nil {
			data, _ := os.ReadFile(pidFile)
			fmt.Printf("Mock: Healthy Control Plane and Worker\n")
			fmt.Printf("Status: RUNNING (PID: %s)\n", string(data))
			fmt.Println("Uptime: 2h 45m")
			fmt.Println("Endpoint: unix:///tmp/heddle-cp.sock")
		} else {
			fmt.Println("Status: STOPPED")
		}
	},
}

var stopCmd = &cobra.Command{
	Use:   "stop",
	Short: "Gracefully terminates local background processes",
	Run: func(cmd *cobra.Command, args []string) {
		if _, err := os.Stat(pidFile); err == nil {
			fmt.Println("Gracefully terminating local background processes...")
			os.Remove(pidFile)
			fmt.Println("Mock: Heddle services stopped.")
		} else {
			fmt.Println("No local processes are running in background.")
		}
	},
}

func init() {
	startCmd.Flags().Bool("daemon", false, "Runs background processes (detached from the terminal)")
	LocalCmd.AddCommand(startCmd)
	LocalCmd.AddCommand(statusCmd)
	LocalCmd.AddCommand(stopCmd)
}

func StartLocalServices(ctx context.Context) error {
	defer logger.Sync()

	cpSocket := "unix:///tmp/heddle-cp.sock"
	workerSocket := "/tmp/heddle-worker.sock"

	errCh := make(chan error, 2)

	// 1. Start Control Plane
	cp := controlplane.NewControlPlaneServer()
	go func() {
		defer logger.Sync()
		if err := cp.Listen(cpSocket); err != nil {
			errCh <- fmt.Errorf("control plane failed: %w", err)
		}
	}()

	select {
	case <-cp.Ready:
		// CP is ready
	case err := <-errCh:
		return err
	case <-ctx.Done():
		return ctx.Err()
	}

	// 2. Start Worker
	w, err := worker.NewWorker(cpSocket, workerSocket)
	if err != nil {
		return fmt.Errorf("failed to create worker: %w", err)
	}
	go func() {
		if err := w.Start(ctx); err != nil {
			errCh <- fmt.Errorf("worker failed: %w", err)
		}
	}()

	select {
	case <-w.Ready:
		// Worker is ready
	case err := <-errCh:
		return err
	case <-ctx.Done():
		return ctx.Err()
	}

	// 3. Start Standard Library Plugins (std and std/io)
	<-stdplugin.Register()

	logger.L().Info("Heddle is running in local mode.")
	return nil
}

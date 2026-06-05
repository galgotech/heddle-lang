package cluster

import (
	"fmt"
	"time"

	"github.com/spf13/cobra"
	"github.com/spf13/viper"

	"github.com/galgotech/heddle-lang/internal/config"
	controlplane "github.com/galgotech/heddle-lang/internal/control-plane"
	"github.com/galgotech/heddle-lang/internal/control-plane/registry"
	"github.com/galgotech/heddle-lang/pkg/logger"
)

var controlPlaneGroupCmd = &cobra.Command{
	Use:   "cp",
	Short: "Control Plane Management in the cluster",
}

var controlPlaneRunCmd = &cobra.Command{
	Use:   "run",
	Short: "Starts the Control Plane binary in cluster mode",
	PersistentPreRunE: func(cmd *cobra.Command, args []string) error {
		cfgFile, _ := cmd.Flags().GetString("config")
		return config.Init("HEDDLE_CP", cfgFile)
	},
	Run: func(cmd *cobra.Command, args []string) {
		defer logger.Sync()

		port := viper.GetInt("port")
		logger.L().Info("Heddle Control Plane starting",
			logger.Int("port", port),
			logger.String("standard", "Apache Arrow Flight"))

		workerRegistry := registry.NewWorkerRegistry()
		cp := controlplane.NewControlPlaneServer(workerRegistry)
		err := cp.Listen(fmt.Sprintf(":%d", port))
		if err != nil {
			logger.L().Fatal("failed to start control plane", logger.Error(err))
		}
	},
}

var controlPlaneLogsCmd = &cobra.Command{
	Use:   "logs",
	Short: "Streams Control Plane logs",
	Run: func(cmd *cobra.Command, args []string) {
		fmt.Println("Streaming Control Plane logs (Mock)...")
		for i := 0; i < 5; i++ {
			fmt.Printf("2024-05-11 17:42:%02d [INFO] Handled action: SubmitWorkflow from client-alpha\n", i*5)
			time.Sleep(500 * time.Millisecond)
		}
		fmt.Println("... use Ctrl+C to stop streaming")
	},
}

var controlPlaneHealthCmd = &cobra.Command{
	Use:   "health",
	Short: "Checks the health and uptime of the Control Plane",
	Run: func(cmd *cobra.Command, args []string) {
		fmt.Println("Mock: Control Plane Health Check")
		fmt.Println("Status: HEALTHY")
		fmt.Println("Uptime: 42h 12m")
		fmt.Println("Version: v0.4.2-beta")
	},
}

func init() {
	controlPlaneRunCmd.Flags().StringP("port", "p", "50051", "Port for the Control Plane")
	controlPlaneRunCmd.Flags().String("config", "", "config file (default is ./heddle-cp.yaml)")
	viper.BindPFlag("port", controlPlaneRunCmd.Flags().Lookup("port"))

	controlPlaneGroupCmd.AddCommand(controlPlaneRunCmd)
	controlPlaneGroupCmd.AddCommand(controlPlaneLogsCmd)
	controlPlaneGroupCmd.AddCommand(controlPlaneHealthCmd)
}

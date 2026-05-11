package cluster

import (
	"fmt"
	"time"

	"github.com/spf13/cobra"
	"github.com/spf13/viper"
	"go.uber.org/zap"

	controlplane "github.com/galgotech/heddle-lang/internal/services/control-plane"
	"github.com/galgotech/heddle-lang/pkg/config"
	"github.com/galgotech/heddle-lang/pkg/logger"
)

var cpGroupCmd = &cobra.Command{
	Use:   "cp",
	Short: "Control Plane Management in the cluster",
}

var cpRunCmd = &cobra.Command{
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
			zap.Int("port", port),
			zap.String("standard", "Apache Arrow Flight"))

		cp := controlplane.NewControlPlaneServer()
		err := cp.Listen(fmt.Sprintf(":%d", port))
		if err != nil {
			logger.L().Fatal("failed to start control plane", zap.Error(err))
		}
	},
}

var cpLogsCmd = &cobra.Command{
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

var cpHealthCmd = &cobra.Command{
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
	cpRunCmd.Flags().StringP("port", "p", "50051", "Port for the Control Plane")
	cpRunCmd.Flags().String("config", "", "config file (default is ./heddle-cp.yaml)")
	viper.BindPFlag("port", cpRunCmd.Flags().Lookup("port"))

	cpGroupCmd.AddCommand(cpRunCmd)
	cpGroupCmd.AddCommand(cpLogsCmd)
	cpGroupCmd.AddCommand(cpHealthCmd)
}

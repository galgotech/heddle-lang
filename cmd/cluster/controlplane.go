package cluster

import (
	"fmt"
	"time"

	"github.com/spf13/cobra"
	"github.com/spf13/viper"

	"github.com/galgotech/heddle-lang/internal/config"
	"github.com/galgotech/heddle-lang/internal/controlplane"
	"github.com/galgotech/heddle-lang/internal/controlplane/registry"
	"github.com/galgotech/heddle-lang/pkg/logger"
	"github.com/galgotech/heddle-lang/pkg/transport"
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

		workerRegistry := registry.NewNodeRegistry()
		workerRegistry.StartSweeper(cmd.Context(), 5*time.Second, 15*time.Second)
		cp := controlplane.NewControlPlaneServer(workerRegistry)

		flightTransport := transport.NewFlightServerTransport(fmt.Sprintf(":%d", port))
		flightTransport.SetServer(cp)

		err := flightTransport.Start()
		if err != nil {
			logger.L().Fatal("failed to start control plane", logger.Error(err))
		}
	},
}

func init() {
	controlPlaneRunCmd.Flags().StringP("port", "p", "50051", "Port for the Control Plane")
	controlPlaneRunCmd.Flags().String("config", "", "config file (default is ./heddle-cp.yaml)")
	viper.BindPFlag("port", controlPlaneRunCmd.Flags().Lookup("port"))

	controlPlaneGroupCmd.AddCommand(controlPlaneRunCmd)
}

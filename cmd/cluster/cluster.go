package cluster

import (
	"github.com/spf13/cobra"
	"github.com/spf13/viper"
)

// ClusterCmd is the root command for running Heddle in cluster mode.
var ClusterCmd = &cobra.Command{
	Use:   "cluster",
	Short: "Run Heddle components in cluster mode",
	Long: `In cluster mode, each component of Heddle (Control Plane, Worker) runs in its own process.
This is suitable for production environments or complex distributed testing.`,
}

func init() {
	ClusterCmd.PersistentFlags().IntP("port", "p", 50051, "Port for the Control Plane")
	viper.BindPFlag("port", ClusterCmd.PersistentFlags().Lookup("port"))

	ClusterCmd.AddCommand(cpCmd)
	ClusterCmd.AddCommand(workerCmd)
}

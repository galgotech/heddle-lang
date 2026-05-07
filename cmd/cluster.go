package main

import (
	"github.com/spf13/cobra"
	"github.com/spf13/viper"
)

var clusterCmd = &cobra.Command{
	Use:   "cluster",
	Short: "Run Heddle components in cluster mode",
	Long: `In cluster mode, each component of Heddle (Control Plane, Worker) runs in its own process.
This is suitable for production environments or complex distributed testing.`,
}

func init() {
	clusterCmd.PersistentFlags().IntP("port", "p", 50051, "Port for the Control Plane")
	viper.BindPFlag("port", clusterCmd.PersistentFlags().Lookup("port"))

	clusterCmd.AddCommand(cpCmd)
	clusterCmd.AddCommand(workerCmd)
	rootCmd.AddCommand(clusterCmd)
}

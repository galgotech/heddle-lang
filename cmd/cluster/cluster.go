package cluster

import (
	"github.com/spf13/cobra"
)

// ClusterCmd is the root command for running Heddle in cluster mode.
var ClusterCmd = &cobra.Command{
	Use:   "cluster",
	Short: "Remote Infrastructure Orchestration and Telemetry",
	Long: `Manage Heddle components in a cluster environment, 
including Control Plane and Worker lifecycle, logging, and health monitoring.`,
}

func init() {
	ClusterCmd.AddCommand(cpGroupCmd)
	ClusterCmd.AddCommand(workerGroupCmd)
}

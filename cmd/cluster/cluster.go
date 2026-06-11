package cluster

import (
	"github.com/spf13/cobra"
)

// ClusterCmd is the root command for running Heddle in cluster mode.
var ClusterCmd = &cobra.Command{
	Use:   "cluster",
	Short: "Distributed Execution",
	Long: `Manage Heddle components in a cluster environment, 
including Control Plane and Worker lifecycle.`,
}

func init() {
	ClusterCmd.AddCommand(controlPlaneGroupCmd)
	ClusterCmd.AddCommand(workerGroupCmd)
}

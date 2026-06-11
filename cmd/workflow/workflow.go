package workflow

import (
	"fmt"

	"github.com/spf13/cobra"
)

// WorkflowCmd represents the workflow group
var WorkflowCmd = &cobra.Command{
	Use:   "workflow",
	Short: "Inspects the metrics and history of a specific DAG execution",
}

var lsCmd = &cobra.Command{
	Use:   "ls",
	Short: "Lists DAG executions",
	Run: func(cmd *cobra.Command, args []string) {
		fmt.Println("ID          NAME            STATUS      DURATION    STARTED")
		fmt.Println("wf-883a12   process_data    COMPLETED   12s         2024-05-11 14:02:11")
		fmt.Println("wf-91c2b4   etl_nightly     RUNNING     4m          2024-05-11 17:35:01")
		fmt.Println("wf-0f4d32   sync_users      FAILED      1s          2024-05-11 17:40:00")
	},
}

var statsCmd = &cobra.Command{
	Use:   "stats <workflow_id>",
	Short: "Inspects the metrics of a specific DAG execution",
	Args:  cobra.ExactArgs(1),
	Run: func(cmd *cobra.Command, args []string) {
		id := args[0]
		fmt.Printf("Mock: Metrics for workflow %s\n", id)
		fmt.Println("--------------------------------------------------")
		fmt.Println("Total Steps: 15")
		fmt.Println("Success Rate: 100%")
		fmt.Println("Peak Memory (SHM): 256MB")
		fmt.Println("Data Movement: 1.2GB (Zero-copy)")
		fmt.Println("Latency (p99): 140ms")
	},
}

var stopCmd = &cobra.Command{
	Use:   "stop <workflow_id>",
	Short: "Gracefully terminates a specific DAG execution",
	Args:  cobra.ExactArgs(1),
	Run: func(cmd *cobra.Command, args []string) {
		id := args[0]
		fmt.Printf("Mock: Gracefully stopping workflow %s...\n", id)
		fmt.Println("Signal sent to Control Plane. Waiting for workers to flush state.")
	},
}

var killCmd = &cobra.Command{
	Use:   "kill <workflow_id>",
	Short: "Forcefully terminates a specific DAG execution",
	Args:  cobra.ExactArgs(1),
	Run: func(cmd *cobra.Command, args []string) {
		id := args[0]
		fmt.Printf("Mock: Force-killing workflow %s...\n", id)
		fmt.Println("SIGKILL sent to all workers associated with this DAG.")
	},
}

var rmCmd = &cobra.Command{
	Use:   "rm <workflow_id>",
	Short: "Removes a specific DAG execution",
	Args:  cobra.ExactArgs(1),
	Run: func(cmd *cobra.Command, args []string) {
		id := args[0]
		fmt.Printf("Mock: Removing workflow %s record from history.\n", id)
	},
}

func init() {
	WorkflowCmd.AddCommand(RunCmd)
	WorkflowCmd.AddCommand(InitCmd)
	WorkflowCmd.AddCommand(lsCmd)
	WorkflowCmd.AddCommand(statsCmd)
	WorkflowCmd.AddCommand(stopCmd)
	WorkflowCmd.AddCommand(killCmd)
	WorkflowCmd.AddCommand(rmCmd)
}

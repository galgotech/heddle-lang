package inspect

import (
	"fmt"

	"github.com/spf13/cobra"
)

// InspectCmd represents the inspect group
var InspectCmd = &cobra.Command{
	Use:   "inspect",
	Short: "Global observability tool",
}

var cpCmd = &cobra.Command{
	Use:   "cp",
	Short: "Inspects the internal state of the Control Plane",
	Run: func(cmd *cobra.Command, args []string) {
		fmt.Println("Mock: Healthy Control Plane")
		fmt.Println("Address: localhost:50051 (TCP)")
		fmt.Println("Active Workers: 3")
		fmt.Println("Pending Tasks: 0")
		fmt.Println("Uptime: 24h 15m")
	},
}

var workerCmd = &cobra.Command{
	Use:   "worker <worker_id>",
	Short: "Inspects resources and workload of a specific Worker",
	Args:  cobra.MaximumNArgs(1),
	Run: func(cmd *cobra.Command, args []string) {
		id := "worker-local"
		if len(args) > 0 {
			id = args[0]
		}
		fmt.Printf("Mock: Inspecting Worker %s\n", id)
		fmt.Println("CPU Usage: 15%")
		fmt.Println("Memory Usage: 450MB")
		fmt.Println("Active Plugins: std, std/io, python-sdk")
		fmt.Println("Slot Availability: 8/10")
	},
}

var pluginsCmd = &cobra.Command{
	Use:   "plugins",
	Short: "Lists languages/plugins connected via gRPC/UDS to a node",
	Run: func(cmd *cobra.Command, args []string) {
		fmt.Println("ID          LANGUAGE    TRANSPORT   CAPABILITIES")
		fmt.Println("std         Go          Internal    arithmetic, control-flow")
		fmt.Println("std/io      Go          Internal    fs, network, stdout")
		fmt.Println("py-ml       Python      UDS         transformer, pandas-agg")
		fmt.Println("rs-crypto   Rust        TCP         blake3, sha256")
	},
}

var memoryCmd = &cobra.Command{
	Use:   "memory",
	Short: "Inspects zero-copy memory allocation (Apache Arrow)",
	Run: func(cmd *cobra.Command, args []string) {
		fmt.Println("Mock: Returning zero-copy memory status")
		fmt.Println("Shared Memory Segment: /dev/shm/heddle-*")
		fmt.Println("Allocated: 1.2GB")
		fmt.Println("Available: 6.8GB")
		fmt.Println("Active Buffers: 42")
		fmt.Println("Fragmentation: 2%")
	},
}

var stepsCmd = &cobra.Command{
	Use:   "steps",
	Short: "Inspects the tasks (steps) available in the Data Locality Registry",
	Run: func(cmd *cobra.Command, args []string) {
		fmt.Println("STEP NAME           VERSION     PROVIDER    LOCALITY")
		fmt.Println("csv_read            1.0.0       std/io      Local")
		fmt.Println("vector_search       0.2.1       py-ml       Remote")
		fmt.Println("sql_query           1.4.0       std/io      Local")
		fmt.Println("tensor_product      0.5.0       py-ml       Remote")
	},
}

func init() {
	InspectCmd.AddCommand(cpCmd)
	InspectCmd.AddCommand(workerCmd)
	InspectCmd.AddCommand(pluginsCmd)
	InspectCmd.AddCommand(memoryCmd)
	InspectCmd.AddCommand(stepsCmd)
}

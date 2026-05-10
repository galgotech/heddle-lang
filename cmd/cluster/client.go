package cluster

import (
	"context"
	"fmt"
	"log"
	"os"

	"github.com/spf13/cobra"
	"github.com/spf13/viper"

	"github.com/galgotech/heddle-lang/internal/services/client"
	"github.com/galgotech/heddle-lang/pkg/config"
)

var clientCfgFile string

var clientCmd = &cobra.Command{
	Use:   "client",
	Short: "Heddle Client interacts with the control plane",
	PersistentPreRunE: func(cmd *cobra.Command, args []string) error {
		return config.Init("HEDDLE_CLIENT", clientCfgFile)
	},
}

var submitCmd = &cobra.Command{
	Use:   "submit <file.he>",
	Short: "Submit a heddle file for processing",
	Args:  cobra.ExactArgs(1),
	Run: func(cmd *cobra.Command, args []string) {
		filePath := args[0]
		content, err := os.ReadFile(filePath)
		if err != nil {
			log.Fatalf("Failed to read file: %v", err)
		}

		serverAddr := viper.GetString("server")
		c, err := client.NewControlPlaneClient(serverAddr)
		if err != nil {
			log.Fatalf("Failed to connect to control plane: %v", err)
		}

		ctx := context.Background()
		result, err := c.SubmitWorkflow(ctx, string(content))
		if err != nil {
			log.Fatalf("Submission failed: %v", err)
		}

		fmt.Printf("Workflow submitted successfully: %s\n", result)
	},
}

func init() {
	clientCmd.PersistentFlags().StringVar(&clientCfgFile, "config", "", "config file (default is ./heddle-client.yaml)")
	clientCmd.PersistentFlags().String("server", "localhost:50051", "Control plane address")
	viper.BindPFlag("server", clientCmd.PersistentFlags().Lookup("server"))

	clientCmd.AddCommand(submitCmd)
	ClusterCmd.AddCommand(clientCmd)
}

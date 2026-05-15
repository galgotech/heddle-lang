package run

import (
	"context"
	"fmt"
	"os"
	"path/filepath"
	"time"

	"github.com/spf13/cobra"
	"github.com/spf13/viper"

	"github.com/galgotech/heddle-lang/internal/services/client"
	"github.com/galgotech/heddle-lang/pkg/config"
)

// RunCmd implements the 'run' command.
var RunCmd = &cobra.Command{
	Use:   "run <file.he>",
	Short: "Executes an orchestration DAG (.he)",
	Long: `Submits a Heddle (.he) file to a running Control Plane for execution.
If mode is 'local', it connects to the local Control Plane via Unix Domain Socket.
If mode is 'remote', it connects to the specified target address via gRPC.`,
	Args: cobra.ExactArgs(1),
	RunE: func(cmd *cobra.Command, args []string) error {
		filePath := args[0]
		if filepath.Ext(filePath) != ".he" {
			return fmt.Errorf("invalid file extension: %s (expected .he)", filePath)
		}

		content, err := os.ReadFile(filePath)
		if err != nil {
			return fmt.Errorf("failed to read file: %w", err)
		}

		// Load config
		cfgFile, _ := cmd.Flags().GetString("config")
		cfg, err := config.LoadHeddleConfig(cfgFile)
		if err != nil {
			return fmt.Errorf("failed to load config: %w", err)
		}
		if cfg == nil {
			cfg = &config.HeddleConfig{}
		}

		// Flag overrides
		mode, _ := cmd.Flags().GetString("mode")
		if mode == "" && cfg.Client.Mode != "" {
			mode = cfg.Client.Mode
		} else if mode == "" {
			mode = "local"
		}

		target, _ := cmd.Flags().GetString("target")
		if target == "" && cfg.Client.Target != "" {
			target = cfg.Client.Target
		}

		timeoutStr, _ := cmd.Flags().GetString("timeout")
		if timeoutStr == "" && cfg.Client.Workflow.Timeout != "" {
			timeoutStr = cfg.Client.Workflow.Timeout
		} else if timeoutStr == "" {
			timeoutStr = "30s"
		}

		timeout, err := time.ParseDuration(timeoutStr)
		if err != nil {
			return fmt.Errorf("invalid timeout duration: %w", err)
		}

		// Connection logic
		var addr string
		if mode == "local" {
			addr = "unix:///tmp/heddle-cp.sock"
		} else {
			if target == "" {
				return fmt.Errorf("--target is required for remote mode when not specified in config")
			}
			addr = target
		}

		fmt.Printf("Simulating execution of DAG file %s in %s mode...\n", filePath, mode)
		fmt.Printf("Connecting to Control Plane at %s...\n", addr)

		// Real submission logic (since the CP might be running)
		ctx, cancel := context.WithTimeout(context.Background(), timeout)
		defer cancel()

		c, err := client.NewControlPlaneClient(addr)
		if err != nil {
			return fmt.Errorf("failed to connect to control plane: %w", err)
		}

		workflowName, _ := cmd.Flags().GetString("workflow")

		res, err := c.SubmitWorkflow(ctx, string(content), workflowName)
		if err != nil {
			return fmt.Errorf("submission failed: %w", err)
		}

		fmt.Printf("Workflow submitted successfully. Result: %s\n", res)
		return nil
	},
}

func init() {
	RunCmd.Flags().String("timeout", "30s", "Timeout for plugin handshake (e.g., 30s)")
	RunCmd.Flags().String("mode", "local", "Defines the execution mode: 'local' or 'remote'")
	RunCmd.Flags().String("target", "", "Control Plane address (Required if --mode=remote and absent in config)")
	RunCmd.Flags().String("workflow", "", "Specific workflow name to execute")

	viper.BindPFlag("client.mode", RunCmd.Flags().Lookup("mode"))
	viper.BindPFlag("client.target", RunCmd.Flags().Lookup("target"))
	viper.BindPFlag("client.workflow.timeout", RunCmd.Flags().Lookup("timeout"))
	viper.BindPFlag("client.workflow.name", RunCmd.Flags().Lookup("workflow"))
}

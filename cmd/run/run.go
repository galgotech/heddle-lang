package run

import (
	"context"
	"fmt"
	"os"
	"path/filepath"
	"time"

	"github.com/spf13/cobra"
	"github.com/spf13/viper"

	"github.com/galgotech/heddle-lang/internal/client"
	"github.com/galgotech/heddle-lang/internal/config"
	"github.com/galgotech/heddle-lang/pkg/runtime"
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
		var cfgFile string
		if cmd.Flags().Lookup("config") != nil {
			var err error
			cfgFile, err = cmd.Flags().GetString("config")
			if err != nil {
				return fmt.Errorf("failed to get config: %w", err)
			}
		}

		cfg, err := config.LoadHeddleConfig(cfgFile)
		if err != nil {
			return fmt.Errorf("failed to load config: %w", err)
		}
		if cfg == nil {
			cfg = &config.HeddleConfig{}
		}

		// Read execution mode and target from configuration
		mode := cfg.Client.Mode
		if mode == "" {
			mode = "local"
		}

		target := cfg.Client.Target

		timeoutStr := cfg.Client.Workflow.Timeout
		if timeoutStr == "" {
			timeoutStr = "30s"
		}

		timeout, err := time.ParseDuration(timeoutStr)
		if err != nil {
			return fmt.Errorf("invalid timeout duration: %w", err)
		}

		// Connection logic
		var addr string
		if mode == "local" {
			addr = runtime.ControlPlaneUDSPath
		} else {
			if target == "" {
				return fmt.Errorf("target is required for remote mode when not specified in config")
			}
			addr = target
		}

		fmt.Printf("Execution of DAG file %s in %s...\n", filePath, mode)
		fmt.Printf("Connecting to Control Plane at %s...\n", addr)

		// Real submission logic (since the CP might be running)
		ctx, cancel := context.WithTimeout(context.Background(), timeout)
		defer cancel()

		client, err := client.NewControlPlaneClient(ctx, addr)
		if err != nil {
			return fmt.Errorf("failed to connect to control plane: %w", err)
		}

		workflowName, err := cmd.Flags().GetString("flow")
		if err != nil {
			return fmt.Errorf("failed to get flow name: %w", err)
		}

		asyncFlag, err := cmd.Flags().GetBool("async")
		if err != nil {
			return fmt.Errorf("failed to get async flag: %w", err)
		}

		interactiveFlag, err := cmd.Flags().GetBool("interactive")
		if err != nil {
			return fmt.Errorf("failed to get interactive flag: %w", err)
		}

		strategy := "recursive"
		if interactiveFlag {
			strategy = "interactive"
		}

		res, err := client.SubmitWorkflow(string(content), workflowName, strategy, asyncFlag)
		if err != nil {
			return fmt.Errorf("submission failed: %w", err)
		}

		fmt.Printf("Workflow submitted successfully. Result: %s\n", res)
		return nil
	},
}

func init() {
	RunCmd.Flags().String("flow", "", "Specific workflow name to execute")

	RunCmd.Flags().BoolP("async", "a", false, "Asynchronous execution (releases terminal)")
	RunCmd.Flags().BoolP("interactive", "i", false, "Interactive execution (user must confirm each step)")

	viper.BindPFlag("client.workflow.name", RunCmd.Flags().Lookup("flow"))
	viper.BindPFlag("client.workflow.async", RunCmd.Flags().Lookup("async"))
	viper.BindPFlag("client.workflow.interactive", RunCmd.Flags().Lookup("interactive"))
}

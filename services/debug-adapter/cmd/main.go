package main

import (
	"fmt"
	"os"

	"github.com/spf13/cobra"
	"github.com/spf13/viper"

	"github.com/galgotech/heddle-lang/pkg/config"
	"github.com/galgotech/heddle-lang/pkg/logger"
	"github.com/galgotech/heddle-lang/services/debug-adapter/pkg/adapter"
)

var (
	cfgFile string
)

var rootCmd = &cobra.Command{
	Use:   "heddle-dap",
	Short: "Heddle Debug Adapter",
	PersistentPreRunE: func(cmd *cobra.Command, args []string) error {
		return initializeConfig()
	},
	Run: func(cmd *cobra.Command, args []string) {
		// Initialize logger with file output
		err := logger.Init(logger.Config{
			Development: true,
			OutputPaths: []string{"stdout", "heddle-dap.log"},
		})
		if err != nil {
			panic(err)
		}
		defer logger.Sync()

		logger.L().Info("Heddle Debug Adapter starting")

		if viper.GetBool("server") {
			adapter.StartServer(viper.GetString("addr"))
		} else {
			adapter.Serve(os.Stdin, os.Stdout)
		}
	},
}

func initializeConfig() error {
	return config.Init("HEDDLE_DAP", cfgFile)
}

func init() {
	rootCmd.PersistentFlags().StringVar(&cfgFile, "config", "", "config file (default is ./heddle-dap.yaml)")

	rootCmd.Flags().Bool("server", false, "Start in server mode")
	rootCmd.Flags().String("addr", "localhost:4711", "Address to listen on in server mode")

	viper.BindPFlag("server", rootCmd.Flags().Lookup("server"))
	viper.BindPFlag("addr", rootCmd.Flags().Lookup("addr"))
}

func main() {
	if err := rootCmd.Execute(); err != nil {
		fmt.Fprintln(os.Stderr, err)
		os.Exit(1)
	}
}

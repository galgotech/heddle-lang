package main

import (
	"fmt"
	"os"

	"github.com/spf13/cobra"
	"go.uber.org/zap"

	"github.com/galgotech/heddle-lang/pkg/logger"
)

var (
	port int
)

var rootCmd = &cobra.Command{
	Use:   "heddle-control-plane",
	Short: "Heddle Control Plane manages workers and workflow execution",
	Run: func(cmd *cobra.Command, args []string) {
		// Initialize logger
		if err := logger.Init(logger.Config{Development: true}); err != nil {
			panic(err)
		}
		defer logger.Sync()

		logger.L().Info("Heddle Control Plane starting", zap.Int("port", port))
		logger.L().Info("Arrow Flight server initializing")

		StartServer(port)
	},
}

func init() {
	rootCmd.Flags().IntVarP(&port, "port", "p", 50051, "Port to listen on")
}

func main() {
	if err := rootCmd.Execute(); err != nil {
		fmt.Fprintln(os.Stderr, err)
		os.Exit(1)
	}
}

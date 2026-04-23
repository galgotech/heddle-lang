package main

import (
	"fmt"
	"os"
	"strings"

	"github.com/spf13/cobra"
	"github.com/spf13/viper"
	"go.uber.org/zap"

	"github.com/galgotech/heddle-lang/pkg/logger"
)

var (
	cfgFile string
)

var rootCmd = &cobra.Command{
	Use:   "heddle-control-plane",
	Short: "Heddle Control Plane manages workers and workflow execution",
	PersistentPreRunE: func(cmd *cobra.Command, args []string) error {
		return initializeConfig(cmd)
	},
	Run: func(cmd *cobra.Command, args []string) {
		// Initialize logger
		if err := logger.Init(logger.Config{Development: true}); err != nil {
			panic(err)
		}
		defer logger.Sync()

		port := viper.GetInt("port")
		logger.L().Info("Heddle Control Plane starting", zap.Int("port", port))
		logger.L().Info("Arrow Flight server initializing")

		StartServer(port)
	},
}

func initializeConfig(cmd *cobra.Command) error {
	viper.SetEnvPrefix("HEDDLE_CP")
	viper.SetEnvKeyReplacer(strings.NewReplacer("-", "_"))
	viper.AutomaticEnv()

	if cfgFile != "" {
		viper.SetConfigFile(cfgFile)
	} else {
		viper.SetConfigName("heddle-cp")
		viper.SetConfigType("yaml")
		viper.AddConfigPath(".")
		viper.AddConfigPath("$HOME/.heddle")
	}

	if err := viper.ReadInConfig(); err != nil {
		if _, ok := err.(viper.ConfigFileNotFoundError); !ok {
			return err
		}
	}

	return nil
}

func init() {
	rootCmd.PersistentFlags().StringVar(&cfgFile, "config", "", "config file (default is ./heddle-cp.yaml)")

	rootCmd.Flags().IntP("port", "p", 50051, "Port to listen on")
	viper.BindPFlag("port", rootCmd.Flags().Lookup("port"))
}

func main() {
	if err := rootCmd.Execute(); err != nil {
		fmt.Fprintln(os.Stderr, err)
		os.Exit(1)
	}
}



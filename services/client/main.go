package main

import (
	"context"
	"fmt"
	"io"
	"os"
	"strings"
	"time"

	"github.com/spf13/cobra"
	"github.com/spf13/viper"
	"go.uber.org/zap"

	"github.com/galgotech/heddle-lang/pkg/logger"
	heddlesdk "github.com/galgotech/heddle-lang/sdk/go"
)

var (
	cfgFile string
)

var rootCmd = &cobra.Command{
	Use:   "heddle-client",
	Short: "Heddle Client interacts with the control plane",
	PersistentPreRunE: func(cmd *cobra.Command, args []string) error {
		return initializeConfig(cmd)
	},
}

var submitCmd = &cobra.Command{
	Use:   "submit <file.he>",
	Short: "Submit a heddle file for processing",
	Args:  cobra.ExactArgs(1),
	Run: func(cmd *cobra.Command, args []string) {
		// Initialize logger
		if err := logger.Init(logger.Config{Development: true}); err != nil {
			panic(err)
		}
		defer logger.Sync()

		serverAddr := viper.GetString("server")
		filePath := args[0]

		ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
		defer cancel()

		client, err := heddlesdk.NewControlPlaneClient(serverAddr)
		if err != nil {
			logger.L().Fatal("Failed to create client", zap.Error(err))
		}
		defer client.Close()

		file, err := os.Open(filePath)
		if err != nil {
			logger.L().Fatal("Failed to open file", zap.Error(err), zap.String("path", filePath))
		}
		defer file.Close()

		content, err := io.ReadAll(file)
		if err != nil {
			logger.L().Fatal("Failed to read file", zap.Error(err), zap.String("path", filePath))
		}

		logger.L().Info("Submitting workflow", zap.String("path", filePath))
		result, err := client.SubmitWorkflow(ctx, content)
		if err != nil {
			logger.L().Fatal("Submission failed", zap.Error(err))
		}

		fmt.Printf("Success: %s\n", result)
	},
}

func initializeConfig(cmd *cobra.Command) error {
	viper.SetEnvPrefix("HEDDLE_CLIENT")
	viper.SetEnvKeyReplacer(strings.NewReplacer("-", "_"))
	viper.AutomaticEnv()

	if cfgFile != "" {
		viper.SetConfigFile(cfgFile)
	} else {
		viper.SetConfigName("heddle-client")
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
	rootCmd.PersistentFlags().StringVar(&cfgFile, "config", "", "config file (default is ./heddle-client.yaml)")
	rootCmd.PersistentFlags().String("server", "localhost:50051", "Control plane address")
	viper.BindPFlag("server", rootCmd.PersistentFlags().Lookup("server"))

	rootCmd.AddCommand(submitCmd)
}

func main() {
	if err := rootCmd.Execute(); err != nil {
		fmt.Fprintln(os.Stderr, err)
		os.Exit(1)
	}
}

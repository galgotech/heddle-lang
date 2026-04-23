package main

import (
	"context"
	"flag"
	"fmt"
	"io"
	"os"
	"time"

	"go.uber.org/zap"

	"github.com/galgotech/heddle-lang/pkg/logger"
	heddlesdk "github.com/galgotech/heddle-lang/sdk/go"
)

func main() {
	serverAddr := flag.String("server", "localhost:50051", "Control plane address")
	flag.Parse()

	// Initialize logger
	if err := logger.Init(logger.Config{Development: true}); err != nil {
		panic(err)
	}
	defer logger.Sync()

	if flag.NArg() < 1 {
		fmt.Println("Usage: heddle-client [options] <command> [args]")
		fmt.Println("Commands:")
		fmt.Println("  submit <file.he>  Submit a heddle file for processing")
		os.Exit(1)
	}

	command := flag.Arg(0)

	ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
	defer cancel()

	client, err := heddlesdk.NewControlPlaneClient(*serverAddr)
	if err != nil {
		logger.L().Fatal("Failed to create client", zap.Error(err))
	}
	defer client.Close()

	switch command {
	case "submit":
		if flag.NArg() < 2 {
			logger.L().Fatal("Missing heddle file path")
		}
		filePath := flag.Arg(1)

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

	default:
		logger.L().Fatal("Unknown command", zap.String("command", command))
	}
}

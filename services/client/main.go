package main

import (
	"context"
	"flag"
	"fmt"
	"io"
	"log"
	"os"
	"time"

	"github.com/galgotech/heddle-lang/sdk/go"
)

func main() {
	serverAddr := flag.String("server", "localhost:50051", "Control plane address")
	flag.Parse()

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
		log.Fatalf("Failed to create client: %v", err)
	}
	defer client.Close()

	switch command {
	case "submit":
		if flag.NArg() < 2 {
			log.Fatal("Missing heddle file path")
		}
		filePath := flag.Arg(1)
		
		file, err := os.Open(filePath)
		if err != nil {
			log.Fatalf("Failed to open file: %v", err)
		}
		defer file.Close()

		content, err := io.ReadAll(file)
		if err != nil {
			log.Fatalf("Failed to read file: %v", err)
		}

		log.Printf("Submitting workflow from %s...", filePath)
		result, err := client.SubmitWorkflow(ctx, content)
		if err != nil {
			log.Fatalf("Submission failed: %v", err)
		}

		fmt.Printf("Success: %s\n", result)

	default:
		log.Fatalf("Unknown command: %s", command)
	}
}

package main

import (
	"context"
	"log"
	"os"
	"os/signal"
	"syscall"

	heddlesdk "github.com/galgotech/heddle-lang/sdk/go"
)

func main() {
	pluginID := "calculator-plugin"
	serverAddr := os.Getenv("HEDDLE_PLUGIN_SERVER")
	if serverAddr == "" {
		serverAddr = "localhost:50052"
	}

	log.Printf("Starting Calculator Plugin (ID: %s)...", pluginID)

	client, err := heddlesdk.NewPluginClient(pluginID, serverAddr)
	if err != nil {
		log.Fatalf("failed to create plugin client: %v", err)
	}
	defer client.Close()

	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

	// Handle graceful shutdown
	sigChan := make(chan os.Signal, 1)
	signal.Notify(sigChan, syscall.SIGINT, syscall.SIGTERM)
	go func() {
		<-sigChan
		log.Println("Shutting down...")
		cancel()
	}()

	if err := client.Run(ctx); err != nil && ctx.Err() == nil {
		log.Fatalf("Plugin exited with error: %v", err)
	}

	log.Println("Calculator Plugin stopped.")
}

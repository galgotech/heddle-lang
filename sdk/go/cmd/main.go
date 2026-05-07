package main

import (
	"fmt"
	"log"
	"net"
	"os"

	"github.com/galgotech/heddle-lang/pkg/stdlib"
	"github.com/galgotech/heddle-lang/sdk/go/plugin"
)

func main() {
	namespace := "std"
	p := plugin.New(namespace)

	// Register stdlib steps
	stdlib.RegisterAll(p)

	// Listen on UDS
	socketPath := fmt.Sprintf("/tmp/heddle-plugin-%s.sock", namespace)
	if os.Getenv("HEDDLE_PLUGIN_ADDR") != "" {
		socketPath = os.Getenv("HEDDLE_PLUGIN_ADDR")
	}
	// Trim unix:// prefix if present
	if len(socketPath) > 7 && socketPath[:7] == "unix://" {
		socketPath = socketPath[7:]
	}

	_ = os.Remove(socketPath)
	lis, err := net.Listen("unix", socketPath)
	if err != nil {
		log.Fatalf("failed to listen on %s: %v", socketPath, err)
	}
	defer os.Remove(socketPath)

	// Print the address for the worker to discover
	fmt.Printf("ADDRESS=unix://%s\n", socketPath)

	log.Printf("Go Plugin [%s] listening on %s", namespace, socketPath)
	if err := p.ServeListener(lis); err != nil {
		log.Fatalf("plugin server failed: %v", err)
	}
}

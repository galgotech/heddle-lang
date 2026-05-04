package main

import (
	"context"
	"fmt"
	"log"
	"net"
	"os"

	"github.com/galgotech/heddle-lang/sdk/go/core"
	"github.com/galgotech/heddle-lang/sdk/go/plugin"
)

// PrintStep implements std:io:print.
func PrintStep(ctx context.Context, config struct{}, input *core.Table) (*core.Table, error) {
	if input == nil || input.Record == nil {
		fmt.Println("<nil>")
		return nil, nil
	}

	fmt.Printf("--- std:io:print (via Go Plugin) ---\n")
	fmt.Printf("Rows: %d, Cols: %d\n", input.Record.NumRows(), input.Record.NumCols())

	for i := 0; i < int(input.Record.NumCols()); i++ {
		field := input.Record.Schema().Field(i)
		fmt.Printf("Column %d (%s): %v\n", i, field.Name, input.Record.Column(i))
	}
	fmt.Printf("-------------------\n")

	return input, nil
}

func main() {
	p := plugin.New()

	// Register stdlib steps
	p.RegisterStep("print", PrintStep)

	// Listen on UDS
	socketPath := "/tmp/heddle-plugin-go.sock"
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

	log.Printf("Go Plugin listening on %s", socketPath)
	if err := p.ServeListener(lis); err != nil {
		log.Fatalf("plugin server failed: %v", err)
	}
}

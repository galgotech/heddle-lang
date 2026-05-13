package std

import (
	"context"
	"fmt"

	"github.com/galgotech/heddle-lang/sdk/go/plugin"
)

type Config struct {
	plugin.Config
}

type PrintFrame struct {
	plugin.HeddleFrame

	Print *plugin.String
}

// PrintStep implements std:io:print.
func PrintStep(ctx context.Context, config Config, input *PrintFrame, output *plugin.VoidFrame) error {
	fmt.Printf("--- std/io.print ---\n")

	a := input.Print.Value(0)
	fmt.Println(a)

	fmt.Printf("Value: %s\n", input.Print.Value(0))

	fmt.Printf("-------------------\n")
	return nil
}

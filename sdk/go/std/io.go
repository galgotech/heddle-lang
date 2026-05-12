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

	Print plugin.Field[string]
}

// PrintStep implements std:io:print.
func PrintStep(ctx context.Context, config Config, input PrintFrame) (plugin.VoidFrame, error) {
	fmt.Printf("--- std:io:print (via Go Stdlib) ---\n")
	fmt.Printf("Rows: %d, Cols: %d\n", input.NumRows(), input.NumCols())

	fmt.Print(input.Print.Value(0))

	fmt.Printf("-------------------\n")
	return plugin.VoidFrame{}, nil
}

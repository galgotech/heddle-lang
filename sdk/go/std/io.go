package std

import (
	"context"
	"fmt"

	"github.com/galgotech/heddle-lang/sdk/go/core"
	"github.com/galgotech/heddle-lang/sdk/go/plugin"
)

type Config struct {
	plugin.Config
}

// PrintStep implements std:io:print.
func PrintStep(ctx context.Context, config Config, input core.Table) (core.Table, error) {
	if input == nil || input.Native() == nil {
		fmt.Println("<nil>")
		return nil, nil
	}

	fmt.Printf("--- std:io:print (via Go Stdlib) ---\n")
	fmt.Printf("Rows: %d, Cols: %d\n", input.Native().NumRows(), input.Native().NumCols())

	for i := 0; i < int(input.Native().NumCols()); i++ {
		field := input.Native().Schema().Field(i)
		fmt.Printf("Column %d (%s): %v\n", i, field.Name, input.Native().Column(i))
	}
	fmt.Printf("-------------------\n")

	return input, nil
}

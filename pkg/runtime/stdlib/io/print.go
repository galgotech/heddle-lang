package io

import (
	"context"
	"fmt"

	"github.com/galgotech/heddle-lang/pkg/runtime/execution"

	"github.com/apache/arrow/go/v18/arrow"
)

// PrintStep implements std:io:print.
func PrintStep(ctx context.Context, input arrow.Record) (arrow.Record, error) {
	if input == nil {
		fmt.Println("<nil>")
		return nil, nil
	}

	fmt.Printf("--- std:io:print ---\n")
	fmt.Printf("Rows: %d, Cols: %d\n", input.NumRows(), input.NumCols())

	for i := 0; i < int(input.NumCols()); i++ {
		field := input.Schema().Field(i)
		fmt.Printf("Column %d (%s): %v\n", i, field.Name, input.Column(i))
	}
	fmt.Printf("-------------------\n")

	// Print usually returns the same input so it can be piped.
	return input, nil
}

func init() {
	execution.GlobalRegistry.Register("std:io", "print", PrintStep)
}

package stdlib

import (
	"context"
	"fmt"

	"github.com/galgotech/heddle-lang/sdk/go/core"
	"github.com/galgotech/heddle-lang/sdk/go/plugin"
)

type DataConfig struct {
	plugin.Config
	Data []map[string]any `json:"data"`
}

// DataStep implements std:data. It receives planning-time data
// and makes it available as a table in the pipeline.
func DataStep(ctx context.Context, config DataConfig, input core.Table) (core.Table, error) {
	// In a real implementation, this would convert JSON data to an Arrow RecordBatch.
	// For now, we return nil as a placeholder, as the primary goal is the
	// planning-time data injection mechanism.
	return nil, nil
}

// DefaultPlanningHandler is a built-in handler that simply logs the received data.
func DefaultPlanningHandler(data []map[string]any) error {
	fmt.Printf("--- std:data (Planning Time) ---\n")
	fmt.Printf("Received %d rows of planning-time data\n", len(data))
	for i, row := range data {
		if i < 5 { // Only print first 5 rows
			fmt.Printf("Row %d: %v\n", i, row)
		}
	}
	if len(data) > 5 {
		fmt.Printf("... and %d more rows\n", len(data)-5)
	}
	fmt.Printf("-------------------------------\n")
	return nil
}

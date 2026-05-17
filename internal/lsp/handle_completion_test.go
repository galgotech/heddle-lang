package lsp

import (
	"testing"

	"github.com/stretchr/testify/assert"
	"go.lsp.dev/protocol"

	"github.com/galgotech/heddle-lang/internal/models"
	"github.com/galgotech/heddle-lang/pkg/schema"
)

func TestGetCompletionItems(t *testing.T) {
	// logger := zap.NewNop() // No longer needed

	registry := &models.RegistryInfo{
		Steps: map[string]schema.StepSchemas{
			"postgres.query":   {},
			"postgres.execute": {},
			"mailgun.send":     {},
		},
	}

	tests := []struct {
		name     string
		source   string
		pos      protocol.Position
		expected []string
	}{
		{
			name:     "suggest namespaces at start",
			source:   "workflow main {\n  \n}",
			pos:      protocol.Position{Line: 1, Character: 2},
			expected: []string{"postgres", "mailgun"},
		},
		{
			name:     "suggest steps after dot",
			source:   "workflow main {\n  postgres.\n}",
			pos:      protocol.Position{Line: 1, Character: 11},
			expected: []string{"query", "execute"},
		},
		{
			name: "complete name of step imported",
			source: `import "std/io" io
workflow main {
  
}`,
			pos:      protocol.Position{Line: 2, Character: 2},
			expected: []string{"io"},
		},
		{
			name: "complete name of steps created in the same file .he",
			source: `step test = io.print
workflow main {
  
}`,
			pos:      protocol.Position{Line: 2, Character: 2},
			expected: []string{"test"},
		},
		{
			name: "complete name from assign created inside same workflow",
			source: `workflow main {
  []
  > my_var
}`,
			pos:      protocol.Position{Line: 2, Character: 2},
			expected: []string{"my_var"},
		},
		{
			name: "complete the resource name when define a step in .he file",
			source: `resource db = pg.connection { host: "localhost" }
step query = <connection=`,
			pos:      protocol.Position{Line: 1, Character: 25},
			expected: []string{"db"},
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			items := getCompletionItems(tt.source, tt.pos, registry)
			var labels []string
			for _, item := range items {
				labels = append(labels, item.Label)
			}
			for _, exp := range tt.expected {
				assert.Contains(t, labels, exp)
			}
		})
	}
}

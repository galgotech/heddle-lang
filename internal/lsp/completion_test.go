package lsp

import (
	"testing"

	"github.com/stretchr/testify/assert"
	"go.lsp.dev/protocol"
	"go.uber.org/zap"

	"github.com/galgotech/heddle-lang/internal/models"
	"github.com/galgotech/heddle-lang/pkg/schema"
)

func TestGetCompletionItems(t *testing.T) {
	logger := zap.NewNop()
	server := NewServer(logger, "localhost:50051")

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
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			items := server.getCompletionItems(tt.source, tt.pos, registry)
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

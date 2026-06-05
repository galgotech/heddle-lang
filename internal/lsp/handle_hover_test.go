package lsp

import (
	"context"
	"encoding/json"
	"strings"
	"testing"

	"github.com/stretchr/testify/assert"
	"go.lsp.dev/jsonrpc2"
	"go.lsp.dev/protocol"

	"github.com/galgotech/heddle-lang/internal/models"
	"github.com/galgotech/heddle-lang/pkg/schema"
)

func TestCleanBlockComment(t *testing.T) {
	tests := []struct {
		name     string
		input    string
		expected string
	}{
		{
			name: "Simple block comment",
			input: `/*
This is a comment.
*/`,
			expected: "This is a comment.",
		},
		{
			name: "Asterisk line prefix",
			input: `/*
 * Line one.
 * Line two.
 */`,
			expected: "Line one.\nLine two.",
		},
		{
			name:     "Empty comment",
			input:    "/**/",
			expected: "",
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			assert.Equal(t, tt.expected, CleanBlockComment(tt.input))
		})
	}
}

func TestHandleHover(t *testing.T) {
	s := NewServer(nil, "")
	ctx := context.Background()

	content := `import "fhub/postgres" pg
import "std/io" io

/*
This is local database connection.
*/
resource db = pg.connection { host: "localhost" }

/*
This is print step.
*/
step test = io.print

step query = <connection=db> pg.query { sql: "select 1" }

workflow main {
  test
  query
}
`
	uri := protocol.DocumentURI("file:///test.he")
	s.files.Store(uri, content)

	registry := &models.RegistryInfo{
		Steps: map[string]schema.StepSchemas{
			"io.print": {
				Documentation: "Prints values to standard output.",
				Input: schema.FrameSchema{
					Columns: []schema.ColumnSchema{
						{Name: "message", ArrowType: "utf8"},
					},
				},
				Output: schema.FrameSchema{},
			},
			"postgres.query": {
				Documentation: "Executes a SQL query.",
				Config: schema.FieldSchema{
					Fields: []schema.Field{
						{Name: "sql", Type: "string"},
					},
				},
				Input: schema.FrameSchema{},
				Output: schema.FrameSchema{
					Columns: []schema.ColumnSchema{
						{Name: "id", ArrowType: "int64"},
					},
				},
			},
		},
		Resources: map[string]schema.ResourceSchemas{
			"postgres.connection": {
				Documentation: "Postgres database connector.",
				Config: schema.FieldSchema{
					Fields: []schema.Field{
						{Name: "host", Type: "string"},
					},
				},
			},
		},
	}
	registryGetter := func(ctx context.Context) (*models.RegistryInfo, error) {
		return registry, nil
	}

	tests := []struct {
		name           string
		line           uint32
		char           uint32
		expectedSubstr []string
	}{
		{
			name: "Local step hover (hovering test in step definition)",
			line: 11, // 12th line: "step test = io.print"
			char: 5,  // 'e' of "test"
			expectedSubstr: []string{
				"Step: **test**",
				"Binds to: `io.print`",
				"This is print step.",
				"Input HeddleFrame",
				"message",
				"utf8",
			},
		},
		{
			name: "Local resource hover (hovering db in resource definition)",
			line: 6, // 7th line: "resource db = pg.connection..."
			char: 9, // 'd' of "db"
			expectedSubstr: []string{
				"Resource: **db**",
				"Binds to connector: `postgres.connection`",
				"This is local database connection.",
				"Configuration",
				"host",
			},
		},
		{
			name: "Workflow hover",
			line: 15, // 16th line: "workflow main {"
			char: 9,  // 'm' of "main"
			expectedSubstr: []string{
				"Workflow: **main**",
				"Orchestrated Directed Acyclic Graph",
			},
		},
		{
			name: "Registry step hover (hovering pg.query call)",
			line: 13, // 14th line: "step query = <connection=db> pg.query ..."
			char: 33, // 'q' of "query" in "pg.query"
			expectedSubstr: []string{
				"Step: **postgres.query**",
				"Executes a SQL query.",
				"Configuration",
				"sql",
				"Output HeddleFrame",
				"id",
			},
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			params := protocol.HoverParams{
				TextDocumentPositionParams: protocol.TextDocumentPositionParams{
					TextDocument: protocol.TextDocumentIdentifier{URI: uri},
					Position:     protocol.Position{Line: tt.line, Character: tt.char},
				},
			}

			var hover protocol.Hover
			reply := func(ctx context.Context, res any, err error) error {
				if err != nil {
					return err
				}
				if res == nil {
					return nil
				}
				b, _ := json.Marshal(res)
				json.Unmarshal(b, &hover)
				return nil
			}

			req, _ := jsonrpc2.NewCall(jsonrpc2.NewNumberID(1), protocol.MethodTextDocumentHover, params)
			err := handleHover(ctx, reply, req, &s.files, registryGetter)
			assert.NoError(t, err)
			assert.NotEmpty(t, hover.Contents.Value)

			for _, sub := range tt.expectedSubstr {
				assert.True(t, strings.Contains(hover.Contents.Value, sub), "Expected hover contents to contain %q, but got %q", sub, hover.Contents.Value)
			}
		})
	}
}

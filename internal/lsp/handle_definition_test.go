package lsp

import (
	"context"
	"encoding/json"
	"testing"

	"github.com/stretchr/testify/assert"
	"go.lsp.dev/jsonrpc2"
	"go.lsp.dev/protocol"

	"github.com/galgotech/heddle-lang/internal/models"
	"github.com/galgotech/heddle-lang/pkg/schema"
)

func TestHandleDefinition(t *testing.T) {
	s := NewServer(nil, "")
	ctx := context.Background()

	content := `import "fhub/postgres" pg
import "std/io" io

resource db = pg.connection { host: "localhost" }

step test = io.print
step query = <connection=db> pg.query { sql: "select 1" }

workflow main {
  test
  > my_var

  my_var
    | query
}
`
	uri := protocol.DocumentURI("file:///test.he")
	s.files.Store(uri, content)

	registry := &models.RegistryInfo{
		Steps: map[string]schema.StepSchemas{
			"postgres.query": {
				SourceFile: "/mock/postgres/query.go",
				SourceLine: 42,
			},
		},
	}
	registryGetter := func(ctx context.Context) (*models.RegistryInfo, error) {
		return registry, nil
	}

	tests := []struct {
		name              string
		line              uint32
		char              uint32
		expectedURI       protocol.DocumentURI
		expectedLine      uint32
		expectedStartChar uint32
	}{
		{
			name:              "Local step jump (click test in workflow)",
			line:              9, // 10th line: "  test"
			char:              4, // 's' of "test"
			expectedURI:       uri,
			expectedLine:      5, // 6th line: "step test = io.print"
			expectedStartChar: 5, // start of "test"
		},
		{
			name:              "Local resource jump (click db in step injection)",
			line:              6, // 7th line: "step query = <connection=db> pg.query ..."
			char:              26, // 'b' of "db" in "<connection=db>"
			expectedURI:       uri,
			expectedLine:      3, // 4th line: "resource db = pg.connection { host: \"localhost\" }"
			expectedStartChar: 9, // start of "db"
		},
		{
			name:              "Import alias jump from prefix (click pg in pg.connection)",
			line:              3, // 4th line: "resource db = pg.connection..."
			char:              15, // 'g' of "pg"
			expectedURI:       uri,
			expectedLine:      0, // 1st line: "import \"fhub/postgres\" pg"
			expectedStartChar: 23, // start of "pg"
		},
		{
			name:              "Import alias jump from prefix in step call (click pg in pg.query)",
			line:              6, // 7th line: "step query = <connection=db> pg.query ..."
			char:              30, // 'g' of "pg"
			expectedURI:       uri,
			expectedLine:      0, // 1st line: "import \"fhub/postgres\" pg"
			expectedStartChar: 23, // start of "pg"
		},
		{
			name:         "Registry step definition jump (click query in pg.query)",
			line:         6, // 7th line: "step query = <connection=db> pg.query ..."
			char:         33, // 'q' of "query"
			expectedURI:  "file:///mock/postgres/query.go",
			expectedLine: 41, // registry specifies line 42, which translates to 41 (0-indexed)
		},
		{
			name:              "Local assignment reference jump (click my_var reference in workflow)",
			line:              12, // 13th line: "  my_var"
			char:              4, // 'm' of "my_var"
			expectedURI:       uri,
			expectedLine:      10, // 11th line: "  > my_var"
			expectedStartChar: 4, // start of "my_var" after "  > "
		},
		{
			name:              "Local assignment definition self jump (click my_var definition in workflow)",
			line:              10, // 11th line: "  > my_var"
			char:              4, // 'm' of "my_var" after "  > "
			expectedURI:       uri,
			expectedLine:      10, // 11th line: "  > my_var"
			expectedStartChar: 4, // start of "my_var" after "  > "
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			params := protocol.DefinitionParams{
				TextDocumentPositionParams: protocol.TextDocumentPositionParams{
					TextDocument: protocol.TextDocumentIdentifier{URI: uri},
					Position:     protocol.Position{Line: tt.line, Character: tt.char},
				},
			}

			var location protocol.Location
			reply := func(ctx context.Context, res any, err error) error {
				if err != nil {
					return err
				}
				if res == nil {
					return nil
				}
				b, _ := json.Marshal(res)
				json.Unmarshal(b, &location)
				return nil
			}

			req, _ := jsonrpc2.NewCall(jsonrpc2.NewNumberID(1), protocol.MethodTextDocumentDefinition, params)
			err := handleDefinition(ctx, reply, req, &s.files, registryGetter)
			assert.NoError(t, err)
			assert.Equal(t, tt.expectedURI, location.URI)
			assert.Equal(t, tt.expectedLine, location.Range.Start.Line)
			if tt.expectedStartChar != 0 {
				assert.Equal(t, tt.expectedStartChar, location.Range.Start.Character)
			}
		})
	}
}

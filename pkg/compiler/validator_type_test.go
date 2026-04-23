package compiler

import (
	"testing"

	"github.com/stretchr/testify/assert"

	"github.com/galgotech/heddle-lang/pkg/lexer"
	"github.com/galgotech/heddle-lang/pkg/parser"
)

func TestValidator_TypeMismatch(t *testing.T) {
	tests := []struct {
		name    string
		code    string
		wantErr string
	}{
		{
			name: "Incompatible Schema",
			code: `schema User {
  id: int
}
schema Order {
  id: int
}
step s1: void -> User = fhub.src
step s2: Order -> void = fhub.sink
workflow main {
  s1
    | s2
}
`,
			wantErr: "type mismatch in pipe: step 's1' outputs 'User' but step 's2' expects 'Order'",
		},
		{
			name: "Void Output",
			code: `schema User {
  id: int
}
step s1: void -> void = fhub.src
step s2: User -> void = fhub.sink
workflow main {
  s1
    | s2
}
`,
			wantErr: "invalid step 's1': 'void -> void' steps are not allowed in pipelines",
		},
		{
			name: "Incompatible Handler",
			code: `schema User {
  id: int
}
schema Order {
  id: int
}
step s1: void -> User = fhub.src
step h_step: Order -> void = fhub.h
handler h1 {
  h_step
}
workflow main {
  s1 ? h1
}
`,
			wantErr: "type mismatch in handler: step 's1' outputs 'User' but handler 'h1' expects 'Order'",
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			l := lexer.New(tt.code)
			p := parser.New(l)
			prog := p.Parse()
			if len(p.Errors()) > 0 {
				t.Fatalf("Parser errors: %v", p.Errors())
			}

			v := NewValidator(prog)
			err := v.Validate()
			assert.Error(t, err)
			assert.Contains(t, err.Error(), tt.wantErr)
		})
	}
}

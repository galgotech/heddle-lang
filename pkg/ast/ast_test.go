package ast

import (
	"testing"

	"github.com/galgotech/heddle-lang/pkg/lexer"
)

func TestString(t *testing.T) {
	program := &Program{
		Statements: []Statement{
			&ImportStatement{
				Token: lexer.Token{Type: lexer.IMPORT, Literal: "import"},
				Path:  &StringLiteral{Token: lexer.Token{Type: lexer.STRING_LIT, Literal: "std/http"}, Value: "std/http"},
				Alias: &Identifier{Token: lexer.Token{Type: lexer.IDENT, Literal: "http"}, Value: "http"},
			},
			&WorkflowDefinition{
				Token: lexer.Token{Type: lexer.WORKFLOW, Literal: "workflow"},
				Name:  &Identifier{Token: lexer.Token{Type: lexer.IDENT, Literal: "main"}, Value: "main"},
				Statements: []*PipelineStatement{
					{
						Expression: &PipeChain{
							Calls: []*CallExpression{
								{
									Step: &StepCall{
										Token: lexer.Token{Type: lexer.IDENT, Literal: "getData"},
										Name:  &Identifier{Token: lexer.Token{Type: lexer.IDENT, Literal: "getData"}, Value: "getData"},
									},
								},
								{
									Step: &StepCall{
										Token: lexer.Token{Type: lexer.IDENT, Literal: "process"},
										Name:  &Identifier{Token: lexer.Token{Type: lexer.IDENT, Literal: "process"}, Value: "process"},
									},
									TrapHandler: &TrapHandler{
										Token: lexer.Token{Type: lexer.QUESTION, Literal: "?"},
										Name:  &Identifier{Token: lexer.Token{Type: lexer.IDENT, Literal: "onErr"}, Value: "onErr"},
									},
								},
							},
						},
						Assignment: &Identifier{Token: lexer.Token{Type: lexer.IDENT, Literal: "result"}, Value: "result"},
					},
				},
			},
		},
	}

	expected := "import \"std/http\" httpworkflow main {getData | process?onErr > result}"
	if program.String() != expected {
		t.Errorf("program.String() wrong. got=%q, want=%q", program.String(), expected)
	}
}

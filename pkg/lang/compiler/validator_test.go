package compiler

import (
	"testing"

	"github.com/galgotech/heddle-lang/pkg/lang/ast"
	"github.com/galgotech/heddle-lang/pkg/lang/lexer"
	"github.com/galgotech/heddle-lang/pkg/lang/parser"
)

func TestValidator_UndefinedStep(t *testing.T) {
	code := `
workflow main {
  undefined_step
}
`
	c := New()
	_, err := c.Compile(code)
	if err == nil {
		t.Fatal("expected error for undefined step, got nil")
	}
}

func TestValidator_UndefinedResource(t *testing.T) {
	code := `
import "fhub/etl" etl

step s1: void -> void = etl.extract <res = undefined_res>

workflow main {
  s1
}
`
	c := New()
	_, err := c.Compile(code)
	if err == nil {
		t.Fatal("expected error for undefined resource, got nil")
	}
}

func TestValidator_CycleDetection(t *testing.T) {
	code := `
import "fhub/etl" etl

step s1: void -> void = etl.extract

handler h1 {
  s1 ? h1
}

workflow main {
  s1 ? h1
}
`
	c := New()
	_, err := c.Compile(code)
	if err == nil {
		t.Fatal("expected error for cycle detection, got nil")
	}
}

func TestValidator_UndefinedHandler(t *testing.T) {
	code := `
import "fhub/etl" etl
step s1: void -> void = etl.extract
workflow main {
  s1 ? undefined_handler
}
`
	c := New()
	_, err := c.Compile(code)
	if err == nil {
		t.Fatal("expected error for undefined handler, got nil")
	}
}

func TestValidator_UndefinedResourceInStepBinding(t *testing.T) {
	code := `
import "fhub/etl" etl
step s1: void -> void = etl.extract <res = undefined_res>
workflow main {
  s1
}
`
	c := New()
	_, err := c.Compile(code)
	if err == nil {
		t.Fatal("expected error for undefined resource in step binding, got nil")
	}
}

func TestValidator_Lookup(t *testing.T) {
	code := `resource res1 = fhub.res
step s1: void -> void = fhub.my_step
handler h1 {
  s1
}
`
	l := lexer.New(code)
	p := parser.New(l)
	prog := p.Parse()
	if len(p.Errors()) > 0 {
		t.Fatalf("parser errors: %v", p.Errors())
	}

	v := NewValidator(prog)
	v.Validate()

	assertNotNil := func(n ast.Node, name string) {
		if n == nil {
			t.Errorf("expected to find %s", name)
		}
	}

	assertNotNil(v.Lookup("res1"), "res1")
	assertNotNil(v.Lookup("s1"), "s1")
	assertNotNil(v.Lookup("h1"), "h1")

	if v.Lookup("nonexistent") != nil {
		t.Error("expected nil for nonexistent")
	}
}

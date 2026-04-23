package compiler

import (
	"testing"
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


package ir

import "fmt"

// InstructionType defines the category of an IR instruction.
type InstructionType string

const (
	StepInst     InstructionType = "step"
	FlowInst     InstructionType = "flow"
	ResourceInst InstructionType = "resource"
	ProgramInst  InstructionType = "program"
)

// Instruction is the interface that all IR nodes must implement.
type Instruction interface {
	GetID() string
	GetType() InstructionType
}

// SourceLocation represents the position in the source code for debugging.
type SourceLocation struct {
	Line   int `json:"line"`
	Column int `json:"column"`
}

// BaseInstruction provides common functionality and fields for all instructions.
type BaseInstruction struct {
	ID             string          `json:"id"`
	Type           InstructionType `json:"type"`
	SourceLocation *SourceLocation `json:"source_location,omitempty"`
}

// GetID returns the unique identifier for the instruction.
func (b *BaseInstruction) GetID() string {
	return b.ID
}

// GetType returns the category of the instruction.
func (b *BaseInstruction) GetType() InstructionType {
	return b.Type
}

func (b *BaseInstruction) String() string {
	return fmt.Sprintf("[%s] %s", b.Type, b.ID)
}

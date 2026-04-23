package ir

import (
	"encoding/json"
	"fmt"
)

// ProgramIR is the top-level container for a compiled Heddle program.
// It is designed to be fully serializable for transmission to workers.
type ProgramIR struct {
	BaseInstruction

	// Instructions is a flat registry of all instructions by their unique ID.
	Instructions map[string]interface{} `json:"instructions"`

	// Workflows lists the IDs of the entry points (FlowInstructions) in the program.
	Workflows []string `json:"workflows"`
}

// Inflate reconstructs concrete instruction types from raw map[string]interface{}
// after JSON unmarshalling.
func (p *ProgramIR) Inflate() error {
	for id, raw := range p.Instructions {
		// If it's already a struct (e.g. from local compilation), skip
		if _, ok := raw.(Instruction); ok {
			continue
		}

		// Otherwise, it's likely a map[string]interface{} from JSON
		data, err := json.Marshal(raw)
		if err != nil {
			return fmt.Errorf("failed to re-marshal instruction %s: %w", id, err)
		}

		// Peek at the type
		var base BaseInstruction
		if err := json.Unmarshal(data, &base); err != nil {
			return fmt.Errorf("failed to unmarshal base for %s: %w", id, err)
		}

		var concrete Instruction
		switch base.Type {
		case StepInst:
			concrete = &StepInstruction{}
		case FlowInst:
			concrete = &FlowInstruction{}
		case ResourceInst:
			concrete = &ResourceInstruction{}
		default:
			return fmt.Errorf("unknown instruction type: %s", base.Type)
		}

		if err := json.Unmarshal(data, concrete); err != nil {
			return fmt.Errorf("failed to unmarshal concrete type for %s: %w", id, err)
		}

		p.Instructions[id] = concrete
	}
	return nil
}

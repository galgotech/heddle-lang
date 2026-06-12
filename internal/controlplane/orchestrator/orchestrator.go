package orchestrator

import (
	"context"

	"github.com/galgotech/heddle-lang/internal/models"
	"github.com/galgotech/heddle-lang/pkg/lang/compiler/ir"
	"github.com/galgotech/heddle-lang/pkg/logger"
	"github.com/galgotech/heddle-lang/pkg/plugin"
)

type Strategy string

const (
	StrategyRecursive   Strategy = "recursive"
	StrategyGraph       Strategy = "graph"
	StrategyInteractive Strategy = "interactive"
	StrategyDebug       Strategy = "debug"
)

type Orchestrator interface {
	OrchestrateTask(ctx context.Context, task models.Task)
}

type Worker interface {
	GetID() string
}

// ResolveResources extracts the definitions and configurations for all resources referenced by a step.
func ResolveResources(prog ir.Program, step ir.StepInstruction) map[string]plugin.ResourceDefinition {
	resources := make(map[string]plugin.ResourceDefinition)
	logger.L().Debug("resource resolution initiated: processing resources for step",
		logger.Component("orchestrator"),
		logger.Any("resources", step.Resources),
	)

	for _, resourceID := range step.Resources {
		if inst, ok := prog.Instructions[resourceID]; ok {
			var provider []string
			var config map[string]any

			r, ok := inst.(ir.ResourceInstruction)
			if !ok {
				logger.L().Warn("resource resolution anomaly: instruction is not a resource instruction",
					logger.Component("orchestrator"),
					logger.String("resource_id", resourceID),
				)
				continue
			}

			provider = r.Provider
			config = r.Config

			var resourceType string
			if len(provider) > 1 {
				resourceType = provider[1]
			} else if len(provider) > 0 {
				resourceType = provider[0]
			}

			resources[resourceID] = plugin.ResourceDefinition{
				Type:   resourceType,
				Config: config,
			}
			logger.L().Info("resource resolution completed: resource successfully resolved",
				logger.Component("orchestrator"),
				logger.String("resource_id", resourceID),
				logger.String("type", resourceType),
			)
		} else {
			logger.L().Error("resource resolution failed: resource instruction not found in program",
				logger.Component("orchestrator"),
				logger.String("resource_id", resourceID),
			)
		}
	}
	return resources
}

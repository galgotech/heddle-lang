package orchestrator

import (
	"context"

	"github.com/galgotech/heddle-lang/internal/models"
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

package maestro

import (
	"context"
	"fmt"
	"os"
	"os/exec"

	"path/filepath"
	"sync"
	"syscall"

	"go.uber.org/zap"

	"github.com/galgotech/heddle-lang/pkg/logger"
)

type GoExecutor struct {
	Namespace  string
	PluginName string
	Dir        string
	Cmd        *exec.Cmd
	mu         sync.Mutex
}

func (e *GoExecutor) Start(ctx context.Context, socketPath string) error {
	e.mu.Lock()
	defer e.mu.Unlock()

	// Use go run . to execute the worker directly from the cmd directory
	e.Cmd = exec.CommandContext(ctx, "go", "run", "./cmd/...")
	e.Cmd.Dir = filepath.Join(e.Dir)
	e.Cmd.Stdout = os.Stdout
	e.Cmd.Stderr = os.Stderr
	e.Cmd.Env = append(os.Environ(), fmt.Sprintf("HEDDLE_WORKER_ADDRESS=unix://%s", socketPath))

	logger.L().Info("Spawning worker via go run...", zap.String("plugin", e.PluginName))
	if err := e.Cmd.Start(); err != nil {
		return fmt.Errorf("failed to start process for %s/%s: %w", e.Namespace, e.PluginName, err)
	}

	return nil
}

func (e *GoExecutor) Stop() error {
	e.mu.Lock()
	defer e.mu.Unlock()

	if e.Cmd != nil && e.Cmd.Process != nil {
		logger.L().Info("Gracefully stopping plugin process...", zap.String("plugin", e.PluginName))
		if err := e.Cmd.Process.Signal(syscall.SIGTERM); err != nil {
			e.Cmd.Process.Kill()
		}
		e.Cmd.Wait()
		e.Cmd = nil
	}
	return nil
}

func (e *GoExecutor) Name() string {
	return e.Namespace + "/" + e.PluginName
}

func NewGoExecutor(namespace, pluginName, dir string) WorkerExecutor {
	return &GoExecutor{
		Namespace:  namespace,
		PluginName: pluginName,
		Dir:        dir,
	}
}

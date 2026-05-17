package maestro

import (
	"context"
	"fmt"
	"os"
	"os/signal"
	"path/filepath"
	"strings"
	"syscall"
	"time"

	"github.com/fsnotify/fsnotify"
	"go.uber.org/zap"

	"github.com/galgotech/heddle-lang/pkg/logger"
)

var Executors = map[string]ExecutorFactory{
	"go": NewGoExecutor,
}

type WorkerExecutor interface {
	Start(ctx context.Context, socketPath string) error
	Stop() error
	Name() string
}

type ExecutorFactory func(namespace, pluginName, dir string) WorkerExecutor

type Maestro struct {
	Workers     map[string]WorkerExecutor
	Watcher     *fsnotify.Watcher
	ProjectPath string
}

func (m *Maestro) Run(ctx context.Context) error {
	defer m.Watcher.Close()

	// Scan workers/ recursively for worker.toml
	if err := m.scanWorkers(); err != nil {
		return fmt.Errorf("failed to scan workers: %w", err)
	}

	// Start all initial workers
	for dir, executor := range m.Workers {
		if err := executor.Start(ctx, "/tmp/heddle-worker.sock"); err != nil {
			logger.L().Error("Worker start failed", zap.Error(err))
		}

		// Watch the directory
		if err := m.watchRecursive(dir); err != nil {
			logger.L().Error("Failed to watch worker directory", zap.Error(err))
		}
	}

	// Wait for OS signals for graceful shutdown
	sigCh := make(chan os.Signal, 1)
	signal.Notify(sigCh, os.Interrupt, syscall.SIGTERM)

	// File watcher loop
	debounceTimers := make(map[string]*time.Timer)

	for {
		select {
		case <-ctx.Done():
			return m.Shutdown()
		case <-sigCh:
			logger.L().Info("Received termination signal, shutting down...")
			return m.Shutdown()
		case event, ok := <-m.Watcher.Events:
			if !ok {
				return nil
			}
			// Only trigger on write or create
			if event.Op&fsnotify.Write == fsnotify.Write || event.Op&fsnotify.Create == fsnotify.Create {
				// Find which worker this belongs to
				executor := m.findWorkerForPath(event.Name)
				if executor != nil {
					key := executor.Name()
					if timer, exists := debounceTimers[key]; exists {
						timer.Stop()
					}

					debounceTimers[key] = time.AfterFunc(500*time.Millisecond, func() {
						logger.L().Info("File changed, hot-reloading worker...", zap.String("file", event.Name), zap.String("plugin", executor.Name()))
						executor.Stop()
						if err := executor.Start(ctx, "/tmp/heddle-worker.sock"); err != nil {
							logger.L().Error("Hot-reload failed", zap.Error(err))
						}
					})
				}

				// If a new directory is created, watch it
				if stat, err := os.Stat(event.Name); err == nil && stat.IsDir() {
					m.Watcher.Add(event.Name)
				}
			}
		case err, ok := <-m.Watcher.Errors:
			if !ok {
				return nil
			}
			logger.L().Error("Watcher error", zap.Error(err))
		}
	}
}

func (m *Maestro) scanWorkers() error {
	workersDir := filepath.Join(m.ProjectPath, "workers")
	if _, err := os.Stat(workersDir); os.IsNotExist(err) {
		return nil
	}

	return filepath.Walk(workersDir, func(path string, info os.FileInfo, err error) error {
		if err != nil {
			return err
		}
		if !info.IsDir() && info.Name() == "worker.toml" {
			dir := filepath.Dir(path)

			// Load and validate configuration from worker.toml
			meta, err := LoadWorkerConfig(path)
			if err != nil {
				logger.L().Error("Invalid worker configuration", zap.String("path", path), zap.Error(err))
				return nil
			}

			if factory, ok := Executors[meta.Worker.Runtime]; ok {
				m.Workers[dir] = factory(meta.Worker.Namespace, meta.Worker.Name, dir)
			} else {
				logger.L().Error("Unsupported runtime", zap.String("runtime", meta.Worker.Runtime), zap.String("path", path))
			}
		}
		return nil
	})
}

func (m *Maestro) findWorkerForPath(path string) WorkerExecutor {
	for dir, executor := range m.Workers {
		if strings.HasPrefix(path, dir) {
			return executor
		}
	}
	return nil
}

func (m *Maestro) watchRecursive(dir string) error {
	return filepath.Walk(dir, func(path string, info os.FileInfo, err error) error {
		if err != nil {
			return err
		}
		if info.IsDir() {
			return m.Watcher.Add(path)
		}
		return nil
	})
}

func (m *Maestro) Shutdown() error {
	for _, executor := range m.Workers {
		executor.Stop()
	}
	return nil
}

func NewMaestro(projectPath string) (*Maestro, error) {
	if projectPath == "" {
		return nil, fmt.Errorf("project path cannot be empty")
	}

	watcher, err := fsnotify.NewWatcher()
	if err != nil {
		return nil, fmt.Errorf("failed to create fsnotify watcher: %w", err)
	}

	return &Maestro{
		Workers:     make(map[string]WorkerExecutor),
		Watcher:     watcher,
		ProjectPath: projectPath,
	}, nil
}

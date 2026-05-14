package dev

import (
	"context"
	"fmt"
	"os"
	"os/exec"
	"os/signal"
	"path/filepath"
	"strings"
	"sync"
	"syscall"
	"time"

	"github.com/fsnotify/fsnotify"
	"go.uber.org/zap"

	"github.com/galgotech/heddle-lang/internal/services/control-plane"
	"github.com/galgotech/heddle-lang/internal/services/worker"
	"github.com/galgotech/heddle-lang/pkg/logger"
)

type WorkerProcess struct {
	Namespace     string
	PluginName    string
	Language      string
	Dir           string
	Cmd           *exec.Cmd
	WorkerService *worker.Worker
	SocketPath    string
	mu            sync.Mutex
}

func (w *WorkerProcess) Start(ctx context.Context, cpAddr string) error {
	w.mu.Lock()
	defer w.mu.Unlock()

	// 1. Start Worker Service if not already running
	if w.WorkerService == nil {
		pluginSocket := filepath.Join("/tmp", fmt.Sprintf("heddle-worker-%s-%s.sock", w.Namespace, w.PluginName))
		ws, err := worker.NewWorker(cpAddr, pluginSocket)
		if err != nil {
			return fmt.Errorf("failed to create worker for %s/%s: %w", w.Namespace, w.PluginName, err)
		}
		w.WorkerService = ws
		w.SocketPath = pluginSocket

		go func() {
			if err := w.WorkerService.Start(ctx); err != nil {
				logger.L().Error("Worker service failed", zap.String("plugin", w.PluginName), zap.Error(err))
			}
		}()
		<-w.WorkerService.Ready
		logger.L().Info("Worker service ready for plugin", zap.String("plugin", w.PluginName), zap.String("socket", w.SocketPath))
	}

	if w.Language == "go" {
		// Native build orchestrator: go build
		buildCmd := exec.CommandContext(ctx, "go", "build", "-o", "tmp_bin", "main.go")
		buildCmd.Dir = w.Dir
		buildCmd.Stdout = os.Stdout
		buildCmd.Stderr = os.Stderr

		logger.L().Info("Building worker...", zap.String("plugin", w.PluginName))
		if err := buildCmd.Run(); err != nil {
			return fmt.Errorf("build failed for %s/%s: %w", w.Namespace, w.PluginName, err)
		}

		// Spawn
		binPath := filepath.Join(w.Dir, "tmp_bin")
		w.Cmd = exec.CommandContext(ctx, binPath)
		w.Cmd.Dir = w.Dir
		w.Cmd.Stdout = os.Stdout
		w.Cmd.Stderr = os.Stderr
		w.Cmd.Env = append(os.Environ(), fmt.Sprintf("HEDDLE_WORKER_ADDRESS=unix://%s", w.SocketPath))

		logger.L().Info("Spawning worker process...", zap.String("plugin", w.PluginName))
		if err := w.Cmd.Start(); err != nil {
			return fmt.Errorf("failed to start process for %s/%s: %w", w.Namespace, w.PluginName, err)
		}
	} else {
		logger.L().Warn("Language not supported for hot-reload yet", zap.String("language", w.Language))
	}

	return nil
}

func (w *WorkerProcess) Stop() error {
	w.mu.Lock()
	defer w.mu.Unlock()

	if w.Cmd != nil && w.Cmd.Process != nil {
		logger.L().Info("Gracefully stopping plugin process...", zap.String("plugin", w.PluginName))
		if err := w.Cmd.Process.Signal(syscall.SIGTERM); err != nil {
			w.Cmd.Process.Kill()
		}
		w.Cmd.Wait()
		w.Cmd = nil
	}
	return nil
}

type Maestro struct {
	ControlPlane *control_plane.ControlPlaneServer
	Workers      map[string]*WorkerProcess
	Watcher      *fsnotify.Watcher
	SocketPath   string
}

func NewMaestro() (*Maestro, error) {
	watcher, err := fsnotify.NewWatcher()
	if err != nil {
		return nil, fmt.Errorf("failed to create fsnotify watcher: %w", err)
	}

	return &Maestro{
		ControlPlane: control_plane.NewControlPlaneServer(),
		Workers:      make(map[string]*WorkerProcess),
		Watcher:      watcher,
		SocketPath:   "/tmp/heddle-local-cp.sock",
	}, nil
}

func (m *Maestro) Run(ctx context.Context) error {
	defer m.Watcher.Close()

	cpAddr := fmt.Sprintf("unix://%s", m.SocketPath)

	// Start Control Plane
	go func() {
		if err := m.ControlPlane.Listen(cpAddr); err != nil {
			logger.L().Fatal("Control plane failed", zap.Error(err))
		}
	}()

	<-m.ControlPlane.Ready
	logger.L().Info("Local Control Plane started", zap.String("address", cpAddr))

	// Scan workers/ recursively for worker.toml
	if err := m.scanWorkers(); err != nil {
		return fmt.Errorf("failed to scan workers: %w", err)
	}

	// Start all initial workers
	for _, worker := range m.Workers {
		if err := worker.Start(ctx, cpAddr); err != nil {
			logger.L().Error("Worker start failed", zap.Error(err))
		}

		// Watch the directory
		if err := m.watchRecursive(worker.Dir); err != nil {
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
				// Don't trigger on tmp_bin
				if strings.HasSuffix(event.Name, "tmp_bin") {
					continue
				}

				// Find which worker this belongs to
				worker := m.findWorkerForPath(event.Name)
				if worker != nil {
					key := worker.Namespace + "/" + worker.PluginName
					if timer, exists := debounceTimers[key]; exists {
						timer.Stop()
					}
					
					debounceTimers[key] = time.AfterFunc(500*time.Millisecond, func() {
						logger.L().Info("File changed, hot-reloading worker...", zap.String("file", event.Name), zap.String("plugin", worker.PluginName))
						worker.Stop()
						if err := worker.Start(ctx, cpAddr); err != nil {
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
	workersDir := "workers"
	if _, err := os.Stat(workersDir); os.IsNotExist(err) {
		return nil
	}

	return filepath.Walk(workersDir, func(path string, info os.FileInfo, err error) error {
		if err != nil {
			return err
		}
		if !info.IsDir() && info.Name() == "worker.toml" {
			dir := filepath.Dir(path)
			rel, _ := filepath.Rel(workersDir, dir)
			parts := strings.Split(rel, string(os.PathSeparator))
			if len(parts) == 2 {
				namespace := parts[0]
				pluginName := parts[1]
				
				// Read worker.toml to find runtime, for now assume go
				content, err := os.ReadFile(path)
				language := "go" // default
				if err == nil {
					if strings.Contains(string(content), "runtime = \"python\"") {
						language = "python"
					}
				}

				worker := &WorkerProcess{
					Namespace:  namespace,
					PluginName: pluginName,
					Language:   language,
					Dir:        dir,
					SocketPath: m.SocketPath,
				}
				m.Workers[dir] = worker
			}
		}
		return nil
	})
}

func (m *Maestro) findWorkerForPath(path string) *WorkerProcess {
	for dir, worker := range m.Workers {
		if strings.HasPrefix(path, dir) {
			return worker
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
	for _, worker := range m.Workers {
		worker.Stop()
	}
	return nil
}

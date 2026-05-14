package dev

import (
	"fmt"
	"os"
	"os/exec"
	"path/filepath"
	"regexp"
	"strings"

	"go.uber.org/zap"

	"github.com/galgotech/heddle-lang/pkg/logger"
)

var namespaceRegex = regexp.MustCompile(`^[a-z0-9_]+/[a-z0-9_]+$`)

type ScaffoldService struct{}

func NewScaffoldService() *ScaffoldService {
	return &ScaffoldService{}
}

func (s *ScaffoldService) Init(projectName string) error {
	dirs := []string{
		"workflows",
		"workers",
	}

	for _, dir := range dirs {
		if err := os.MkdirAll(dir, 0755); err != nil {
			return err
		}
	}

	workflowPath := filepath.Join("workflows", "helloworld.he")
	workflowContent := `import "std/io" io

workflow hello_world {
  []
    | io.print
}
`
	if err := os.WriteFile(workflowPath, []byte(workflowContent), 0644); err != nil {
		return err
	}

	heddleTomlPath := "heddle.toml"
	heddleTomlContent := fmt.Sprintf(`[project]
name = "%s"
version = "0.1.0"

[dev]
workers_dir = "workers"
`, projectName)
	if err := os.WriteFile(heddleTomlPath, []byte(heddleTomlContent), 0644); err != nil {
		return err
	}

	logger.L().Info("Heddle project initialized successfully", zap.String("project", projectName), zap.String("workflow", workflowPath))
	return nil
}

func (s *ScaffoldService) WorkerAdd(language, fullName string) error {
	if !namespaceRegex.MatchString(fullName) {
		return fmt.Errorf("invalid worker name '%s': must follow <namespace>/<worker_name> format with lowercase alphanumeric characters and underscores", fullName)
	}

	parts := strings.Split(fullName, "/")
	namespace := parts[0]
	workerName := parts[1]

	baseDir := filepath.Join("workers", namespace, workerName)

	dirs := []string{
		filepath.Join(baseDir, "steps"),
		filepath.Join(baseDir, "configs"),
		filepath.Join(baseDir, "resources"),
		filepath.Join(baseDir, "tests"),
	}

	for _, dir := range dirs {
		if err := os.MkdirAll(dir, 0755); err != nil {
			return fmt.Errorf("failed to create directory %s: %w", dir, err)
		}
	}

	workerTomlPath := filepath.Join(baseDir, "worker.toml")
	workerTomlContent := fmt.Sprintf(`[worker]
name = "%s"
namespace = "%s"
runtime = "%s"
sdk_version = "0.1.0"
`, workerName, namespace, language)

	if err := os.WriteFile(workerTomlPath, []byte(workerTomlContent), 0644); err != nil {
		return fmt.Errorf("failed to write worker.toml: %w", err)
	}

	if language == "go" {
		mainGoPath := filepath.Join(baseDir, "main.go")
		mainGoContent := fmt.Sprintf(`package main

import (
	"os"

	"go.uber.org/zap"

	"github.com/galgotech/heddle-lang/pkg/logger"
	"github.com/galgotech/heddle-lang/sdk/go/plugin"
)

func main() {
	p := plugin.New("%s")

	if addr := os.Getenv("HEDDLE_WORKER_ADDRESS"); addr != "" {
		p.WorkerAddress = addr
	}

	// Register your steps here:
	// p.RegisterStep("my_step", steps.MyStep)

	logger.L().Info("Starting worker...", zap.String("namespace", "%s"))
	if err := p.Start(); err != nil {
		logger.L().Fatal("Worker failed", zap.Error(err))
	}
}
`, namespace, namespace)

		if err := os.WriteFile(mainGoPath, []byte(mainGoContent), 0644); err != nil {
			return fmt.Errorf("failed to write main.go: %w", err)
		}

		// Initialize go module
		modName := fmt.Sprintf("%s/%s", namespace, workerName)
		goModCmd := exec.Command("go", "mod", "init", modName)
		goModCmd.Dir = baseDir
		goModCmd.Stdout = os.Stdout
		goModCmd.Stderr = os.Stderr
		if err := goModCmd.Run(); err != nil {
			return fmt.Errorf("failed to run go mod init: %w", err)
		}

		// Run go mod tidy in the background
		go func() {
			tidyCmd := exec.Command("go", "mod", "tidy")
			tidyCmd.Dir = baseDir
			tidyCmd.Run()
		}()
	}

	logger.L().Info("Worker scaffolded successfully", zap.String("path", baseDir))
	return nil
}

func (s *ScaffoldService) WorkerValidate() (int, error) {
	workersDir := "workers"
	if _, err := os.Stat(workersDir); os.IsNotExist(err) {
		logger.L().Warn("Workers directory not found", zap.String("dir", workersDir))
		return 0, nil
	}

	validCount := 0
	err := filepath.Walk(workersDir, func(path string, info os.FileInfo, err error) error {
		if err != nil {
			return err
		}

		if !info.IsDir() && info.Name() == "worker.toml" {
			dir := filepath.Dir(path)
			rel, _ := filepath.Rel(workersDir, dir)
			
			parts := strings.Split(rel, string(os.PathSeparator))
			if len(parts) != 2 {
				logger.L().Error("Invalid worker directory depth", zap.String("path", dir))
				return nil
			}
			
			namespace := parts[0]
			workerName := parts[1]
			
			fullName := fmt.Sprintf("%s/%s", namespace, workerName)
			if !namespaceRegex.MatchString(fullName) {
				logger.L().Error("Invalid worker name format", zap.String("name", fullName))
				return nil
			}
			
			validCount++
		}
		return nil
	})

	if err != nil {
		return 0, fmt.Errorf("failed to walk workers directory: %w", err)
	}

	logger.L().Info("Validation complete", zap.Int("valid_workers", validCount))
	return validCount, nil
}

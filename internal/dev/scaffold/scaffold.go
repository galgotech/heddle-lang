package scaffold

import (
	"bytes"
	"fmt"
	"os"
	"path/filepath"
	"regexp"
	"strings"
	"text/template"

	"github.com/galgotech/heddle-lang/pkg/logger"
)

var namespaceRegex = regexp.MustCompile(`^[a-z0-9_]+/[a-z0-9_]+$`)

type ScaffoldService struct{}

func NewScaffoldService() *ScaffoldService {
	return &ScaffoldService{}
}

func (s *ScaffoldService) Init(projectName string) error {
	dirs := []string{
		"flows",
		"workers",
		"tests",
	}

	for _, dir := range dirs {
		if err := os.MkdirAll(dir, 0755); err != nil {
			return err
		}
	}

	workflowPath := filepath.Join("flows", "helloworld.he")
	workflowContent, err := templatesFS.ReadFile("templates/init/helloworld.he.tmpl")
	if err != nil {
		return fmt.Errorf("failed to read helloworld.he template: %w", err)
	}
	if err := os.WriteFile(workflowPath, workflowContent, 0644); err != nil {
		return err
	}

	heddleTomlPath := "heddle.toml"
	tmpl, err := template.ParseFS(templatesFS, "templates/init/heddle.toml.tmpl")
	if err != nil {
		return fmt.Errorf("failed to parse heddle.toml template: %w", err)
	}

	var buf bytes.Buffer
	data := struct{ ProjectName string }{ProjectName: projectName}
	if err := tmpl.Execute(&buf, data); err != nil {
		return fmt.Errorf("failed to execute heddle.toml template: %w", err)
	}

	if err := os.WriteFile(heddleTomlPath, buf.Bytes(), 0644); err != nil {
		return err
	}

	logger.L().Info("Heddle project initialized successfully", logger.String("project", projectName), logger.String("workflow", workflowPath))
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

	// Create base directory
	if err := os.MkdirAll(baseDir, 0755); err != nil {
		return fmt.Errorf("failed to create worker directory %s: %w", baseDir, err)
	}

	workerTomlPath := filepath.Join(baseDir, "worker.toml")
	workerTomlTmpl, err := template.ParseFS(templatesFS, "templates/worker.toml.tmpl")
	if err != nil {
		return fmt.Errorf("failed to parse worker.toml template: %w", err)
	}

	var workerTomlBuf bytes.Buffer
	workerData := struct {
		WorkerName string
		Namespace  string
		Language   string
	}{
		WorkerName: workerName,
		Namespace:  namespace,
		Language:   language,
	}
	if err := workerTomlTmpl.Execute(&workerTomlBuf, workerData); err != nil {
		return fmt.Errorf("failed to execute worker.toml template: %w", err)
	}

	if err := os.WriteFile(workerTomlPath, workerTomlBuf.Bytes(), 0644); err != nil {
		return fmt.Errorf("failed to write worker.toml: %w", err)
	}

	if language == "go" {
		if err := s.scaffoldGoWorker(baseDir, namespace, workerName); err != nil {
			return err
		}
	}

	logger.L().Info("Worker scaffolded successfully", logger.String("path", baseDir))
	return nil
}

func (s *ScaffoldService) WorkerValidate() (int, error) {
	workersDir := "workers"
	if _, err := os.Stat(workersDir); os.IsNotExist(err) {
		logger.L().Warn("Workers directory not found", logger.String("dir", workersDir))
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
				logger.L().Error("Invalid worker directory depth", logger.String("path", dir))
				return nil
			}

			namespace := parts[0]
			workerName := parts[1]

			fullName := fmt.Sprintf("%s/%s", namespace, workerName)
			if !namespaceRegex.MatchString(fullName) {
				logger.L().Error("Invalid worker name format", logger.String("name", fullName))
				return nil
			}

			validCount++
		}
		return nil
	})

	if err != nil {
		return 0, fmt.Errorf("failed to walk workers directory: %w", err)
	}

	logger.L().Info("Validation complete", logger.Int("valid_workers", validCount))
	return validCount, nil
}

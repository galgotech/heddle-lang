package scaffold

import (
	"bytes"
	"fmt"
	"os"
	"path/filepath"
	"text/template"

	"github.com/galgotech/heddle-lang/pkg/logger"
)

type ScaffoldService struct{}

func NewScaffoldService() *ScaffoldService {
	return &ScaffoldService{}
}

func (s *ScaffoldService) Init(projectName string) error {
	dirs := []string{
		"flows",
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

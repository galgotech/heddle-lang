package dev

import (
	"bytes"
	"fmt"
	"os"
	"os/exec"
	"path/filepath"
	"text/template"
)

func (s *ScaffoldService) scaffoldGoWorker(baseDir, namespace, workerName string) error {
	modName := fmt.Sprintf("%s/%s", namespace, workerName)
	goData := struct {
		ModName   string
		Namespace string
	}{
		ModName:   modName,
		Namespace: namespace,
	}

	// 1. Create main.go
	mainGoPath := filepath.Join(baseDir, "main.go")
	mainGoTmpl, err := template.ParseFS(templatesFS, "templates/go/main.go.tmpl")
	if err != nil {
		return fmt.Errorf("failed to parse main.go template: %w", err)
	}
	var mainGoBuf bytes.Buffer
	if err := mainGoTmpl.Execute(&mainGoBuf, goData); err != nil {
		return fmt.Errorf("failed to execute main.go template: %w", err)
	}
	if err := os.WriteFile(mainGoPath, mainGoBuf.Bytes(), 0644); err != nil {
		return fmt.Errorf("failed to write main.go: %w", err)
	}

	// 2. Create config/config.go
	configPath := filepath.Join(baseDir, "config", "config.go")
	configContent, err := templatesFS.ReadFile("templates/go/config.go.tmpl")
	if err != nil {
		return fmt.Errorf("failed to read config.go template: %w", err)
	}
	if err := os.WriteFile(configPath, configContent, 0644); err != nil {
		return fmt.Errorf("failed to write config/config.go: %w", err)
	}

	// 3. Create steps/helloworld.go
	stepsPath := filepath.Join(baseDir, "steps", "helloworld.go")
	stepsTmpl, err := template.ParseFS(templatesFS, "templates/go/steps.go.tmpl")
	if err != nil {
		return fmt.Errorf("failed to parse steps.go template: %w", err)
	}
	var stepsBuf bytes.Buffer
	if err := stepsTmpl.Execute(&stepsBuf, goData); err != nil {
		return fmt.Errorf("failed to execute steps.go template: %w", err)
	}
	if err := os.WriteFile(stepsPath, stepsBuf.Bytes(), 0644); err != nil {
		return fmt.Errorf("failed to write steps/helloworld.go: %w", err)
	}

	// 4. Create resource/resource.go
	resourcePath := filepath.Join(baseDir, "resource", "resource.go")
	resourceContent, err := templatesFS.ReadFile("templates/go/resource.go.tmpl")
	if err != nil {
		return fmt.Errorf("failed to read resource.go template: %w", err)
	}
	if err := os.WriteFile(resourcePath, resourceContent, 0644); err != nil {
		return fmt.Errorf("failed to write resource/resource.go: %w", err)
	}

	// Initialize go module
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

	return nil
}

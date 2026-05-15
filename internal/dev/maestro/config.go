package maestro

import (
	"fmt"

	"github.com/spf13/viper"
)

type WorkerConfig struct {
	Worker struct {
		Name      string `mapstructure:"name"`
		Namespace string `mapstructure:"namespace"`
		Runtime   string `mapstructure:"runtime"`
	} `mapstructure:"worker"`
}

func (c *WorkerConfig) Validate() error {
	if c.Worker.Namespace == "" {
		return fmt.Errorf("missing namespace")
	}
	if c.Worker.Name == "" {
		return fmt.Errorf("missing name")
	}
	if c.Worker.Runtime == "" {
		return fmt.Errorf("missing runtime")
	}
	return nil
}

func LoadWorkerConfig(path string) (*WorkerConfig, error) {
	v := viper.New()
	v.SetConfigFile(path)
	if err := v.ReadInConfig(); err != nil {
		return nil, err
	}

	var cfg WorkerConfig
	if err := v.Unmarshal(&cfg); err != nil {
		return nil, err
	}

	if err := cfg.Validate(); err != nil {
		return nil, err
	}

	return &cfg, nil
}

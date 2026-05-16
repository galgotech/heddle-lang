package config

import (
	"os"
	"path/filepath"
	"strings"

	"github.com/spf13/viper"
)

type LogConfig struct {
	Level  string `mapstructure:"level"`
	Format string `mapstructure:"format"`
}

// HeddleConfig represents the unified configuration for Heddle components.
type HeddleConfig struct {
	Log          LogConfig          `mapstructure:"log"`
	Client       ClientConfig       `mapstructure:"client"`
	ControlPlane ControlPlaneConfig `mapstructure:"control-plane"`
	Worker       WorkerConfig       `mapstructure:"worker"`
}

type ClientConfig struct {
	Log      LogConfig      `mapstructure:"log"`
	Mode     string         `mapstructure:"mode"`
	Target   string         `mapstructure:"target"`
	Workflow WorkflowConfig `mapstructure:"workflow"`
}

type WorkflowConfig struct {
	Timeout string `mapstructure:"timeout"`
}

type ControlPlaneConfig struct {
	Log    LogConfig `mapstructure:"log"`
	Target string    `mapstructure:"target"`
}

type WorkerConfig struct {
	Log    LogConfig `mapstructure:"log"`
	Target string    `mapstructure:"target"`
}

// Init initializes the configuration using viper for a specific component.
// It sets the environment prefix, config file, and reads the configuration.
func Init(prefix string, cfgFile string) error {
	viper.SetEnvPrefix(prefix)
	viper.SetEnvKeyReplacer(strings.NewReplacer("-", "_"))
	viper.AutomaticEnv()

	if cfgFile != "" {
		viper.SetConfigFile(cfgFile)
	}

	if err := viper.ReadInConfig(); err != nil {
		if _, ok := err.(viper.ConfigFileNotFoundError); !ok {
			return err
		}
	}

	return nil
}

// LoadHeddleConfig loads the unified heddle.yaml configuration.
func LoadHeddleConfig(cfgFile string) (*HeddleConfig, error) {
	v := viper.New()
	v.SetEnvPrefix("HEDDLE")
	v.SetEnvKeyReplacer(strings.NewReplacer("-", "_", ".", "_"))
	v.AutomaticEnv()

	v.SetDefault("log.level", "info")

	if cfgFile != "" {
		v.SetConfigFile(cfgFile)
	} else {
		v.SetConfigName("heddle.yaml")
		v.SetConfigType("yaml")

		// 1. Search in binary directory
		if exePath, err := os.Executable(); err == nil {
			v.AddConfigPath(filepath.Dir(exePath))
		}

		// 2. Search in current directory
		v.AddConfigPath(".")

		// 3. Search in $HOME/.heddle/
		if home, err := os.UserHomeDir(); err == nil {
			v.AddConfigPath(filepath.Join(home, ".heddle"))
		}
	}

	if err := v.ReadInConfig(); err != nil {
		if _, ok := err.(viper.ConfigFileNotFoundError); !ok {
			return nil, err
		}
	}

	var cfg HeddleConfig
	if err := v.Unmarshal(&cfg); err != nil {
		return nil, err
	}

	return &cfg, nil
}

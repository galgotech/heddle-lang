package config

import (
	"strings"

	"github.com/spf13/viper"
)

// Init initializes the configuration using viper.
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

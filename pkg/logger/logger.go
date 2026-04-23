package logger

import (
	"go.uber.org/zap"
	"go.uber.org/zap/zapcore"
)

var globalLogger *zap.Logger

func init() {
	// Initialize with a sane default for development
	config := zap.NewDevelopmentConfig()
	config.EncoderConfig.EncodeLevel = zapcore.CapitalColorLevelEncoder

	logger, err := config.Build()
	if err != nil {
		// Fallback to basic production logger if development build fails
		logger = zap.NewNop()
	}
	globalLogger = logger
}

// Config defines the configuration for the logger
type Config struct {
	Development bool
	Level       string
	OutputPaths []string
}

// Init initializes the global logger with the provided configuration
func Init(cfg Config) error {
	var zapCfg zap.Config
	if cfg.Development {
		zapCfg = zap.NewDevelopmentConfig()
		zapCfg.EncoderConfig.EncodeLevel = zapcore.CapitalColorLevelEncoder
	} else {
		zapCfg = zap.NewProductionConfig()
	}

	if cfg.Level != "" {
		var level zapcore.Level
		if err := level.UnmarshalText([]byte(cfg.Level)); err == nil {
			zapCfg.Level = zap.NewAtomicLevelAt(level)
		}
	}

	if len(cfg.OutputPaths) > 0 {
		zapCfg.OutputPaths = cfg.OutputPaths
	}

	logger, err := zapCfg.Build()
	if err != nil {
		return err
	}

	globalLogger = logger
	return nil
}

// L returns the global logger instance
func L() *zap.Logger {
	return globalLogger
}

// S returns the global sugared logger instance
func S() *zap.SugaredLogger {
	return globalLogger.Sugar()
}

// Sync flushes any buffered log entries
func Sync() error {
	return globalLogger.Sync()
}

// Field helpers to avoid importing zap everywhere
func String(key, val string) zap.Field {
	return zap.String(key, val)
}

func Int(key string, val int) zap.Field {
	return zap.Int(key, val)
}

func Float64(key string, val float64) zap.Field {
	return zap.Float64(key, val)
}

func Error(err error) zap.Field {
	return zap.Error(err)
}

func Any(key string, val interface{}) zap.Field {
	return zap.Any(key, val)
}

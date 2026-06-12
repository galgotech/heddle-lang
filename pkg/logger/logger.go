package logger

import (
	"go.uber.org/zap"
	"go.uber.org/zap/zapcore"
)

// Logger defines the interface for structured logging.
type Logger interface {
	Debug(msg string, fields ...Field)
	Info(msg string, fields ...Field)
	Warn(msg string, fields ...Field)
	Error(msg string, fields ...Field)
	Fatal(msg string, fields ...Field)
	With(fields ...Field) Logger
	Sync() error
}

// Field represents a key-value pair to add context to a log entry.
type Field struct {
	zapField zap.Field
}

type zapLogger struct {
	l *zap.Logger
}

func (z *zapLogger) Debug(msg string, fields ...Field) {
	z.l.Debug(msg, toZapFields(fields)...)
}

func (z *zapLogger) Info(msg string, fields ...Field) {
	z.l.Info(msg, toZapFields(fields)...)
}

func (z *zapLogger) Warn(msg string, fields ...Field) {
	z.l.Warn(msg, toZapFields(fields)...)
}

func (z *zapLogger) Error(msg string, fields ...Field) {
	z.l.Error(msg, toZapFields(fields)...)
}

func (z *zapLogger) Fatal(msg string, fields ...Field) {
	z.l.Fatal(msg, toZapFields(fields)...)
}

func (z *zapLogger) With(fields ...Field) Logger {
	return &zapLogger{l: z.l.With(toZapFields(fields)...)}
}

func (z *zapLogger) Sync() error {
	return z.l.Sync()
}

func toZapFields(fields []Field) []zap.Field {
	if len(fields) == 0 {
		return nil
	}
	zf := make([]zap.Field, len(fields))
	for i, f := range fields {
		zf[i] = f.zapField
	}
	return zf
}

var globalLogger Logger

func init() {
	// Initialize with a sane default for development
	config := zap.NewDevelopmentConfig()
	config.EncoderConfig.EncodeLevel = zapcore.CapitalColorLevelEncoder

	logger, err := config.Build(zap.AddCallerSkip(1))
	if err != nil {
		// Fallback to basic production logger if development build fails
		globalLogger = NewNop()
	} else {
		globalLogger = &zapLogger{l: logger}
	}
}

// Config defines the configuration for the logger
type Config struct {
	Development bool
	Level       string
	Encoding    string // "json" or "console"
	OutputPaths []string
}

// Init initializes the global logger with the provided configuration
func Init(cfg Config) error {
	var zapCfg zap.Config
	if cfg.Development {
		zapCfg = zap.NewDevelopmentConfig()
	} else {
		zapCfg = zap.NewProductionConfig()
	}

	if cfg.Encoding != "" {
		zapCfg.Encoding = cfg.Encoding
	}

	// Apply beautiful console formatting if console is selected (or development mode)
	if zapCfg.Encoding == "console" || cfg.Development {
		zapCfg.Encoding = "aligned-console"
		zapCfg.EncoderConfig.EncodeLevel = alignedColorLevelEncoder
		zapCfg.EncoderConfig.EncodeTime = zapcore.ISO8601TimeEncoder
		zapCfg.EncoderConfig.EncodeDuration = zapcore.StringDurationEncoder
		zapCfg.EncoderConfig.EncodeCaller = alignedCallerEncoder
	} else if zapCfg.Encoding == "console-plain" {
		zapCfg.Encoding = "aligned-console"
		zapCfg.EncoderConfig.EncodeLevel = alignedCapitalLevelEncoder
		zapCfg.EncoderConfig.EncodeTime = zapcore.ISO8601TimeEncoder
		zapCfg.EncoderConfig.EncodeDuration = zapcore.StringDurationEncoder
		zapCfg.EncoderConfig.EncodeCaller = alignedCallerEncoder
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

	logger, err := zapCfg.Build(zap.AddCallerSkip(1))
	if err != nil {
		return err
	}

	globalLogger = &zapLogger{l: logger}
	zap.ReplaceGlobals(logger)
	return nil
}

// L returns the global logger instance
func L() Logger {
	return globalLogger
}

// NewNop returns a no-op logger instance
func NewNop() Logger {
	return &zapLogger{l: zap.NewNop()}
}

// Sync flushes any buffered log entries
func Sync() error {
	return globalLogger.Sync()
}

// Field helpers to avoid importing zap everywhere
func String(key, val string) Field {
	return Field{zapField: zap.String(key, val)}
}

func Strings(key string, val []string) Field {
	return Field{zapField: zap.Strings(key, val)}
}

func Int(key string, val int) Field {
	return Field{zapField: zap.Int(key, val)}
}

func Int64(key string, val int64) Field {
	return Field{zapField: zap.Int64(key, val)}
}

func Float64(key string, val float64) Field {
	return Field{zapField: zap.Float64(key, val)}
}

func Error(err error) Field {
	return Field{zapField: zap.Error(err)}
}

func Any(key string, val any) Field {
	return Field{zapField: zap.Any(key, val)}
}

// Standard keys for fields to ensure project-wide consistency
const (
	FieldComponent  = "component"
	FieldTraceID    = "trace_id"
	FieldTaskID     = "task_id"
	FieldWorkerID   = "worker_id"
	FieldClientID   = "client_id"
	FieldNamespace  = "namespace"
	FieldCapability = "capability"
)

// Component creates a component tag field.
func Component(val string) Field {
	return String(FieldComponent, val)
}

// TraceID creates a trace_id field to correlate asynchronous flows and workflow tasks.
func TraceID(val string) Field {
	return String(FieldTraceID, val)
}

// TaskID creates a task_id field for individual execution step tasks.
func TaskID(val string) Field {
	return String(FieldTaskID, val)
}

// WorkerID creates a worker_id field identifying the target worker node.
func WorkerID(val string) Field {
	return String(FieldWorkerID, val)
}

// ClientID creates a client_id field identifying the origin client node.
func ClientID(val string) Field {
	return String(FieldClientID, val)
}

// Namespace creates a namespace field for plugin namespaces.
func Namespace(val string) Field {
	return String(FieldNamespace, val)
}

// Capability creates a capability field indicating a specific supported plugin function.
func Capability(val string) Field {
	return String(FieldCapability, val)
}


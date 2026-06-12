package logger

import (
	"fmt"
	"strings"

	"go.uber.org/zap"
	"go.uber.org/zap/buffer"
	"go.uber.org/zap/zapcore"
)

func init() {
	_ = zap.RegisterEncoder("aligned-console", func(cfg zapcore.EncoderConfig) (zapcore.Encoder, error) {
		return &alignedConsoleEncoder{Encoder: zapcore.NewConsoleEncoder(cfg)}, nil
	})
}

type alignedConsoleEncoder struct {
	zapcore.Encoder
}

func (ace *alignedConsoleEncoder) EncodeEntry(ent zapcore.Entry, fields []zapcore.Field) (*buffer.Buffer, error) {
	const targetWidth = 45
	msg := ent.Message
	if len(msg) < targetWidth {
		ent.Message = msg + strings.Repeat(" ", targetWidth-len(msg))
	}
	return ace.Encoder.EncodeEntry(ent, fields)
}

func (ace *alignedConsoleEncoder) Clone() zapcore.Encoder {
	return &alignedConsoleEncoder{Encoder: ace.Encoder.Clone()}
}

func alignedColorLevelEncoder(l zapcore.Level, enc zapcore.PrimitiveArrayEncoder) {
	switch l {
	case zapcore.DebugLevel:
		enc.AppendString("\x1b[35mDEBUG\x1b[0m")
	case zapcore.InfoLevel:
		enc.AppendString("\x1b[34mINFO \x1b[0m")
	case zapcore.WarnLevel:
		enc.AppendString("\x1b[33mWARN \x1b[0m")
	case zapcore.ErrorLevel:
		enc.AppendString("\x1b[31mERROR\x1b[0m")
	case zapcore.DPanicLevel:
		enc.AppendString("\x1b[31mDPNIC\x1b[0m")
	case zapcore.PanicLevel:
		enc.AppendString("\x1b[31mPANIC\x1b[0m")
	case zapcore.FatalLevel:
		enc.AppendString("\x1b[31mFATAL\x1b[0m")
	default:
		enc.AppendString(fmt.Sprintf("\x1b[31m%-5s\x1b[0m", l.CapitalString()))
	}
}

func alignedCapitalLevelEncoder(l zapcore.Level, enc zapcore.PrimitiveArrayEncoder) {
	switch l {
	case zapcore.DebugLevel:
		enc.AppendString("DEBUG")
	case zapcore.InfoLevel:
		enc.AppendString("INFO ")
	case zapcore.WarnLevel:
		enc.AppendString("WARN ")
	case zapcore.ErrorLevel:
		enc.AppendString("ERROR")
	case zapcore.DPanicLevel:
		enc.AppendString("DPNIC")
	case zapcore.PanicLevel:
		enc.AppendString("PANIC")
	case zapcore.FatalLevel:
		enc.AppendString("FATAL")
	default:
		enc.AppendString(fmt.Sprintf("%-5s", l.CapitalString()))
	}
}

func alignedCallerEncoder(caller zapcore.EntryCaller, enc zapcore.PrimitiveArrayEncoder) {
	path := caller.TrimmedPath()
	const targetWidth = 35
	if len(path) > targetWidth {
		path = path[len(path)-targetWidth:]
	} else {
		path = path + strings.Repeat(" ", targetWidth-len(path))
	}
	enc.AppendString(path)
}

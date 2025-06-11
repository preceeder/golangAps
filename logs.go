package golangAps

import (
	"context"
	"log/slog"
)

type ApsLog interface {
	Info(ctx context.Context, msg string, data ...any)
	Error(ctx context.Context, msg string, data ...any)
	Debug(ctx context.Context, msg string, data ...any)
	Warn(ctx context.Context, msg string, data ...any)
}

type DefaultApsLog struct {
	EchoLevel slog.Level // 	LevelDebug Level = -4
	// LevelInfo  Level = 0
	// LevelWarn  Level = 4
	// LevelError Level = 8
}

func (l DefaultApsLog) Debug(ctx context.Context, msg string, data ...any) {
	if l.EchoLevel <= slog.LevelDebug {
		slog.DebugContext(ctx, msg, data...)
	}
}
func (l DefaultApsLog) Info(ctx context.Context, msg string, data ...any) {
	if l.EchoLevel <= slog.LevelInfo {
		slog.InfoContext(ctx, msg, data...)
	}
}

func (l DefaultApsLog) Warn(ctx context.Context, msg string, data ...any) {
	if l.EchoLevel <= slog.LevelWarn {
		slog.WarnContext(ctx, msg, data...)
	}
}
func (l DefaultApsLog) Error(ctx context.Context, msg string, data ...any) {
	if l.EchoLevel <= slog.LevelError {
		slog.ErrorContext(ctx, msg, data...)
	}
}

var DefaultLog ApsLog = DefaultApsLog{}

// SetDefaultLog
func SetDefaultLog(l ApsLog) {
	DefaultLog = l
}

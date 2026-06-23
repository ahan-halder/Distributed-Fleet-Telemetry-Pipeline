package observability

import (
	"context"

	"github.com/grpc-ecosystem/go-grpc-middleware/v2/interceptors/logging"
	"go.uber.org/zap"
)

// InterceptorLogger adapts zap logger to grpc-middleware logging interceptor
func InterceptorLogger() logging.Logger {
	return logging.LoggerFunc(func(ctx context.Context, lvl logging.Level, msg string, fields ...any) {
		f := make([]zap.Field, 0, len(fields)/2)
		for i := 0; i < len(fields); i += 2 {
			key := fields[i].(string)
			value := fields[i+1]
			f = append(f, zap.Any(key, value))
		}

		logger := zap.L()
		switch lvl {
		case logging.LevelDebug:
			logger.Debug(msg, f...)
		case logging.LevelInfo:
			logger.Info(msg, f...)
		case logging.LevelWarn:
			logger.Warn(msg, f...)
		case logging.LevelError:
			logger.Error(msg, f...)
		default:
			logger.Info(msg, f...)
		}
	})
}

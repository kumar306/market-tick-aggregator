package logger

import (
	"fmt"
	"log/slog"
	"os"
)

var Log = slog.New(slog.NewJSONHandler(os.Stdout, &slog.HandlerOptions{
	Level: slog.LevelInfo,
}))

func LogAndWrap(msg string, err error, attrs ...any) error {
	if err != nil {
		attrs = append(attrs, "err", err)
	}
	Log.Error(msg, attrs...)

	if err != nil {
		return fmt.Errorf("%s: %w", msg, err)
	} else {
		return fmt.Errorf("%s", msg)
	}
}

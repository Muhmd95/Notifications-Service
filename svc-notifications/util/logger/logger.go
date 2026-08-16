package logger

import (
	"context"
	"os"

	"github.com/rs/zerolog"
	"go.opentelemetry.io/otel/trace"
)

// Log is the globally accessible logger instance
var Log zerolog.Logger

func InitLogger(serviceName string) {
	// console writer for human-readable output
	consoleWriter := zerolog.ConsoleWriter{
		Out:        os.Stdout,
		TimeFormat: "2006-01-02 15:04:05",
	}

	// creae the logger instance with the console writer and timestamp
	Log = zerolog.New(consoleWriter).With().Timestamp().Str("service", serviceName).Logger()
}

// Ctx extracts the OpenTelemetry Trace ID from the context and attaches it to the logger.
// this is for the handlers to know which req is beaing processed by its trace id and span id
func Ctx(ctx context.Context) zerolog.Logger {
	spanContext := trace.SpanFromContext(ctx).SpanContext() // pass the contetx of the req

	// If the context doesn't have an active trace, just return the standard logger
	if !spanContext.IsValid() {
		return Log
	}

	// Attach both the Trace ID (the full request journey) and Span ID (this specific service's step)
	return Log.With().
		Str("trace_id", spanContext.TraceID().String()).
		Str("span_id", spanContext.SpanID().String()).
		Logger()
}

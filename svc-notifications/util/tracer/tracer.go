package tracer

import (
	"go.opentelemetry.io/otel"
	"go.opentelemetry.io/otel/propagation"
	sdktrace "go.opentelemetry.io/otel/sdk/trace"
)

// intialize the tracer locally only without connecting to kibana
// i will pass the service name to the func to know which service this tracer belongs
func InitTracer(serviceName string) (*sdktrace.TracerProvider, error) {
	tp := sdktrace.NewTracerProvider() // i will leave the exporter empty for now
	// i will use kibana later

	otel.SetTracerProvider(tp)
	// this registers the global tracee so that when the otelhttp
	// want to create a span it will use this tracer provider

	// 2. CRITICAL: Set the global propagator so otelhttp knows how to
	// extract and inject Trace IDs into HTTP headers across your services.
	otel.SetTextMapPropagator(propagation.TraceContext{})
	return tp, nil
}

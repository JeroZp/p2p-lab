package common

import (
	"context"
	"fmt"

	"go.opentelemetry.io/otel"
	"go.opentelemetry.io/otel/exporters/otlp/otlptrace/otlptracehttp"
	"go.opentelemetry.io/otel/sdk/resource"
	sdktrace "go.opentelemetry.io/otel/sdk/trace"
	semconv "go.opentelemetry.io/otel/semconv/v1.21.0"
	"go.opentelemetry.io/otel/trace"
	"go.opentelemetry.io/otel/trace/noop"
)

// Tracer is the lgobal tracer for p2p-lab.
// Each package calls Tracer.Start() to create spans.
var Tracer trace.Tracer

// InitTracing sets up the OpenTelemetry SDK and connects to
// an OTLP
func InitTracing(ctx context.Context, nodeName string, endpoint string) (func(context.Context) error, error) {
	exporter, err := otlptracehttp.New(ctx,
		otlptracehttp.WithEndpoint(endpoint),
		otlptracehttp.WithInsecure(), // no TLS for local dev
	)
	if err != nil {
		return nil, fmt.Errorf("create OTLP exporter: %w", err)
	}

	res, err := resource.New(ctx,
		resource.WithAttributes(
			semconv.ServiceName("p2p-lab-node"),
			semconv.ServiceInstanceID(nodeName),
		),
	)
	if err != nil {
		return nil, fmt.Errorf("creates resource: %w", err)
	}

	provider := sdktrace.NewTracerProvider(
		sdktrace.WithBatcher(exporter),
		sdktrace.WithResource(res),
	)

	otel.SetTracerProvider(provider)
	Tracer = otel.Tracer("p2p-lab")
	return provider.Shutdown, nil
}

func init() {
	Tracer = noop.NewTracerProvider().Tracer("p2p-lab")
}

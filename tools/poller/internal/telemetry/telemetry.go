// Package telemetry wires the poller's OpenTelemetry TracerProvider to
// Logfire's OTLP/HTTP endpoint. No Logfire-branded Go SDK exists (Logfire's
// Go guidance is "bring your own OpenTelemetry SDK"), so this is stock
// go.opentelemetry.io/otel + otlptracehttp; Logfire is reached purely via
// the OTEL_EXPORTER_OTLP_* env vars documented in docs/observability.md and
// PLAN.md ("Observability (Logfire + OpenTelemetry)").
package telemetry

import (
	"context"
	"fmt"

	"go.opentelemetry.io/otel"
	"go.opentelemetry.io/otel/exporters/otlp/otlptrace/otlptracehttp"
	"go.opentelemetry.io/otel/sdk/resource"
	sdktrace "go.opentelemetry.io/otel/sdk/trace"
	semconv "go.opentelemetry.io/otel/semconv/v1.26.0"
)

// Init configures the global TracerProvider to export spans via OTLP/HTTP,
// reading endpoint/headers from OTEL_EXPORTER_OTLP_ENDPOINT and
// OTEL_EXPORTER_OTLP_HEADERS. serviceName is set explicitly as a resource
// attribute (rather than relying solely on OTEL_SERVICE_NAME) so each
// binary is unambiguous in Logfire regardless of how it's launched.
//
// The returned shutdown func MUST be called before the process exits (e.g.
// via defer) -- the exporter batches spans in the background, and a
// short-lived `--once` run that skips this drops its last spans silently.
func Init(ctx context.Context, serviceName string) (shutdown func(context.Context) error, err error) {
	exporter, err := otlptracehttp.New(ctx)
	if err != nil {
		return nil, fmt.Errorf("telemetry: creating OTLP exporter: %w", err)
	}

	res, err := resource.Merge(
		resource.Default(),
		resource.NewSchemaless(semconv.ServiceName(serviceName)),
	)
	if err != nil {
		return nil, fmt.Errorf("telemetry: building resource: %w", err)
	}

	tp := sdktrace.NewTracerProvider(
		sdktrace.WithBatcher(exporter),
		sdktrace.WithResource(res),
	)
	otel.SetTracerProvider(tp)

	return tp.Shutdown, nil
}

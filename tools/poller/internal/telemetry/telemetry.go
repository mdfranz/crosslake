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
	"os/exec"
	"strings"

	"go.opentelemetry.io/otel"
	"go.opentelemetry.io/otel/attribute"
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

	// service.version/deployment.environment power Logfire's Services page
	// (RED metrics) and let error rate/latency be compared across commits --
	// directly useful given how many fixes have landed in quick succession.
	// Go's own VCS build-info stamping (runtime/debug.ReadBuildInfo) turned
	// up empty in this git worktree (module root isn't the repo root the
	// way VCS auto-stamping expects), so this shells out to `git`, matching
	// tools/compare/compare/report.py's existing pattern for the same data.
	attrs := []attribute.KeyValue{
		semconv.ServiceName(serviceName),
		// deployment.environment.name (not semconv's DeploymentEnvironment,
		// which is the older "deployment.environment" key): the OTEL
		// semantic conventions renamed this attribute, and the Python
		// Logfire SDK already emits the new name (confirmed by inspecting
		// its resource attributes directly) -- semconv v1.26.0 here doesn't
		// have the renamed helper yet (added in v1.27.0), so this is
		// spelled out explicitly rather than bumping the whole semconv
		// import just for one attribute. Using the old key would split
		// crosslake-poller from the two Python services in Logfire's
		// Services page filtering, since they'd disagree on the attribute
		// name for the same concept.
		attribute.String("deployment.environment.name", "local"), // vs. a future "cloud" mode -- see PLAN.md
	}
	if sha := gitSHA(); sha != "" {
		attrs = append(attrs, semconv.ServiceVersion(sha))
	}

	res, err := resource.Merge(
		resource.Default(),
		resource.NewSchemaless(attrs...),
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

// gitSHA returns the short current commit hash, or "" outside a git
// checkout or if git isn't on PATH. Best-effort: telemetry should never
// fail to initialize just because version metadata isn't available.
func gitSHA() string {
	out, err := exec.Command("git", "rev-parse", "--short", "HEAD").Output()
	if err != nil {
		return ""
	}
	return strings.TrimSpace(string(out))
}

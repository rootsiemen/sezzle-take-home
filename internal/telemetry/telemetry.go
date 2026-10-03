// Package telemetry configures the optional OpenTelemetry trace exporter.
package telemetry

import (
	"context"
	"fmt"
	"net/http"
	"net/url"
	"strings"
	"time"

	"go.opentelemetry.io/contrib/instrumentation/net/http/otelhttp"
	"go.opentelemetry.io/otel"
	"go.opentelemetry.io/otel/attribute"
	"go.opentelemetry.io/otel/exporters/otlp/otlptrace/otlptracehttp"
	"go.opentelemetry.io/otel/propagation"
	"go.opentelemetry.io/otel/sdk/resource"
	sdktrace "go.opentelemetry.io/otel/sdk/trace"
)

const defaultServiceName = "weatherlookup"

// NewTracerProvider returns nil when tracing is disabled by an empty endpoint.
// Endpoints may be specified as http(s) URLs; a bare host:port is treated as
// an insecure HTTP endpoint for convenient in-cluster configuration.
func NewTracerProvider(ctx context.Context, endpoint, serviceName string) (*sdktrace.TracerProvider, error) {
	endpoint = strings.TrimSpace(endpoint)
	if endpoint == "" {
		return nil, nil
	}
	if serviceName == "" {
		serviceName = defaultServiceName
	}
	if !strings.Contains(endpoint, "://") {
		endpoint = "http://" + endpoint
	}
	parsed, err := url.Parse(endpoint)
	if err != nil || parsed.Host == "" || (parsed.Scheme != "http" && parsed.Scheme != "https") {
		return nil, fmt.Errorf("invalid OTLP endpoint %q", endpoint)
	}
	if parsed.Path == "" || parsed.Path == "/" {
		parsed.Path = "/v1/traces"
		endpoint = parsed.String()
	}

	exporterOptions := []otlptracehttp.Option{
		otlptracehttp.WithEndpointURL(endpoint),
		otlptracehttp.WithTimeout(2 * time.Second),
	}
	if parsed.Scheme == "http" {
		exporterOptions = append(exporterOptions, otlptracehttp.WithInsecure())
	}
	exporter, err := otlptracehttp.New(ctx, exporterOptions...)
	if err != nil {
		return nil, fmt.Errorf("create OTLP trace exporter: %w", err)
	}

	provider := sdktrace.NewTracerProvider(
		sdktrace.WithBatcher(exporter),
		sdktrace.WithResource(resource.NewWithAttributes(
			"",
			attribute.String("service.name", serviceName),
			attribute.String("service.version", "1.0.0"),
		)),
	)
	otel.SetTracerProvider(provider)
	otel.SetTextMapPropagator(propagation.TraceContext{})
	return provider, nil
}

// HTTPHandler adds server spans when tracing is enabled.
func HTTPHandler(next http.Handler, provider *sdktrace.TracerProvider, serviceNames ...string) http.Handler {
	if provider == nil {
		return next
	}
	serviceName := defaultServiceName
	if len(serviceNames) > 0 && serviceNames[0] != "" {
		serviceName = serviceNames[0]
	}
	return otelhttp.NewHandler(next, serviceName)
}

// HTTPTransport instruments outbound HTTP requests as child spans.
func HTTPTransport(provider *sdktrace.TracerProvider) http.RoundTripper {
	if provider == nil {
		return http.DefaultTransport
	}
	return otelhttp.NewTransport(http.DefaultTransport)
}

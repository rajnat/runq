package observability

import (
	"context"
	"log"
	"net/http"
	"net/url"
	"strings"

	"go.opentelemetry.io/otel"
	"go.opentelemetry.io/otel/attribute"
	"go.opentelemetry.io/otel/exporters/otlp/otlptrace/otlptracehttp"
	"go.opentelemetry.io/otel/propagation"
	"go.opentelemetry.io/otel/sdk/resource"
	sdktrace "go.opentelemetry.io/otel/sdk/trace"
	"go.opentelemetry.io/otel/trace"
)

func InitTracing(ctx context.Context, logger *log.Logger, serviceName, endpoint string) (func(context.Context) error, error) {
	otel.SetTextMapPropagator(propagation.NewCompositeTextMapPropagator(
		propagation.TraceContext{},
		propagation.Baggage{},
	))

	res, err := resource.Merge(resource.Default(), resource.NewSchemaless(
		attribute.String("service.name", serviceName),
	))
	if err != nil {
		return nil, err
	}

	if strings.TrimSpace(endpoint) == "" {
		tp := sdktrace.NewTracerProvider(
			sdktrace.WithSampler(sdktrace.NeverSample()),
			sdktrace.WithResource(res),
		)
		otel.SetTracerProvider(tp)
		return tp.Shutdown, nil
	}

	options := []otlptracehttp.Option{
		otlptracehttp.WithInsecure(),
	}

	if parsed, err := url.Parse(endpoint); err == nil && parsed.Host != "" {
		options = append(options, otlptracehttp.WithEndpoint(parsed.Host))
		if parsed.Path != "" && parsed.Path != "/" {
			options = append(options, otlptracehttp.WithURLPath(parsed.Path))
		}
	} else {
		options = append(options, otlptracehttp.WithEndpoint(strings.TrimSpace(endpoint)))
	}

	exporter, err := otlptracehttp.New(ctx, options...)
	if err != nil {
		return nil, err
	}

	tp := sdktrace.NewTracerProvider(
		sdktrace.WithBatcher(exporter),
		sdktrace.WithSampler(sdktrace.ParentBased(sdktrace.TraceIDRatioBased(1))),
		sdktrace.WithResource(res),
	)
	otel.SetTracerProvider(tp)
	logger.Printf("tracing enabled service=%s endpoint=%s", serviceName, endpoint)
	return tp.Shutdown, nil
}

func Tracer(name string) trace.Tracer {
	return otel.Tracer(name)
}

func Extract(ctx context.Context, header http.Header) context.Context {
	return otel.GetTextMapPropagator().Extract(ctx, propagation.HeaderCarrier(header))
}

func Inject(ctx context.Context, header http.Header) {
	otel.GetTextMapPropagator().Inject(ctx, propagation.HeaderCarrier(header))
}

package observability

import (
	"context"
	"io"
	"log"
	"net/http"
	"testing"

	"go.opentelemetry.io/otel/trace"
)

func TestInitTracingNeverSampleStillPropagatesHeaders(t *testing.T) {
	shutdown, err := InitTracing(context.Background(), log.New(io.Discard, "", 0), "runq-test", "")
	if err != nil {
		t.Fatalf("init tracing: %v", err)
	}
	defer shutdown(context.Background())

	ctx, span := Tracer("test").Start(context.Background(), "roundtrip")
	defer span.End()

	headers := http.Header{}
	Inject(ctx, headers)
	if headers.Get("traceparent") == "" {
		t.Fatal("expected traceparent header after inject")
	}

	extracted := Extract(context.Background(), headers)
	spanCtx := span.SpanContext()
	extractedCtx := trace.SpanContextFromContext(extracted)
	if !extractedCtx.IsValid() {
		t.Fatal("expected extracted span context to be valid")
	}
	if extractedCtx.TraceID() != spanCtx.TraceID() {
		t.Fatalf("expected trace id %s, got %s", spanCtx.TraceID(), extractedCtx.TraceID())
	}
}

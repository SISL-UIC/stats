package otlp

import (
	"context"
	"net"
	"strings"
	"sync"
	"testing"
	"time"

	collectormetricspb "go.opentelemetry.io/proto/otlp/collector/metrics/v1"
	metricspb "go.opentelemetry.io/proto/otlp/metrics/v1"
	"google.golang.org/grpc"

	"github.com/segmentio/stats/v5"
)

// captureCollector is an in-process OTLP/gRPC metrics collector that records
// every ExportMetricsServiceRequest it receives. It lets the tests assert on
// what actually arrives over the wire rather than on internal handler state.
type captureCollector struct {
	collectormetricspb.UnimplementedMetricsServiceServer

	mu       sync.Mutex
	requests []*collectormetricspb.ExportMetricsServiceRequest
	received chan struct{} // signalled on every Export
}

func (c *captureCollector) Export(ctx context.Context, req *collectormetricspb.ExportMetricsServiceRequest) (*collectormetricspb.ExportMetricsServiceResponse, error) {
	c.mu.Lock()
	c.requests = append(c.requests, req)
	c.mu.Unlock()
	select {
	case c.received <- struct{}{}:
	default:
	}
	return &collectormetricspb.ExportMetricsServiceResponse{}, nil
}

// metrics flattens every metric across all captured requests.
func (c *captureCollector) metrics() []*metricspb.Metric {
	c.mu.Lock()
	defer c.mu.Unlock()
	var out []*metricspb.Metric
	for _, req := range c.requests {
		for _, rm := range req.GetResourceMetrics() {
			for _, sm := range rm.GetScopeMetrics() {
				out = append(out, sm.GetMetrics()...)
			}
		}
	}
	return out
}

// startCaptureCollector starts a gRPC OTLP collector on a loopback address and
// returns the collector plus the "host:port" endpoint it listens on.
func startCaptureCollector(t *testing.T) (*captureCollector, string) {
	t.Helper()

	lis, err := net.Listen("tcp", "127.0.0.1:0")
	if err != nil {
		t.Fatalf("failed to listen: %v", err)
	}

	coll := &captureCollector{received: make(chan struct{}, 16)}
	srv := grpc.NewServer()
	collectormetricspb.RegisterMetricsServiceServer(srv, coll)

	go func() { _ = srv.Serve(lis) }()
	t.Cleanup(srv.Stop)

	return coll, lis.Addr().String()
}

// waitForExport blocks until the collector has received at least one request or
// the deadline elapses.
func (c *captureCollector) waitForExport(t *testing.T, timeout time.Duration) {
	t.Helper()
	select {
	case <-c.received:
	case <-time.After(timeout):
		t.Fatal("timed out waiting for the collector to receive an export")
	}
}

// TestSDKHandler_ExportsOverGRPC drives a measure through the handler to a real
// in-process gRPC collector and asserts on what arrives over the wire. This is
// the end-to-end behavior check that the unit tests (which only inspect
// handler.instruments) cannot provide.
func TestSDKHandler_ExportsOverGRPC(t *testing.T) {
	coll, endpoint := startCaptureCollector(t)

	ctx := context.Background()
	handler, err := NewSDKHandler(ctx, SDKConfig{
		Protocol: ProtocolGRPC,
		// The exporter applies OTEL_EXPORTER_OTLP_INSECURE etc. from env, but an
		// http:// scheme on the endpoint selects an insecure connection directly.
		EndpointURL:    "http://" + endpoint,
		ExportInterval: 50 * time.Millisecond,
	})
	if err != nil {
		t.Fatalf("failed to create handler: %v", err)
	}
	defer handler.Shutdown(ctx)

	handler.HandleMeasures(time.Now(), stats.Measure{
		Name:   "wire.test",
		Fields: []stats.Field{stats.MakeField("count", 7, stats.Counter)},
		Tags:   []stats.Tag{{Name: "env", Value: "test"}},
	})

	handler.Flush()
	coll.waitForExport(t, 5*time.Second)

	metrics := coll.metrics()
	var found *metricspb.Metric
	for _, m := range metrics {
		if m.GetName() == "wire.test.count" {
			found = m
			break
		}
	}
	if found == nil {
		t.Fatalf("metric wire.test.count not received; got %d metrics", len(metrics))
	}

	sum := found.GetSum()
	if sum == nil {
		t.Fatalf("expected a Sum for a counter, got %v", found.GetData())
	}
	points := sum.GetDataPoints()
	if len(points) != 1 {
		t.Fatalf("expected 1 data point, got %d", len(points))
	}
	if got := points[0].GetAsInt(); got != 7 {
		t.Errorf("expected counter value 7, got %d", got)
	}

	// Assert the tag survived as an attribute.
	var sawTag bool
	for _, attr := range points[0].GetAttributes() {
		if attr.GetKey() == "env" && attr.GetValue().GetStringValue() == "test" {
			sawTag = true
		}
	}
	if !sawTag {
		t.Errorf("expected attribute env=test on the data point")
	}
}

// TestSDKHandler_ProtocolEnvVarSelectsGRPC proves that, with config.Protocol
// empty, OTEL_EXPORTER_OTLP_PROTOCOL=grpc is honored: the metric reaches our
// in-process gRPC collector. OTEL_EXPORTER_OTLP_ENDPOINT (read by the exporter
// itself, not by us) routes it there.
func TestSDKHandler_ProtocolEnvVarSelectsGRPC(t *testing.T) {
	coll, endpoint := startCaptureCollector(t)

	t.Setenv("OTEL_EXPORTER_OTLP_PROTOCOL", "grpc")
	t.Setenv("OTEL_EXPORTER_OTLP_ENDPOINT", "http://"+endpoint)

	ctx := context.Background()
	handler, err := NewSDKHandler(ctx, SDKConfig{
		// Protocol deliberately left empty so the env var decides.
		ExportInterval: 50 * time.Millisecond,
	})
	if err != nil {
		t.Fatalf("failed to create handler: %v", err)
	}
	defer handler.Shutdown(ctx)

	handler.HandleMeasures(time.Now(), stats.Measure{
		Name:   "proto.env.grpc",
		Fields: []stats.Field{stats.MakeField("count", 1, stats.Counter)},
	})
	handler.Flush()
	coll.waitForExport(t, 5*time.Second)

	for _, m := range coll.metrics() {
		if m.GetName() == "proto.env.grpc.count" {
			return
		}
	}
	t.Fatal("metric did not arrive over gRPC despite OTEL_EXPORTER_OTLP_PROTOCOL=grpc")
}

// TestSDKHandler_MetricsProtocolEnvVarPrecedence proves the metrics-specific
// variable wins over the generic one. The generic var asks for http/protobuf,
// but the metrics-specific var asks for grpc, so the export must reach the gRPC
// collector.
func TestSDKHandler_MetricsProtocolEnvVarPrecedence(t *testing.T) {
	coll, endpoint := startCaptureCollector(t)

	t.Setenv("OTEL_EXPORTER_OTLP_PROTOCOL", "http/protobuf")
	t.Setenv("OTEL_EXPORTER_OTLP_METRICS_PROTOCOL", "grpc")
	t.Setenv("OTEL_EXPORTER_OTLP_ENDPOINT", "http://"+endpoint)

	ctx := context.Background()
	handler, err := NewSDKHandler(ctx, SDKConfig{ExportInterval: 50 * time.Millisecond})
	if err != nil {
		t.Fatalf("failed to create handler: %v", err)
	}
	defer handler.Shutdown(ctx)

	handler.HandleMeasures(time.Now(), stats.Measure{
		Name:   "proto.env.precedence",
		Fields: []stats.Field{stats.MakeField("count", 1, stats.Counter)},
	})
	handler.Flush()
	coll.waitForExport(t, 5*time.Second)

	for _, m := range coll.metrics() {
		if m.GetName() == "proto.env.precedence.count" {
			return
		}
	}
	t.Fatal("metrics-specific protocol var did not take precedence over the generic one")
}

// TestSDKHandler_InvalidProtocolEnvVar proves an unrecognized protocol value is
// rejected with an error rather than silently defaulting.
func TestSDKHandler_InvalidProtocolEnvVar(t *testing.T) {
	t.Setenv("OTEL_EXPORTER_OTLP_PROTOCOL", "http/json")

	_, err := NewSDKHandler(context.Background(), SDKConfig{})
	if err == nil {
		t.Fatal("expected an error for an unsupported OTEL_EXPORTER_OTLP_PROTOCOL value")
	}
	// The error should name the offending value and the env var it came from.
	if msg := err.Error(); !strings.Contains(msg, "http/json") ||
		!strings.Contains(msg, "OTEL_EXPORTER_OTLP_PROTOCOL") {
		t.Errorf("error should name the bad value and source env var, got: %v", err)
	}
}

// TestSDKHandler_ExplicitProtocolOverridesEnv proves config.Protocol takes
// precedence over the environment: even with an invalid env value, an explicit
// gRPC protocol is used and the export succeeds.
func TestSDKHandler_ExplicitProtocolOverridesEnv(t *testing.T) {
	coll, endpoint := startCaptureCollector(t)

	t.Setenv("OTEL_EXPORTER_OTLP_PROTOCOL", "http/json") // would error if consulted

	ctx := context.Background()
	handler, err := NewSDKHandler(ctx, SDKConfig{
		Protocol:       ProtocolGRPC, // explicit, wins over env
		EndpointURL:    "http://" + endpoint,
		ExportInterval: 50 * time.Millisecond,
	})
	if err != nil {
		t.Fatalf("explicit protocol should bypass env validation, got error: %v", err)
	}
	defer handler.Shutdown(ctx)

	handler.HandleMeasures(time.Now(), stats.Measure{
		Name:   "proto.explicit",
		Fields: []stats.Field{stats.MakeField("count", 1, stats.Counter)},
	})
	handler.Flush()
	coll.waitForExport(t, 5*time.Second)

	for _, m := range coll.metrics() {
		if m.GetName() == "proto.explicit.count" {
			return
		}
	}
	t.Fatal("explicit gRPC protocol export did not arrive")
}

package otlp

import (
	"context"
	"fmt"
	"log/slog"
	"os"
	"sync"
	"time"

	"go.opentelemetry.io/otel/attribute"
	"go.opentelemetry.io/otel/exporters/otlp/otlpmetric/otlpmetricgrpc"
	"go.opentelemetry.io/otel/exporters/otlp/otlpmetric/otlpmetrichttp"
	otelmetric "go.opentelemetry.io/otel/metric"
	sdkmetric "go.opentelemetry.io/otel/sdk/metric"
	"go.opentelemetry.io/otel/sdk/resource"

	"github.com/segmentio/stats/v5"
)

// Protocol defines the transport protocol for OTLP export.
type Protocol string

const (
	// ProtocolGRPC uses gRPC transport.
	ProtocolGRPC Protocol = "grpc"
	// ProtocolHTTPProtobuf uses HTTP with protobuf encoding.
	ProtocolHTTPProtobuf Protocol = "http/protobuf"
)

const (
	// DefaultHistogramMaxSize is the default maximum number of buckets used for
	// exponential histograms when ExponentialHistogram is enabled.
	DefaultHistogramMaxSize int32 = 160

	// DefaultHistogramMaxScale is the default maximum scale (resolution) used
	// for exponential histograms when ExponentialHistogram is enabled.
	DefaultHistogramMaxScale int32 = 20

	// resourceDetectionTimeout bounds how long defaultResource will wait for
	// host and process resource detection before giving up. Resource detection
	// should never block handler creation indefinitely.
	resourceDetectionTimeout = 10 * time.Second
)

// protocolFromEnv resolves the transport protocol from the standard
// OpenTelemetry environment variables, following the spec's precedence: the
// metrics-specific OTEL_EXPORTER_OTLP_METRICS_PROTOCOL takes priority over the
// generic OTEL_EXPORTER_OTLP_PROTOCOL. When neither is set it defaults to gRPC.
// An unrecognized value is an error, matching the behavior of the official
// autoexport package.
//
// We resolve this one variable ourselves because the otlpmetricgrpc and
// otlpmetrichttp exporters -- each tied to a single transport -- do not read
// the protocol selector themselves.
func protocolFromEnv() (Protocol, error) {
	envVar := "OTEL_EXPORTER_OTLP_METRICS_PROTOCOL"
	proto := os.Getenv(envVar)
	if proto == "" {
		envVar = "OTEL_EXPORTER_OTLP_PROTOCOL"
		proto = os.Getenv(envVar)
	}
	switch proto {
	case "":
		return ProtocolGRPC, nil
	case string(ProtocolGRPC):
		return ProtocolGRPC, nil
	case string(ProtocolHTTPProtobuf):
		return ProtocolHTTPProtobuf, nil
	default:
		return "", fmt.Errorf("invalid OTLP protocol %q from %s - should be one of %q or %q",
			proto, envVar, ProtocolGRPC, ProtocolHTTPProtobuf)
	}
}

// defaultResource builds the resource used when SDKConfig.Resource is nil. It
// starts from resource.Default() -- which supplies the service.name fallback,
// telemetry.sdk.*, and any OTEL_RESOURCE_ATTRIBUTES/OTEL_SERVICE_NAME values --
// and merges host and process detection on top. resource.New does not fold in
// Default() itself, so the merge is explicit; the detected host/process
// attributes win on any shared key.
func defaultResource(ctx context.Context) (*resource.Resource, error) {
	// Bound resource detection so a slow or unreachable detector can't hang
	// handler creation forever.
	ctx, cancel := context.WithTimeout(ctx, resourceDetectionTimeout)
	defer cancel()

	extra, err := resource.New(ctx,
		resource.WithHost(),
		resource.WithProcess(),
	)
	if err != nil {
		return nil, fmt.Errorf("failed to detect resource attributes: %w", err)
	}
	res, err := resource.Merge(resource.Default(), extra)
	if err != nil {
		return nil, fmt.Errorf("failed to merge resource attributes: %w", err)
	}
	return res, nil
}

// SDKHandler implements stats.Handler using the official OpenTelemetry SDK.
// It bridges stats metrics to OTel metrics and supports both HTTP and gRPC transports.
//
// This handler supports all standard OpenTelemetry environment variables:
//   - OTEL_EXPORTER_OTLP_ENDPOINT
//   - OTEL_EXPORTER_OTLP_PROTOCOL (grpc, http/protobuf)
//   - OTEL_EXPORTER_OTLP_HEADERS
//   - OTEL_EXPORTER_OTLP_TIMEOUT
//   - OTEL_RESOURCE_ATTRIBUTES
//   - OTEL_SERVICE_NAME
//   - And more...
//
// Example usage:
//
//	handler, err := otlp.NewSDKHandler(ctx, otlp.SDKConfig{
//	    Protocol: otlp.ProtocolGRPC,
//	    EndpointURL: "http://localhost:4317",
//	})
//	if err != nil {
//	    log.Fatal(err)
//	}
//	defer handler.Shutdown(ctx)
//	stats.Register(handler)
type SDKHandler struct {
	provider    *sdkmetric.MeterProvider
	meter       otelmetric.Meter
	shutdownCtx context.Context // Context for shutdown operations only
	mu          sync.RWMutex
	instruments map[string]instrument
}

type instrument struct {
	counter   otelmetric.Int64Counter
	gauge     otelmetric.Float64Gauge
	histogram otelmetric.Float64Histogram
}

// SDKConfig contains configuration for the OpenTelemetry SDK handler.
type SDKConfig struct {
	// Protocol specifies the transport protocol (grpc or http/protobuf).
	// If empty, the OTEL_EXPORTER_OTLP_METRICS_PROTOCOL and
	// OTEL_EXPORTER_OTLP_PROTOCOL environment variables are consulted (in that
	// order), defaulting to gRPC when neither is set. An unrecognized
	// environment value causes NewSDKHandler to return an error.
	Protocol Protocol

	// EndpointURL specifies the full OTLP endpoint URL.
	//
	// Note: this is deliberately "EndpointURL", not "Endpoint". The underlying
	// exporters expose both WithEndpoint (host:port, no scheme) and
	// WithEndpointURL (full URL with scheme), and the two are easy to confuse.
	// This handler always uses WithEndpointURL, so the value MUST include the
	// scheme (http:// or https://).
	// For gRPC: "http://localhost:4317" or "https://api.example.com:4317"
	// For HTTP: "http://localhost:4318" or "https://api.example.com:4318"
	// If empty, uses OTEL_EXPORTER_OTLP_ENDPOINT environment variable
	// or SDK defaults (http://localhost:4317 for gRPC, http://localhost:4318 for HTTP)
	EndpointURL string

	// Resource specifies the resource attributes for all metrics.
	// If nil, a resource is built from the SDK defaults: environment
	// (OTEL_RESOURCE_ATTRIBUTES, OTEL_SERVICE_NAME), telemetry SDK, host, and
	// process attributes. Cloud and Kubernetes detection is not included by
	// default; supply a Resource built with the relevant
	// go.opentelemetry.io/contrib/detectors/* packages to add it.
	Resource *resource.Resource

	// ExportInterval specifies how often to export metrics
	// If zero or not set, uses the SDK default (60 seconds)
	ExportInterval time.Duration

	// ExportTimeout specifies the maximum amount of time to wait for a single
	// export request to the server to complete. This is distinct from
	// ExportInterval, which controls how often exports happen.
	// If zero or not set, uses the SDK default (30 seconds)
	ExportTimeout time.Duration

	// HTTPOptions are additional options for HTTP protocol.
	// Only used when Protocol is ProtocolHTTPProtobuf.
	// See the available options at
	// https://pkg.go.dev/go.opentelemetry.io/otel/exporters/otlp/otlpmetric/otlpmetrichttp#Option
	HTTPOptions []otlpmetrichttp.Option

	// GRPCOptions are additional options for gRPC protocol.
	// Only used when Protocol is ProtocolGRPC.
	// See the available options at
	// https://pkg.go.dev/go.opentelemetry.io/otel/exporters/otlp/otlpmetric/otlpmetricgrpc#Option
	GRPCOptions []otlpmetricgrpc.Option

	// ExponentialHistogram enables exponential histogram aggregation for histogram metrics.
	// When true, histograms use base-2 exponential buckets which provide better accuracy
	// and lower memory overhead compared to explicit bucket histograms.
	// Default: false (uses explicit bucket histograms)
	ExponentialHistogram bool

	// ExponentialHistogramMaxSize sets the maximum number of buckets for exponential histograms.
	// Larger values provide better accuracy but use more memory.
	// Default: DefaultHistogramMaxSize (if ExponentialHistogram is true)
	// Ignored if ExponentialHistogram is false
	ExponentialHistogramMaxSize int32

	// ExponentialHistogramMaxScale sets the maximum scale (resolution) for exponential histograms.
	// Higher values provide finer bucket granularity.
	// Valid range: -10 to 20
	// Default: DefaultHistogramMaxScale (if ExponentialHistogram is true)
	// Ignored if ExponentialHistogram is false
	ExponentialHistogramMaxScale int32

	// TemporalitySelector determines the temporality (cumulative vs delta) for each instrument kind.
	// If nil, uses DefaultTemporalitySelector which returns CumulativeTemporality for all instruments.
	// This is recommended for Prometheus and most OTLP backends.
	//
	// Available selectors:
	//   - sdkmetric.DefaultTemporalitySelector: Cumulative for all (default, Prometheus-compatible)
	//   - sdkmetric.CumulativeTemporalitySelector: Cumulative for all
	//   - sdkmetric.DeltaTemporalitySelector: Delta for all
	//   - sdkmetric.LowMemoryTemporalitySelector: Delta for Counters/Histograms, Cumulative for UpDownCounters
	TemporalitySelector sdkmetric.TemporalitySelector
}

// NewSDKHandler creates a new handler using the OpenTelemetry SDK.
// It builds a resource from the SDK defaults (environment, telemetry SDK, host,
// and process attributes) and supports the standard OTEL environment variables.
func NewSDKHandler(ctx context.Context, config SDKConfig) (*SDKHandler, error) {
	// Set defaults for histogram configuration
	if config.ExponentialHistogram {
		if config.ExponentialHistogramMaxSize == 0 {
			config.ExponentialHistogramMaxSize = DefaultHistogramMaxSize
		}
		if config.ExponentialHistogramMaxScale == 0 {
			config.ExponentialHistogramMaxScale = DefaultHistogramMaxScale
		}
	}

	// Create resource if not provided.
	res := config.Resource
	if res == nil {
		var err error
		if res, err = defaultResource(ctx); err != nil {
			return nil, err
		}
	}

	// Determine the transport protocol. An explicit config value always wins;
	// otherwise we consult the OTEL_EXPORTER_OTLP_PROTOCOL environment variables
	// ourselves. The underlying otlpmetricgrpc/otlpmetrichttp exporters read all
	// the other OTEL_EXPORTER_OTLP_* variables (endpoint, headers, timeout, ...)
	// on their own, but they do NOT read the protocol selector -- that mapping
	// only exists in the autoexport package, which we do not use here.
	protocol := config.Protocol
	if protocol == "" {
		var err error
		if protocol, err = protocolFromEnv(); err != nil {
			return nil, err
		}
	}

	// Create exporter based on protocol
	var exporter sdkmetric.Exporter
	var err error

	switch protocol {
	case ProtocolGRPC:
		opts := config.GRPCOptions
		// Use WithEndpointURL to properly handle http:// scheme
		// This avoids a known bug when using WithEndpoint with http:// scheme
		if config.EndpointURL != "" {
			opts = append([]otlpmetricgrpc.Option{otlpmetricgrpc.WithEndpointURL(config.EndpointURL)}, opts...)
		}
		// Configure temporality if provided (default is cumulative, which is Prometheus-compatible)
		if config.TemporalitySelector != nil {
			opts = append(opts, otlpmetricgrpc.WithTemporalitySelector(config.TemporalitySelector))
		}
		exporter, err = otlpmetricgrpc.New(ctx, opts...)
		if err != nil {
			return nil, fmt.Errorf("failed to create gRPC exporter: %w", err)
		}

	case ProtocolHTTPProtobuf:
		opts := config.HTTPOptions
		// Use WithEndpointURL to properly handle the full URL with scheme
		if config.EndpointURL != "" {
			opts = append([]otlpmetrichttp.Option{otlpmetrichttp.WithEndpointURL(config.EndpointURL)}, opts...)
		}
		// Configure temporality if provided (default is cumulative, which is Prometheus-compatible)
		if config.TemporalitySelector != nil {
			opts = append(opts, otlpmetrichttp.WithTemporalitySelector(config.TemporalitySelector))
		}
		exporter, err = otlpmetrichttp.New(ctx, opts...)
		if err != nil {
			return nil, fmt.Errorf("failed to create HTTP exporter: %w", err)
		}

	default:
		return nil, fmt.Errorf("unsupported protocol: %q", protocol)
	}

	// Configure histogram aggregation if exponential histograms are enabled
	var providerOpts []sdkmetric.Option
	providerOpts = append(providerOpts, sdkmetric.WithResource(res))

	// Configure periodic reader with optional interval and timeout
	readerOpts := []sdkmetric.PeriodicReaderOption{}
	if config.ExportInterval > 0 {
		readerOpts = append(readerOpts, sdkmetric.WithInterval(config.ExportInterval))
	}
	if config.ExportTimeout > 0 {
		readerOpts = append(readerOpts, sdkmetric.WithTimeout(config.ExportTimeout))
	}
	providerOpts = append(providerOpts, sdkmetric.WithReader(sdkmetric.NewPeriodicReader(exporter, readerOpts...)))

	if config.ExponentialHistogram {
		// Configure exponential histogram aggregation for all histogram instruments
		view := sdkmetric.NewView(
			sdkmetric.Instrument{Kind: sdkmetric.InstrumentKindHistogram},
			sdkmetric.Stream{
				Aggregation: sdkmetric.AggregationBase2ExponentialHistogram{
					MaxSize:  config.ExponentialHistogramMaxSize,
					MaxScale: config.ExponentialHistogramMaxScale,
				},
			},
		)
		providerOpts = append(providerOpts, sdkmetric.WithView(view))
	}

	// Create meter provider with configured options
	provider := sdkmetric.NewMeterProvider(providerOpts...)

	return &SDKHandler{
		provider:    provider,
		meter:       provider.Meter("github.com/segmentio/stats"),
		shutdownCtx: ctx,
		instruments: make(map[string]instrument),
	}, nil
}

// NewSDKHandlerFromEnv creates a handler using only environment variables.
// This is the simplest way to create a handler with full OpenTelemetry support.
//
// It respects all standard OTEL environment variables including:
//   - OTEL_EXPORTER_OTLP_ENDPOINT (full URL with scheme, e.g., http://localhost:4317)
//   - OTEL_EXPORTER_OTLP_PROTOCOL (grpc or http/protobuf)
//   - OTEL_EXPORTER_OTLP_HEADERS
//   - OTEL_RESOURCE_ATTRIBUTES
//   - OTEL_SERVICE_NAME
func NewSDKHandlerFromEnv(ctx context.Context) (*SDKHandler, error) {
	// The SDK exporters will automatically read all environment variables
	return NewSDKHandler(ctx, SDKConfig{})
}

// HandleMeasures implements stats.Handler.
func (h *SDKHandler) HandleMeasures(_ time.Time, measures ...stats.Measure) {
	// Use background context for recording metrics to avoid context cancellation issues
	// The shutdownCtx is only used for shutdown operations
	ctx := context.Background()

	for _, measure := range measures {
		for _, field := range measure.Fields {
			metricName := measure.Name + "." + field.Name
			attrs := h.tagsToAttributes(measure.Tags)

			h.mu.RLock()
			inst, exists := h.instruments[metricName]
			h.mu.RUnlock()

			if !exists {
				h.mu.Lock()
				// Double-check after acquiring write lock
				inst, exists = h.instruments[metricName]
				if !exists {
					inst = h.createInstruments(h.meter, metricName, field.Type())
					h.instruments[metricName] = inst
				}
				h.mu.Unlock()
			}

			h.recordMetric(ctx, inst, field, attrs)
		}
	}
}

// createInstruments creates OTel instruments based on field type.
func (h *SDKHandler) createInstruments(meter otelmetric.Meter, name string, fieldType stats.FieldType) instrument {
	var inst instrument

	switch fieldType {
	case stats.Counter:
		counter, err := meter.Int64Counter(name)
		if err != nil {
			slog.Error("stats/otlp: failed to create counter", "name", name, "error", err)
		}
		inst.counter = counter

	case stats.Gauge:
		// Use Float64Gauge for gauges (synchronous gauge instrument)
		gauge, err := meter.Float64Gauge(name)
		if err != nil {
			slog.Error("stats/otlp: failed to create gauge", "name", name, "error", err)
		}
		inst.gauge = gauge

	case stats.Histogram:
		histogram, err := meter.Float64Histogram(name)
		if err != nil {
			slog.Error("stats/otlp: failed to create histogram", "name", name, "error", err)
		}
		inst.histogram = histogram
	}

	return inst
}

// recordMetric records a metric value to the appropriate instrument.
func (h *SDKHandler) recordMetric(ctx context.Context, inst instrument, field stats.Field, attrs []attribute.KeyValue) {
	opts := otelmetric.WithAttributes(attrs...)

	switch field.Type() {
	case stats.Counter:
		if inst.counter != nil {
			inst.counter.Add(ctx, h.valueToInt64(field.Value), opts)
		}

	case stats.Gauge:
		if inst.gauge != nil {
			// Gauges record instantaneous values directly
			inst.gauge.Record(ctx, h.valueToFloat64(field.Value), opts)
		}

	case stats.Histogram:
		if inst.histogram != nil {
			inst.histogram.Record(ctx, h.valueToFloat64(field.Value), opts)
		}
	}
}

// tagsToAttributes converts stats tags to OTel attributes.
func (h *SDKHandler) tagsToAttributes(tags []stats.Tag) []attribute.KeyValue {
	attrs := make([]attribute.KeyValue, len(tags))
	for i, tag := range tags {
		attrs[i] = attribute.String(tag.Name, tag.Value)
	}
	return attrs
}

// valueToInt64 converts stats.Value to int64 for counters.
func (h *SDKHandler) valueToInt64(v stats.Value) int64 {
	switch v.Type() {
	case stats.Bool:
		if v.Bool() {
			return 1
		}
		return 0
	case stats.Int:
		return v.Int()
	case stats.Uint:
		return int64(v.Uint())
	case stats.Float:
		return int64(v.Float())
	case stats.Duration:
		return int64(v.Duration().Nanoseconds())
	}
	return 0
}

// valueToFloat64 converts stats.Value to float64 for gauges and histograms.
func (h *SDKHandler) valueToFloat64(v stats.Value) float64 {
	switch v.Type() {
	case stats.Bool:
		if v.Bool() {
			return 1.0
		}
		return 0.0
	case stats.Int:
		return float64(v.Int())
	case stats.Uint:
		return float64(v.Uint())
	case stats.Float:
		return v.Float()
	case stats.Duration:
		return v.Duration().Seconds()
	}
	return 0.0
}

// Flush implements stats.Flusher.
func (h *SDKHandler) Flush() {
	if err := h.provider.ForceFlush(h.shutdownCtx); err != nil {
		slog.Error("stats/otlp: failed to flush", "error", err)
	}
}

// Shutdown gracefully shuts down the handler and exports any remaining metrics.
func (h *SDKHandler) Shutdown(ctx context.Context) error {
	return h.provider.Shutdown(ctx)
}

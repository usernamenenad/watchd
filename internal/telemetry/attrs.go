package telemetry

import (
	"fmt"
	"strings"

	"go.opentelemetry.io/otel/attribute"
)

// The only attribute keys a watchd instrument may carry. Each key's values
// come from configuration (source, projection) or from a closed set of
// typed enums (the rest), so every instrument's cardinality is bounded by
// configuration. Scope values, tenants, cursors, principals, and row data
// are never attributes.
const (
	KeySource     = attribute.Key("source")
	KeyProjection = attribute.Key("projection")
	KeyReason     = attribute.Key("reason")
	KeyErrorClass = attribute.Key("error_class")
	KeyState      = attribute.Key("state")
	KeyMode       = attribute.Key("mode")
)

// MetricPrefix starts the name of every watchd instrument.
const MetricPrefix = "watchd."

var allowedKeys = map[attribute.Key]bool{
	KeySource:     true,
	KeyProjection: true,
	KeyReason:     true,
	KeyErrorClass: true,
	KeyState:      true,
	KeyMode:       true,
}

// Allowed reports whether key may appear on a watchd instrument.
func Allowed(key attribute.Key) bool { return allowedKeys[key] }

// Attributes builds an attribute set for a watchd instrument. Components
// build their sets once, at construction, and reuse them on every
// measurement, so the hot path neither allocates nor builds attributes from
// free strings. A key outside the allowlist is a programming error and
// panics.
func Attributes(kvs ...attribute.KeyValue) attribute.Set {
	for _, kv := range kvs {
		if !Allowed(kv.Key) {
			panic(fmt.Sprintf("telemetry: attribute key %q is not in the allowlist", kv.Key))
		}
	}
	return attribute.NewSet(kvs...)
}

// IsWatchdMetric reports whether name belongs to a watchd instrument, as
// opposed to runtime or gRPC instrumentation with its own conventions.
func IsWatchdMetric(name string) bool { return strings.HasPrefix(name, MetricPrefix) }

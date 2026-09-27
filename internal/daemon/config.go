// Package daemon assembles one watchd process: a PostgreSQL source, the
// watch hub, and the gRPC server, with deterministic startup and shutdown.
package daemon

import (
	"bytes"
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"os"
	"path/filepath"
	"strings"
	"time"

	"sigs.k8s.io/yaml"

	"github.com/usernamenenad/watchd/internal/cdc"
	"github.com/usernamenenad/watchd/internal/server"
	"github.com/usernamenenad/watchd/internal/watch"
)

// DatabaseURLEnv names the environment variable holding the PostgreSQL URL.
// It is kept out of the configuration file so the file never holds a
// credential.
const DatabaseURLEnv = "WATCHD_DATABASE_URL"

// ErrInvalidConfig indicates configuration watchd cannot start with. Its
// messages never include the database URL.
var ErrInvalidConfig = errors.New("daemon: invalid configuration")

const defaultShutdownTimeout = 10 * time.Second

// Config is one watchd process's configuration.
type Config struct {
	// SourceID names the source. It is part of every cursor clients store,
	// so changing it makes every client resync.
	SourceID string `json:"source_id"`
	// SlotName is the PostgreSQL logical replication slot watchd owns.
	SlotName string `json:"slot_name"`
	// PublicationName is the PostgreSQL publication to stream.
	PublicationName string `json:"publication_name"`
	// ListenAddress is where the gRPC API listens, for example
	// "127.0.0.1:7070".
	ListenAddress string `json:"listen_address"`
	// Projections are the projections clients may watch, by name.
	Projections map[string]ProjectionConfig `json:"projections"`
	// ShutdownTimeout bounds a graceful shutdown, as a Go duration such as
	// "10s". It defaults to 10s.
	ShutdownTimeout Duration `json:"shutdown_timeout,omitempty"`

	// DatabaseURL comes from WATCHD_DATABASE_URL, never from the file.
	DatabaseURL string `json:"-"`
}

// ProjectionConfig describes one projection table.
type ProjectionConfig struct {
	Schema      string   `json:"schema"`
	Table       string   `json:"table"`
	ScopeColumn string   `json:"scope_column"`
	PrimaryKey  []string `json:"primary_key"`
}

// Duration is a time.Duration written as a Go duration string in JSON.
type Duration time.Duration

// UnmarshalJSON implements json.Unmarshaler.
func (d *Duration) UnmarshalJSON(data []byte) error {
	var text string
	if err := json.Unmarshal(data, &text); err != nil {
		return fmt.Errorf("duration must be a string such as \"10s\"")
	}
	parsed, err := time.ParseDuration(text)
	if err != nil {
		return fmt.Errorf("invalid duration %q", text)
	}
	*d = Duration(parsed)
	return nil
}

// LoadConfig reads the configuration file at path, takes the database URL
// from the environment, and validates the result. The file's extension
// selects its format: .json, or .yaml / .yml.
func LoadConfig(path string) (Config, error) {
	file, err := os.Open(path)
	if err != nil {
		return Config{}, fmt.Errorf("%w: %v", ErrInvalidConfig, err)
	}
	defer file.Close()

	databaseURL := os.Getenv(DatabaseURLEnv)
	switch strings.ToLower(filepath.Ext(path)) {
	case ".json":
		return ParseJSONConfig(file, databaseURL)
	case ".yaml", ".yml":
		return ParseYAMLConfig(file, databaseURL)
	default:
		return Config{}, fmt.Errorf("%w: %s: unknown configuration format; use .json, .yaml, or .yml", ErrInvalidConfig, path)
	}
}

// ParseYAMLConfig decodes and validates a YAML configuration. It has the
// same fields as the JSON form, and is converted to JSON first, so both
// forms are decoded and validated identically - unknown fields included.
func ParseYAMLConfig(r io.Reader, databaseURL string) (Config, error) {
	data, err := io.ReadAll(r)
	if err != nil {
		return Config{}, fmt.Errorf("%w: %v", ErrInvalidConfig, err)
	}
	// Strict: a repeated key is an error, not a silent override.
	converted, err := yaml.YAMLToJSONStrict(data)
	if err != nil {
		return Config{}, fmt.Errorf("%w: %v", ErrInvalidConfig, err)
	}
	return ParseJSONConfig(bytes.NewReader(converted), databaseURL)
}

// ParseJSONConfig decodes and validates a JSON configuration. Unknown fields
// and repeated keys are rejected, so a mistyped setting fails loudly instead
// of being ignored.
func ParseJSONConfig(r io.Reader, databaseURL string) (Config, error) {
	data, err := io.ReadAll(r)
	if err != nil {
		return Config{}, fmt.Errorf("%w: %v", ErrInvalidConfig, err)
	}
	// encoding/json keeps the last of two repeated keys; a repeated setting
	// is a mistake, so reject it. JSON is valid YAML, and the strict YAML
	// parser reports duplicates.
	if _, err := yaml.YAMLToJSONStrict(data); err != nil {
		return Config{}, fmt.Errorf("%w: %v", ErrInvalidConfig, err)
	}
	decoder := json.NewDecoder(bytes.NewReader(data))
	decoder.DisallowUnknownFields()
	var cfg Config
	if err := decoder.Decode(&cfg); err != nil {
		return Config{}, fmt.Errorf("%w: %v", ErrInvalidConfig, err)
	}
	if decoder.More() {
		return Config{}, fmt.Errorf("%w: trailing data after the configuration object", ErrInvalidConfig)
	}
	cfg.DatabaseURL = databaseURL
	if cfg.ShutdownTimeout == 0 {
		cfg.ShutdownTimeout = Duration(defaultShutdownTimeout)
	}
	if err := cfg.validate(); err != nil {
		return Config{}, err
	}
	return cfg, nil
}

// validate checks everything that can be checked without connecting, by
// building each component's configuration.
func (c Config) validate() error {
	switch {
	case c.DatabaseURL == "":
		return fmt.Errorf("%w: %s is not set", ErrInvalidConfig, DatabaseURLEnv)
	case c.ListenAddress == "":
		return fmt.Errorf("%w: listen_address is required", ErrInvalidConfig)
	case len(c.Projections) == 0:
		return fmt.Errorf("%w: at least one projection is required", ErrInvalidConfig)
	case c.ShutdownTimeout < 0:
		return fmt.Errorf("%w: shutdown_timeout must not be negative", ErrInvalidConfig)
	}
	if _, err := cdc.NewReader(c.readerConfig(), func(context.Context, cdc.Transaction) error { return nil }); err != nil {
		// cdc's configuration errors never include the URL.
		return fmt.Errorf("%w: source: %v", ErrInvalidConfig, err)
	}
	for name, spec := range c.projectionSpecs() {
		if err := cdc.ValidateProjectionSpec(spec); err != nil {
			return fmt.Errorf("%w: projection %q: %v", ErrInvalidConfig, name, err)
		}
	}
	if _, err := server.New(server.Config{SourceID: c.SourceID}, noWatcher{}); err != nil {
		return fmt.Errorf("%w: source_id: %v", ErrInvalidConfig, err)
	}
	return nil
}

func (c Config) readerConfig() cdc.ReaderConfig {
	return cdc.ReaderConfig{
		DatabaseURL:     c.DatabaseURL,
		SourceID:        c.SourceID,
		SlotName:        c.SlotName,
		PublicationName: c.PublicationName,
	}
}

func (c Config) projectionSpecs() map[string]cdc.ProjectionSpec {
	specs := make(map[string]cdc.ProjectionSpec, len(c.Projections))
	for name, projection := range c.Projections {
		specs[name] = cdc.ProjectionSpec{
			SourceID:    c.SourceID,
			Schema:      projection.Schema,
			Table:       projection.Table,
			ScopeColumn: projection.ScopeColumn,
			PrimaryKey:  projection.PrimaryKey,
		}
	}
	return specs
}

func (c Config) hubConfig() watch.Config {
	return watch.Config{SourceID: c.SourceID, Projections: c.projectionSpecs()}
}

// noWatcher lets validate build a server configuration without a hub.
type noWatcher struct{}

func (noWatcher) Watch(context.Context, watch.Request, func(watch.Event) error) error { return nil }

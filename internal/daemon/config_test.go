package daemon

import (
	"errors"
	"os"
	"path/filepath"
	"reflect"
	"strings"
	"testing"
	"time"
)

const secretURL = "postgres://watchd:hunter2-secret@db.internal/watchd"

const validConfig = `{
  "source_id": "local",
  "slot_name": "watchd_example",
  "publication_name": "watchd_publication",
  "listen_address": "127.0.0.1:7070",
  "projections": {
    "tenant_permissions": {
      "schema": "public",
      "table": "tenant_permissions_projection",
      "scope_column": "tenant_id",
      "primary_key": ["tenant_id", "user_id"]
    }
  }
}`

func TestParseJSONConfigAcceptsValidConfig(t *testing.T) {
	cfg, err := ParseJSONConfig(strings.NewReader(validConfig), secretURL)
	if err != nil {
		t.Fatalf("ParseJSONConfig: %v", err)
	}
	if cfg.DatabaseURL != secretURL || cfg.SourceID != "local" || len(cfg.Projections) != 1 {
		t.Fatalf("config = %+v", cfg)
	}
	if time.Duration(cfg.ShutdownTimeout) != defaultShutdownTimeout {
		t.Fatalf("shutdown timeout = %s, want default %s", time.Duration(cfg.ShutdownTimeout), defaultShutdownTimeout)
	}
}

// TestExampleConfigIsValid keeps the shipped example loadable.
func TestExampleConfigIsValid(t *testing.T) {
	file, err := os.Open("../../examples/postgres/watchd.json")
	if err != nil {
		t.Fatalf("open example: %v", err)
	}
	defer file.Close()
	if _, err := ParseJSONConfig(file, secretURL); err != nil {
		t.Fatalf("example config: %v", err)
	}
}

func TestParseJSONConfigRejectsInvalidConfigWithoutLeakingTheURL(t *testing.T) {
	for name, test := range map[string]struct {
		config string
		url    string
	}{
		"missing database URL": {config: validConfig},
		"unknown field":        {config: strings.Replace(validConfig, `"slot_name"`, `"slotname"`, 1), url: secretURL},
		"trailing data":        {config: validConfig + "{}", url: secretURL},
		"duplicate key":        {config: strings.Replace(validConfig, `"source_id": "local",`, `"source_id": "local", "source_id": "other",`, 1), url: secretURL},
		"unsafe slot name":     {config: strings.Replace(validConfig, "watchd_example", "watchd; DROP TABLE x", 1), url: secretURL},
		"scope outside key":    {config: strings.Replace(validConfig, `"scope_column": "tenant_id"`, `"scope_column": "region"`, 1), url: secretURL},
		"no listen address":    {config: strings.Replace(validConfig, "127.0.0.1:7070", "", 1), url: secretURL},
		"separator in source":  {config: strings.Replace(validConfig, `"source_id": "local"`, `"source_id": "a@b"`, 1), url: secretURL},
		"bad duration":         {config: strings.Replace(validConfig, `"source_id"`, `"shutdown_timeout": "soon", "source_id"`, 1), url: secretURL},
		"no projections":       {config: `{"source_id": "local", "slot_name": "s", "publication_name": "p", "listen_address": ":1", "projections": {}}`, url: secretURL},
	} {
		_, err := ParseJSONConfig(strings.NewReader(test.config), test.url)
		if !errors.Is(err, ErrInvalidConfig) {
			t.Errorf("%s: error = %v, want %v", name, err, ErrInvalidConfig)
			continue
		}
		if strings.Contains(err.Error(), "hunter2") {
			t.Errorf("%s: error leaks the database password: %v", name, err)
		}
	}
}

// TestYAMLAndJSONExamplesAreTheSameConfig keeps the two shipped examples in
// step, and shows both formats decode to the same configuration.
func TestYAMLAndJSONExamplesAreTheSameConfig(t *testing.T) {
	t.Setenv(DatabaseURLEnv, secretURL)
	fromJSON, err := LoadConfig("../../examples/postgres/watchd.json")
	if err != nil {
		t.Fatalf("load JSON example: %v", err)
	}
	fromYAML, err := LoadConfig("../../examples/postgres/watchd.yaml")
	if err != nil {
		t.Fatalf("load YAML example: %v", err)
	}
	if !reflect.DeepEqual(fromJSON, fromYAML) {
		t.Fatalf("YAML example = %+v, JSON example = %+v", fromYAML, fromJSON)
	}
}

func TestParseYAMLConfig(t *testing.T) {
	const valid = `
source_id: local
slot_name: watchd_example
publication_name: watchd_publication
listen_address: 127.0.0.1:7070
shutdown_timeout: 3s
projections:
  tenant_permissions:
    schema: public
    table: tenant_permissions_projection
    scope_column: tenant_id
    primary_key: [tenant_id, user_id]
`
	cfg, err := ParseYAMLConfig(strings.NewReader(valid), secretURL)
	if err != nil {
		t.Fatalf("ParseYAMLConfig: %v", err)
	}
	if time.Duration(cfg.ShutdownTimeout) != 3*time.Second || cfg.Projections["tenant_permissions"].ScopeColumn != "tenant_id" {
		t.Fatalf("config = %+v", cfg)
	}

	for name, config := range map[string]string{
		"unknown field":    strings.Replace(valid, "slot_name:", "slotname:", 1),
		"duplicate key":    valid + "source_id: other\n",
		"not a mapping":    "- source_id: local\n",
		"invalid YAML":     "source_id: [unclosed\n",
		"invalid duration": strings.Replace(valid, "3s", "soon", 1),
	} {
		if _, err := ParseYAMLConfig(strings.NewReader(config), secretURL); !errors.Is(err, ErrInvalidConfig) {
			t.Errorf("%s: error = %v, want %v", name, err, ErrInvalidConfig)
		}
	}
}

func TestLoadConfigChoosesFormatByExtension(t *testing.T) {
	t.Setenv(DatabaseURLEnv, secretURL)
	dir := t.TempDir()
	write := func(name, content string) string {
		t.Helper()
		path := filepath.Join(dir, name)
		if err := os.WriteFile(path, []byte(content), 0o600); err != nil {
			t.Fatalf("write %s: %v", name, err)
		}
		return path
	}

	for _, name := range []string{"watchd.yml", "watchd.YAML"} {
		if _, err := LoadConfig(write(name, "source_id: local\nslot_name: s\npublication_name: p\nlisten_address: ':1'\nprojections:\n  p:\n    schema: public\n    table: t\n    scope_column: tenant_id\n    primary_key: [tenant_id, id]\n")); err != nil {
			t.Errorf("%s: %v", name, err)
		}
	}
	// JSON is valid YAML, but a .json file is still read strictly as JSON.
	if _, err := LoadConfig(write("watchd.json", "source_id: local\n")); !errors.Is(err, ErrInvalidConfig) {
		t.Errorf("YAML in a .json file: error = %v, want %v", err, ErrInvalidConfig)
	}
	if _, err := LoadConfig(write("watchd.toml", validConfig)); !errors.Is(err, ErrInvalidConfig) {
		t.Errorf("unknown extension: error = %v, want %v", err, ErrInvalidConfig)
	}
}

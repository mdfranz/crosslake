package main

import (
	"fmt"
	"os"

	"gopkg.in/yaml.v3"
)

// Config is the poller's on-disk configuration shape -- see
// config.example.yaml. Real values (bucket names, account-scoped prefixes)
// live only in the gitignored config.yaml, never in config.example.yaml.
type Config struct {
	Mode string `yaml:"mode"` // local|pubsub (only "local" is implemented so far)

	AWS struct {
		Region   string `yaml:"region"`
		S3Bucket string `yaml:"s3_bucket"`
		S3Prefix string `yaml:"s3_prefix"`
	} `yaml:"aws"`

	Local struct {
		DataDir string `yaml:"data_dir"`
	} `yaml:"local"`

	CursorFile          string `yaml:"cursor_file"`
	PollIntervalSeconds int    `yaml:"poll_interval_seconds"`
}

// LoadConfig reads and validates the YAML config at path.
func LoadConfig(path string) (Config, error) {
	b, err := os.ReadFile(path)
	if err != nil {
		if os.IsNotExist(err) {
			return Config{}, fmt.Errorf("config: %s not found -- copy config.example.yaml to config.yaml and fill in your bucket/prefix", path)
		}
		return Config{}, fmt.Errorf("config: reading %s: %w", path, err)
	}

	var cfg Config
	if err := yaml.Unmarshal(b, &cfg); err != nil {
		return Config{}, fmt.Errorf("config: parsing %s: %w", path, err)
	}

	if cfg.AWS.S3Bucket == "" {
		return Config{}, fmt.Errorf("config: aws.s3_bucket is required")
	}
	if cfg.Local.DataDir == "" {
		cfg.Local.DataDir = "../../data" // repo-root ./data, assuming cwd == tools/poller
	}
	if cfg.CursorFile == "" {
		cfg.CursorFile = "./cursor.json"
	}
	if cfg.PollIntervalSeconds <= 0 {
		cfg.PollIntervalSeconds = 60
	}

	return cfg, nil
}

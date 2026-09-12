// Package cursor persists the poller's S3 progress marker to a local JSON
// file. LastKey is only safe for a closed, immutable prefix: CloudTrail can
// deliver keys out of order, and region appears before date in a trail-wide
// prefix. See docs/review-telemetry-plan.md before using polling mode.
//
// Not safe for concurrent poller instances -- see PLAN.md "Risks / gotchas".
package cursor

import (
	"encoding/json"
	"fmt"
	"os"
	"path/filepath"
)

// State is the on-disk cursor shape.
type State struct {
	LastKey string `json:"last_key"`
}

// Load reads the cursor file, returning a zero-value State (start from the
// beginning of the prefix) if the file doesn't exist yet.
func Load(path string) (State, error) {
	b, err := os.ReadFile(path)
	if err != nil {
		if os.IsNotExist(err) {
			return State{}, nil
		}
		return State{}, fmt.Errorf("cursor: reading %s: %w", path, err)
	}
	var s State
	if err := json.Unmarshal(b, &s); err != nil {
		return State{}, fmt.Errorf("cursor: parsing %s: %w", path, err)
	}
	return s, nil
}

// Save replaces the cursor atomically so interruption cannot leave a partial
// JSON document. The cursor can reveal source topology, so keep it private.
func Save(path string, s State) error {
	b, err := json.MarshalIndent(s, "", "  ")
	if err != nil {
		return fmt.Errorf("cursor: encoding state: %w", err)
	}
	dir := filepath.Dir(path)
	if dir != "." {
		if err := os.MkdirAll(dir, 0o700); err != nil {
			return fmt.Errorf("cursor: mkdir %s: %w", dir, err)
		}
	}

	tmp, err := os.CreateTemp(dir, ".cursor-*")
	if err != nil {
		return fmt.Errorf("cursor: creating temporary file for %s: %w", path, err)
	}
	tmpPath := tmp.Name()
	defer os.Remove(tmpPath)

	if err := tmp.Chmod(0o600); err != nil {
		tmp.Close()
		return fmt.Errorf("cursor: chmod temporary file for %s: %w", path, err)
	}
	if _, err := tmp.Write(append(b, '\n')); err != nil {
		tmp.Close()
		return fmt.Errorf("cursor: writing temporary file for %s: %w", path, err)
	}
	if err := tmp.Sync(); err != nil {
		tmp.Close()
		return fmt.Errorf("cursor: syncing temporary file for %s: %w", path, err)
	}
	if err := tmp.Close(); err != nil {
		return fmt.Errorf("cursor: closing temporary file for %s: %w", path, err)
	}
	if err := os.Rename(tmpPath, path); err != nil {
		return fmt.Errorf("cursor: replacing %s: %w", path, err)
	}
	dirFile, err := os.Open(dir)
	if err != nil {
		return fmt.Errorf("cursor: opening parent directory for %s: %w", path, err)
	}
	if err := dirFile.Sync(); err != nil {
		dirFile.Close()
		return fmt.Errorf("cursor: syncing parent directory for %s: %w", path, err)
	}
	if err := dirFile.Close(); err != nil {
		return fmt.Errorf("cursor: closing parent directory for %s: %w", path, err)
	}
	return nil
}

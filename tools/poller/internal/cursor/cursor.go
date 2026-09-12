// Package cursor persists the poller's S3 progress marker to a local JSON
// file. CloudTrail's AWSLogs/<acct>/CloudTrail/<region>/YYYY/MM/DD/... key
// layout is lexicographically time-ordered, so the last-processed S3 key is
// a valid resume point (via ListObjectsV2's StartAfter).
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

// Save writes the cursor file, overwriting any previous contents.
func Save(path string, s State) error {
	b, err := json.MarshalIndent(s, "", "  ")
	if err != nil {
		return fmt.Errorf("cursor: encoding state: %w", err)
	}
	if dir := filepath.Dir(path); dir != "." {
		if err := os.MkdirAll(dir, 0o755); err != nil {
			return fmt.Errorf("cursor: mkdir %s: %w", dir, err)
		}
	}
	if err := os.WriteFile(path, b, 0o644); err != nil {
		return fmt.Errorf("cursor: writing %s: %w", path, err)
	}
	return nil
}

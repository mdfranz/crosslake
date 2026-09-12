// Package cursor persists the poller's S3 progress marker to a local JSON
// file. LastKey is only safe for a closed, immutable prefix: CloudTrail can
// deliver keys out of order, and region appears before date in a trail-wide
// prefix. See docs/review-telemetry-plan.md before using polling mode.
//
// This is the legacy progress mechanism, still used for the explicitly
// unsafe continuous-loop path (--allow-unsafe-last-key-polling). The
// supported --once path uses internal/ledger instead, which tracks a
// seen-object set keyed by (bucket, key, etag) rather than a single
// lexicographic boundary -- see ledger.go's package doc for why that closes
// the out-of-order-delivery gap this cursor cannot.
//
// Not safe for concurrent poller instances -- see PLAN.md "Risks / gotchas".
package cursor

import (
	"fmt"

	"github.com/mdfranz/crosslake/tools/poller/internal/atomicfile"
)

// State is the on-disk cursor shape.
type State struct {
	LastKey string `json:"last_key"`
}

// Load reads the cursor file, returning a zero-value State (start from the
// beginning of the prefix) if the file doesn't exist yet.
func Load(path string) (State, error) {
	var s State
	if _, err := atomicfile.ReadJSON(path, &s); err != nil {
		return State{}, fmt.Errorf("cursor: %w", err)
	}
	return s, nil
}

// Save replaces the cursor atomically so interruption cannot leave a partial
// JSON document. The cursor can reveal source topology, so keep it private.
func Save(path string, s State) error {
	if err := atomicfile.WriteJSON(path, s, 0o600); err != nil {
		return fmt.Errorf("cursor: %w", err)
	}
	return nil
}

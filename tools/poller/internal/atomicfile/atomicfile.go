// Package atomicfile writes JSON documents so that a crash mid-write can
// never leave a torn/partial file behind: encode to a temp file in the same
// directory, fsync it, rename over the destination, then fsync the
// directory entry too. Extracted from tools/poller/internal/cursor, which
// had this logic inline before internal/ledger needed the identical
// guarantee -- see cursor.go and ledger.go, both now thin wrappers over
// WriteJSON.
package atomicfile

import (
	"encoding/json"
	"fmt"
	"os"
	"path/filepath"
)

// WriteJSON marshals v and atomically replaces path with the result, creating
// parent directories as needed. perm is applied to both the temp file and
// the final file, so callers can keep sensitive local state (cursors,
// ledgers -- both can reveal source topology) owner-only.
func WriteJSON(path string, v any, perm os.FileMode) error {
	b, err := json.MarshalIndent(v, "", "  ")
	if err != nil {
		return fmt.Errorf("atomicfile: encoding %s: %w", path, err)
	}

	dir := filepath.Dir(path)
	if dir != "." {
		if err := os.MkdirAll(dir, 0o700); err != nil {
			return fmt.Errorf("atomicfile: mkdir %s: %w", dir, err)
		}
	}

	tmp, err := os.CreateTemp(dir, ".atomicfile-*")
	if err != nil {
		return fmt.Errorf("atomicfile: creating temporary file for %s: %w", path, err)
	}
	tmpPath := tmp.Name()
	defer os.Remove(tmpPath) // no-op once the rename below succeeds

	if err := tmp.Chmod(perm); err != nil {
		tmp.Close()
		return fmt.Errorf("atomicfile: chmod temporary file for %s: %w", path, err)
	}
	if _, err := tmp.Write(append(b, '\n')); err != nil {
		tmp.Close()
		return fmt.Errorf("atomicfile: writing temporary file for %s: %w", path, err)
	}
	if err := tmp.Sync(); err != nil {
		tmp.Close()
		return fmt.Errorf("atomicfile: syncing temporary file for %s: %w", path, err)
	}
	if err := tmp.Close(); err != nil {
		return fmt.Errorf("atomicfile: closing temporary file for %s: %w", path, err)
	}
	if err := os.Rename(tmpPath, path); err != nil {
		return fmt.Errorf("atomicfile: replacing %s: %w", path, err)
	}

	dirFile, err := os.Open(dir)
	if err != nil {
		return fmt.Errorf("atomicfile: opening parent directory for %s: %w", path, err)
	}
	if err := dirFile.Sync(); err != nil {
		dirFile.Close()
		return fmt.Errorf("atomicfile: syncing parent directory for %s: %w", path, err)
	}
	if err := dirFile.Close(); err != nil {
		return fmt.Errorf("atomicfile: closing parent directory for %s: %w", path, err)
	}
	return nil
}

// ReadJSON reads and unmarshals the JSON document at path into v. It returns
// (false, nil) without error if path does not exist yet, so callers can
// treat a missing file as an empty/zero-value starting state.
func ReadJSON(path string, v any) (found bool, err error) {
	b, err := os.ReadFile(path)
	if err != nil {
		if os.IsNotExist(err) {
			return false, nil
		}
		return false, fmt.Errorf("atomicfile: reading %s: %w", path, err)
	}
	if err := json.Unmarshal(b, v); err != nil {
		return false, fmt.Errorf("atomicfile: parsing %s: %w", path, err)
	}
	return true, nil
}

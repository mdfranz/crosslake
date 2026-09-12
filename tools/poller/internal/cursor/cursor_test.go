package cursor

import (
	"os"
	"path/filepath"
	"testing"
)

func TestSaveLoadAndReplace(t *testing.T) {
	dir := t.TempDir()
	path := filepath.Join(dir, "state", "cursor.json")

	for _, want := range []State{{LastKey: "first"}, {LastKey: "second"}} {
		if err := Save(path, want); err != nil {
			t.Fatalf("Save(%q): %v", want.LastKey, err)
		}
		got, err := Load(path)
		if err != nil {
			t.Fatalf("Load: %v", err)
		}
		if got != want {
			t.Fatalf("Load = %#v, want %#v", got, want)
		}
	}

	info, err := os.Stat(path)
	if err != nil {
		t.Fatalf("Stat: %v", err)
	}
	if got := info.Mode().Perm(); got != 0o600 {
		t.Fatalf("cursor permissions = %o, want 600", got)
	}

	// Temp file naming moved to internal/atomicfile (".atomicfile-*") when
	// cursor.go was refactored onto it -- see atomicfile.go's package doc.
	matches, err := filepath.Glob(filepath.Join(filepath.Dir(path), ".atomicfile-*"))
	if err != nil {
		t.Fatalf("Glob: %v", err)
	}
	if len(matches) != 0 {
		t.Fatalf("temporary cursor files left behind: %v", matches)
	}
}

func TestLoadMissingIsEmpty(t *testing.T) {
	got, err := Load(filepath.Join(t.TempDir(), "missing.json"))
	if err != nil {
		t.Fatalf("Load: %v", err)
	}
	if got != (State{}) {
		t.Fatalf("Load = %#v, want empty state", got)
	}
}

func TestLoadRejectsPartialJSON(t *testing.T) {
	path := filepath.Join(t.TempDir(), "cursor.json")
	if err := os.WriteFile(path, []byte(`{"last_key":`), 0o600); err != nil {
		t.Fatalf("WriteFile: %v", err)
	}
	if _, err := Load(path); err == nil {
		t.Fatal("Load accepted partial JSON")
	}
}

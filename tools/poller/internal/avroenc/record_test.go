package avroenc

import "testing"

// TestFromJSON_CanonicalizesEscapeHatchJSON locks in the canonical-JSON fix:
// sorted keys, no HTML-escaping, compact separators -- matching the Python
// Beam pipeline's json.dumps(sort_keys=True, separators=(",", ":"),
// ensure_ascii=False) byte-for-byte (see LEARNINGS.md, item 11).
func TestFromJSON_CanonicalizesEscapeHatchJSON(t *testing.T) {
	// Keys deliberately out of alphabetical order, plus '&' and a non-ASCII
	// character, to exercise key sorting, HTML-escaping, and ASCII-escaping
	// all at once.
	raw := []byte(`{
		"eventName": "PutObject",
		"eventTime": "2026-09-12T00:00:00Z",
		"requestParameters": {"zebra": "a&b", "apple": "café"}
	}`)

	rec, err := FromJSON(raw)
	if err != nil {
		t.Fatalf("FromJSON: %v", err)
	}
	if rec.RequestParametersJSON == nil {
		t.Fatal("RequestParametersJSON is nil")
	}

	want := `{"apple":"café","zebra":"a&b"}`
	got := *rec.RequestParametersJSON
	if got != want {
		t.Errorf("RequestParametersJSON:\n got:  %s\n want: %s", got, want)
	}
}

func TestFromJSON_NullEscapeHatchFieldsStayNil(t *testing.T) {
	raw := []byte(`{"eventName": "PutObject", "eventTime": "2026-09-12T00:00:00Z"}`)

	rec, err := FromJSON(raw)
	if err != nil {
		t.Fatalf("FromJSON: %v", err)
	}
	if rec.RequestParametersJSON != nil {
		t.Errorf("RequestParametersJSON = %q, want nil", *rec.RequestParametersJSON)
	}
}

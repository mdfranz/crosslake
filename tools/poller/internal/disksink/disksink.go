// Package disksink is the Local Mode sink: it writes the two local mirrors
// of the cloud pipeline's outputs, using only the standard library plus
// hamba/avro's OCF writer -- no GCP Pub/Sub or Dataflow involved.
package disksink

import (
	"fmt"
	"os"
	"path/filepath"

	"github.com/hamba/avro/v2"
	"github.com/hamba/avro/v2/ocf"

	"github.com/mdfranz/crosslake/tools/poller/internal/avroenc"
)

// Sink appends raw JSON lines to <dataDir>/raw.jsonl (mirrors the
// cloudtrail-raw topic, consumed by the local Beam DirectRunner pipeline)
// and writes typed Avro records to <dataDir>/tier3-avro/events.avro
// (mirrors the Tier 3 GCS subscription).
type Sink struct {
	rawFile  *os.File
	avroFile *os.File
	ocfEnc   *ocf.Encoder
}

// New creates or appends to the local output files under dataDir. schema is
// the parsed cloudtrail.avsc, used as the OCF container's header schema.
func New(dataDir string, schema avro.Schema) (*Sink, error) {
	avroDir := filepath.Join(dataDir, "tier3-avro")
	if err := os.MkdirAll(avroDir, 0o755); err != nil {
		return nil, fmt.Errorf("disksink: mkdir %s: %w", avroDir, err)
	}

	rawPath := filepath.Join(dataDir, "raw.jsonl")
	rawFile, err := os.OpenFile(rawPath, os.O_APPEND|os.O_CREATE|os.O_WRONLY, 0o600)
	if err != nil {
		return nil, fmt.Errorf("disksink: open %s: %w", rawPath, err)
	}
	if err := rawFile.Chmod(0o600); err != nil {
		rawFile.Close()
		return nil, fmt.Errorf("disksink: chmod %s: %w", rawPath, err)
	}

	// ocf.NewEncoderWithSchema appends to an existing OCF file by reading its
	// header back first (see hamba/avro/v2/ocf's newEncoder), so this must
	// open O_RDWR without O_TRUNC to match raw.jsonl's append semantics --
	// otherwise every run after the first silently discards prior records,
	// leaving Tier 3 out of sync with Tier 2/raw.jsonl (a real bug hit
	// during testing: tier3 had only the latest run's 3 records while the
	// other two tiers had the full 1456).
	avroPath := filepath.Join(avroDir, "events.avro")
	avroFile, err := os.OpenFile(avroPath, os.O_CREATE|os.O_RDWR, 0o600)
	if err != nil {
		rawFile.Close()
		return nil, fmt.Errorf("disksink: open %s: %w", avroPath, err)
	}
	if err := avroFile.Chmod(0o600); err != nil {
		rawFile.Close()
		avroFile.Close()
		return nil, fmt.Errorf("disksink: chmod %s: %w", avroPath, err)
	}

	// Deflate: DuckDB's avro extension (used by tools/compare) rejects
	// zstandard-codec OCF files with "unknown codec" even though hamba/avro
	// writes valid zstandard per the Avro spec -- a genuine cross-tool
	// interop gotcha, noted in LEARNINGS.md. Deflate is universally
	// supported and still gives real compression, unlike the default
	// (uncompressed) codec.
	ocfEnc, err := ocf.NewEncoderWithSchema(schema, avroFile, ocf.WithCodec(ocf.Deflate))
	if err != nil {
		rawFile.Close()
		avroFile.Close()
		return nil, fmt.Errorf("disksink: new OCF encoder: %w", err)
	}

	return &Sink{rawFile: rawFile, avroFile: avroFile, ocfEnc: ocfEnc}, nil
}

// WriteRecord appends one record's raw JSON line and its typed Avro
// encoding. rawJSON is the single record's JSON (not the {"Records": [...]}
// blob); rec is the same record already parsed via avroenc.FromJSON.
func (s *Sink) WriteRecord(rawJSON []byte, rec *avroenc.Record) error {
	if _, err := s.rawFile.Write(append(append([]byte{}, rawJSON...), '\n')); err != nil {
		return fmt.Errorf("disksink: write raw.jsonl: %w", err)
	}
	if err := s.ocfEnc.Encode(rec); err != nil {
		return fmt.Errorf("disksink: encode avro record: %w", err)
	}
	return nil
}

// Flush makes both representations durable before the caller advances its
// source checkpoint. It does not make the pair transactional; reconciliation
// metadata is still required to detect a crash between the two writes.
func (s *Sink) Flush() error {
	if err := s.ocfEnc.Flush(); err != nil {
		return fmt.Errorf("disksink: flush ocf encoder: %w", err)
	}
	if err := s.rawFile.Sync(); err != nil {
		return fmt.Errorf("disksink: sync raw.jsonl: %w", err)
	}
	if err := s.avroFile.Sync(); err != nil {
		return fmt.Errorf("disksink: sync events.avro: %w", err)
	}
	return nil
}

// Close flushes and closes both output files.
func (s *Sink) Close() error {
	if err := s.ocfEnc.Close(); err != nil {
		return fmt.Errorf("disksink: close ocf encoder: %w", err)
	}
	if err := s.rawFile.Close(); err != nil {
		return fmt.Errorf("disksink: close raw.jsonl: %w", err)
	}
	if err := s.avroFile.Close(); err != nil {
		return fmt.Errorf("disksink: close events.avro: %w", err)
	}
	return nil
}

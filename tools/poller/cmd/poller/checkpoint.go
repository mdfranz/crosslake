package main

import (
	"context"
	"fmt"

	"github.com/mdfranz/crosslake/tools/poller/internal/cursor"
	"github.com/mdfranz/crosslake/tools/poller/internal/ledger"
	"github.com/mdfranz/crosslake/tools/poller/internal/s3source"
)

// pendingObject is one object queued for processing this pass. ETag/Size
// are known for the ledger path (from a fresh S3 listing) and zero-valued
// for the legacy cursor path, which never lists ETags -- see
// cursorCheckpointer.
type pendingObject struct {
	Key  string
	ETag string
	Size int64
}

// committedObject is a pendingObject plus how many records it produced,
// known only after processing -- this is what gets recorded at checkpoint
// time.
type committedObject struct {
	pendingObject
	RecordsWritten int
}

// checkpointer decouples runOnce from *how* progress is tracked, so the
// same fetch/parse/write loop serves both the ledger-backed --once path
// (safe, the supported default) and the legacy last-key-cursor loop path
// (--allow-unsafe-last-key-polling, unchanged and still gated -- see
// cursorCheckpointer's doc comment for why the ledger doesn't yet make
// continuous polling safe too). It also makes runOnce unit-testable without
// a real S3 client: main_test.go uses a fake checkpointer to exercise
// crash/retry behavior directly.
type checkpointer interface {
	// Pending returns objects to process this pass, in the order they must
	// be processed and checkpointed.
	Pending(ctx context.Context) ([]pendingObject, error)
	// Commit persists that every object in chunk has been durably flushed
	// to the sink (see disksink.Sink.Flush) -- called once per checkpoint
	// batch, never before that batch's Flush succeeds. A crash before
	// Commit is called simply leaves those objects unrecorded, so the next
	// Pending call includes them again: at-least-once, idempotent replay.
	Commit(chunk []committedObject) error
}

// lister is the subset of *s3source.Source that ledgerCheckpointer needs,
// so tests can supply a fake without a real S3 client.
type lister interface {
	List(ctx context.Context) ([]s3source.Object, error)
}

// ledgerCheckpointer is the supported --once path: it re-lists the whole
// prefix every run (no StartAfter boundary) and diffs against a persisted
// seen-object set keyed by (bucket, key, etag). See internal/ledger's
// package doc for the data behind why this replaces the last-key cursor.
type ledgerCheckpointer struct {
	src    lister
	bucket string
	path   string
	l      *ledger.Ledger
}

func newLedgerCheckpointer(path, bucket string, src lister) (*ledgerCheckpointer, error) {
	l, err := ledger.Load(path)
	if err != nil {
		return nil, err
	}
	return &ledgerCheckpointer{src: src, bucket: bucket, path: path, l: l}, nil
}

func (c *ledgerCheckpointer) Pending(ctx context.Context) ([]pendingObject, error) {
	objects, err := c.src.List(ctx)
	if err != nil {
		return nil, fmt.Errorf("ledger checkpointer: listing: %w", err)
	}
	var pending []pendingObject
	for _, o := range objects {
		if c.l.Seen(c.bucket, o.Key, o.ETag) {
			continue
		}
		pending = append(pending, pendingObject{Key: o.Key, ETag: o.ETag, Size: o.Size})
	}
	return pending, nil
}

func (c *ledgerCheckpointer) Commit(chunk []committedObject) error {
	for _, o := range chunk {
		c.l.Record(c.bucket, o.Key, o.ETag, o.Size, o.RecordsWritten)
	}
	if err := c.l.Save(c.path); err != nil {
		return fmt.Errorf("ledger checkpointer: %w", err)
	}
	return nil
}

// legacyLister is the subset of *s3source.Source the legacy cursor path
// needs.
type legacyLister interface {
	ListSince(ctx context.Context, startAfter string) ([]string, error)
}

// cursorCheckpointer is the legacy --allow-unsafe-last-key-polling path,
// unchanged in behavior from before the ledger existed. It is kept
// (instead of switching everything over to the ledger) because a
// seen-object set only closes the out-of-order-delivery gap for a prefix
// that gets re-listed from the start every run -- correct but wasteful
// against a large, continuously growing prefix. Making continuous polling
// both safe and cheap needs S3 event notifications/SQS plus a periodic
// reconciliation cadence (see PLAN.md "Explicit future work"), not just
// this ledger; until that lands, continuous polling stays on the old,
// explicitly-gated mechanism rather than silently relabeled "safe".
type cursorCheckpointer struct {
	src  legacyLister
	path string
	cur  cursor.State
}

func newCursorCheckpointer(path string, src legacyLister) (*cursorCheckpointer, error) {
	cur, err := cursor.Load(path)
	if err != nil {
		return nil, err
	}
	return &cursorCheckpointer{src: src, path: path, cur: cur}, nil
}

func (c *cursorCheckpointer) Pending(ctx context.Context) ([]pendingObject, error) {
	keys, err := c.src.ListSince(ctx, c.cur.LastKey)
	if err != nil {
		return nil, fmt.Errorf("cursor checkpointer: listing: %w", err)
	}
	pending := make([]pendingObject, len(keys))
	for i, k := range keys {
		pending[i] = pendingObject{Key: k}
	}
	return pending, nil
}

func (c *cursorCheckpointer) Commit(chunk []committedObject) error {
	if len(chunk) == 0 {
		return nil
	}
	c.cur.LastKey = chunk[len(chunk)-1].Key
	if err := cursor.Save(c.path, c.cur); err != nil {
		return fmt.Errorf("cursor checkpointer: %w", err)
	}
	return nil
}

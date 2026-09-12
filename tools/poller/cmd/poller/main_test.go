package main

import "testing"

func TestApplySourceOverridesRequiresDedicatedBackfillCursor(t *testing.T) {
	var cfg Config
	cfg.AWS.S3Prefix = "configured-prefix"
	cfg.CursorFile = "configured-cursor.json"

	if err := applySourceOverrides(&cfg, "closed-day-prefix", ""); err == nil {
		t.Fatal("prefix override without cursor override was accepted")
	}
	if cfg.AWS.S3Prefix != "configured-prefix" || cfg.CursorFile != "configured-cursor.json" {
		t.Fatalf("invalid overrides mutated config: %#v", cfg)
	}
}

func TestApplySourceOverridesAppliesBackfillPair(t *testing.T) {
	var cfg Config
	if err := applySourceOverrides(&cfg, "closed-day-prefix", "cursor-backfill.json"); err != nil {
		t.Fatalf("applySourceOverrides: %v", err)
	}
	if cfg.AWS.S3Prefix != "closed-day-prefix" {
		t.Fatalf("prefix = %q", cfg.AWS.S3Prefix)
	}
	if cfg.CursorFile != "cursor-backfill.json" {
		t.Fatalf("cursor = %q", cfg.CursorFile)
	}
}

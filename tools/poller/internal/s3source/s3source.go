// Package s3source lists and fetches CloudTrail objects from S3. The
// source is always real AWS S3 in both Local and Cloud modes -- see
// PLAN.md ("Dual operating modes").
package s3source

import (
	"compress/gzip"
	"context"
	"fmt"
	"io"

	"github.com/aws/aws-sdk-go-v2/aws"
	"github.com/aws/aws-sdk-go-v2/service/s3"
)

// Source lists and fetches objects under one bucket/prefix.
type Source struct {
	client *s3.Client
	bucket string
	prefix string
}

// New builds a Source from an already-configured S3 client.
func New(client *s3.Client, bucket, prefix string) *Source {
	return &Source{client: client, bucket: bucket, prefix: prefix}
}

// ListSince returns object keys under the configured prefix that sort after
// startAfter (empty lists from the beginning), in ascending lexicographic
// order. This is not a safe incremental CloudTrail inventory: delivery can be
// out of order. Callers must restrict it to closed prefixes until a seen-object
// ledger and reconciliation replace the last-key cursor.
func (s *Source) ListSince(ctx context.Context, startAfter string) ([]string, error) {
	var keys []string
	var token *string
	for {
		out, err := s.client.ListObjectsV2(ctx, &s3.ListObjectsV2Input{
			Bucket:            aws.String(s.bucket),
			Prefix:            aws.String(s.prefix),
			StartAfter:        strPtrOrNil(startAfter),
			ContinuationToken: token,
		})
		if err != nil {
			return nil, fmt.Errorf("s3source: ListObjectsV2 bucket=%s prefix=%s: %w", s.bucket, s.prefix, err)
		}
		for _, obj := range out.Contents {
			keys = append(keys, aws.ToString(obj.Key))
		}
		if !aws.ToBool(out.IsTruncated) {
			break
		}
		token = out.NextContinuationToken
	}
	return keys, nil
}

// FetchAndGunzip downloads one object and gunzips it, returning the raw
// bytes of the uncompressed CloudTrail JSON blob: {"Records": [...]}.
//
// Gotcha: each S3 object is gzip JSON of the whole array -- callers must
// unnest and process each individual record, not the blob as a whole.
func (s *Source) FetchAndGunzip(ctx context.Context, key string) ([]byte, error) {
	out, err := s.client.GetObject(ctx, &s3.GetObjectInput{
		Bucket: aws.String(s.bucket),
		Key:    aws.String(key),
	})
	if err != nil {
		return nil, fmt.Errorf("s3source: GetObject(%s): %w", key, err)
	}
	defer out.Body.Close()

	gz, err := gzip.NewReader(out.Body)
	if err != nil {
		return nil, fmt.Errorf("s3source: gzip.NewReader(%s): %w", key, err)
	}
	defer gz.Close()

	data, err := io.ReadAll(gz)
	if err != nil {
		return nil, fmt.Errorf("s3source: reading gunzipped body(%s): %w", key, err)
	}
	return data, nil
}

func strPtrOrNil(s string) *string {
	if s == "" {
		return nil
	}
	return aws.String(s)
}

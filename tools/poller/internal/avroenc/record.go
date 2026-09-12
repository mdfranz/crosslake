// Package avroenc holds the typed CloudTrail record shape (mirroring
// schema/cloudtrail.avsc field-for-field) and converts a raw CloudTrail
// JSON record into it. The same Record value is used both by the Local
// Mode disk sink (encoded via hamba/avro/v2/ocf's reflection-based writer)
// and, in Cloud Mode, would be avro.Marshal'd for a Pub/Sub payload.
//
// Field list is grounded in a real record sampled from live CloudTrail
// delivery, not the AWS docs alone -- see docs/schema-design-notes.md.
package avroenc

import (
	"bytes"
	"encoding/json"
	"fmt"
	"strings"
	"time"
)

// UserIdentity mirrors the "UserIdentity" nested record in
// schema/cloudtrail.avsc. Real samples show this varies materially by
// Type (e.g. AWSService only sets InvokedBy; IAMUser/AssumedRole carry
// ARN/AccountID/AccessKeyID) -- SessionContextJSON is the escape hatch for
// AssumedRole's deeply-nested session info.
type UserIdentity struct {
	Type               *string `avro:"type"`
	PrincipalID        *string `avro:"principalId"`
	ARN                *string `avro:"arn"`
	AccountID          *string `avro:"accountId"`
	AccessKeyID        *string `avro:"accessKeyId"`
	UserName           *string `avro:"userName"`
	InvokedBy          *string `avro:"invokedBy"`
	SessionContextJSON *string `avro:"sessionContextJson"`
}

// Record mirrors schema/cloudtrail.avsc's CloudTrailEvent.
type Record struct {
	EventVersion       *string       `avro:"eventVersion"`
	EventTime          *time.Time    `avro:"eventTime"`
	EventSource        *string       `avro:"eventSource"`
	EventName          *string       `avro:"eventName"`
	EventType          *string       `avro:"eventType"`
	EventCategory      *string       `avro:"eventCategory"`
	AWSRegion          *string       `avro:"awsRegion"`
	SourceIPAddress    *string       `avro:"sourceIPAddress"`
	UserAgent          *string       `avro:"userAgent"`
	RequestID          *string       `avro:"requestID"`
	EventID            *string       `avro:"eventID"`
	SharedEventID      *string       `avro:"sharedEventID"`
	RecipientAccountID *string       `avro:"recipientAccountId"`
	ReadOnly           *bool         `avro:"readOnly"`
	ManagementEvent    *bool         `avro:"managementEvent"`
	UserIdentity       *UserIdentity `avro:"userIdentity"`

	// Event-type-dependent nested blobs, kept as JSON-string escape
	// hatches rather than strict per-service records since their shape
	// varies by eventName/eventSource. Mirrored in
	// pipelines/parquet-writer/parquet_writer/cloudtrail_schema.py.
	RequestParametersJSON   *string `avro:"requestParametersJson"`
	ResponseElementsJSON    *string `avro:"responseElementsJson"`
	AdditionalEventDataJSON *string `avro:"additionalEventDataJson"`
	ResourcesJSON           *string `avro:"resourcesJson"`
	ServiceEventDetailsJSON *string `avro:"serviceEventDetailsJson"`
}

// rawRecord captures one CloudTrail record's raw JSON shape prior to
// typing -- the *Records[i] element, not the whole {"Records": [...]} blob.
type rawRecord struct {
	EventVersion        string          `json:"eventVersion"`
	EventTime           string          `json:"eventTime"`
	EventSource         string          `json:"eventSource"`
	EventName           string          `json:"eventName"`
	EventType           string          `json:"eventType"`
	EventCategory       string          `json:"eventCategory"`
	AWSRegion           string          `json:"awsRegion"`
	SourceIPAddress     string          `json:"sourceIPAddress"`
	UserAgent           string          `json:"userAgent"`
	RequestID           string          `json:"requestID"`
	EventID             string          `json:"eventID"`
	SharedEventID       string          `json:"sharedEventID"`
	RecipientAccountID  string          `json:"recipientAccountId"`
	ReadOnly            *bool           `json:"readOnly"`
	ManagementEvent     *bool           `json:"managementEvent"`
	UserIdentity        json.RawMessage `json:"userIdentity"`
	RequestParameters   json.RawMessage `json:"requestParameters"`
	ResponseElements    json.RawMessage `json:"responseElements"`
	AdditionalEventData json.RawMessage `json:"additionalEventData"`
	Resources           json.RawMessage `json:"resources"`
	ServiceEventDetails json.RawMessage `json:"serviceEventDetails"`
}

type rawUserIdentity struct {
	Type           string          `json:"type"`
	PrincipalID    string          `json:"principalId"`
	ARN            string          `json:"arn"`
	AccountID      string          `json:"accountId"`
	AccessKeyID    string          `json:"accessKeyId"`
	UserName       string          `json:"userName"`
	InvokedBy      string          `json:"invokedBy"`
	SessionContext json.RawMessage `json:"sessionContext"`
}

// FromJSON parses one raw CloudTrail record (a single element of the
// top-level "Records" array) into a typed Record.
func FromJSON(raw []byte) (*Record, error) {
	var rr rawRecord
	if err := json.Unmarshal(raw, &rr); err != nil {
		return nil, fmt.Errorf("avroenc: unmarshal record: %w", err)
	}

	requestParametersJSON, err := canonicalJSONPtr(rr.RequestParameters)
	if err != nil {
		return nil, fmt.Errorf("avroenc: requestParameters: %w", err)
	}
	responseElementsJSON, err := canonicalJSONPtr(rr.ResponseElements)
	if err != nil {
		return nil, fmt.Errorf("avroenc: responseElements: %w", err)
	}
	additionalEventDataJSON, err := canonicalJSONPtr(rr.AdditionalEventData)
	if err != nil {
		return nil, fmt.Errorf("avroenc: additionalEventData: %w", err)
	}
	resourcesJSON, err := canonicalJSONPtr(rr.Resources)
	if err != nil {
		return nil, fmt.Errorf("avroenc: resources: %w", err)
	}
	serviceEventDetailsJSON, err := canonicalJSONPtr(rr.ServiceEventDetails)
	if err != nil {
		return nil, fmt.Errorf("avroenc: serviceEventDetails: %w", err)
	}

	rec := &Record{
		EventVersion:            strPtr(rr.EventVersion),
		EventSource:             strPtr(rr.EventSource),
		EventName:               strPtr(rr.EventName),
		EventType:               strPtr(rr.EventType),
		EventCategory:           strPtr(rr.EventCategory),
		AWSRegion:               strPtr(rr.AWSRegion),
		SourceIPAddress:         strPtr(rr.SourceIPAddress),
		UserAgent:               strPtr(rr.UserAgent),
		RequestID:               strPtr(rr.RequestID),
		EventID:                 strPtr(rr.EventID),
		SharedEventID:           strPtr(rr.SharedEventID),
		RecipientAccountID:      strPtr(rr.RecipientAccountID),
		ReadOnly:                rr.ReadOnly,
		ManagementEvent:         rr.ManagementEvent,
		RequestParametersJSON:   requestParametersJSON,
		ResponseElementsJSON:    responseElementsJSON,
		AdditionalEventDataJSON: additionalEventDataJSON,
		ResourcesJSON:           resourcesJSON,
		ServiceEventDetailsJSON: serviceEventDetailsJSON,
	}

	if rr.EventTime != "" {
		t, err := time.Parse(time.RFC3339, rr.EventTime)
		if err != nil {
			return nil, fmt.Errorf("avroenc: parsing eventTime %q: %w", rr.EventTime, err)
		}
		rec.EventTime = &t
	}

	if len(rr.UserIdentity) > 0 && string(rr.UserIdentity) != "null" {
		var rui rawUserIdentity
		if err := json.Unmarshal(rr.UserIdentity, &rui); err != nil {
			return nil, fmt.Errorf("avroenc: unmarshal userIdentity: %w", err)
		}
		sessionContextJSON, err := canonicalJSONPtr(rui.SessionContext)
		if err != nil {
			return nil, fmt.Errorf("avroenc: userIdentity.sessionContext: %w", err)
		}
		rec.UserIdentity = &UserIdentity{
			Type:               strPtr(rui.Type),
			PrincipalID:        strPtr(rui.PrincipalID),
			ARN:                strPtr(rui.ARN),
			AccountID:          strPtr(rui.AccountID),
			AccessKeyID:        strPtr(rui.AccessKeyID),
			UserName:           strPtr(rui.UserName),
			InvokedBy:          strPtr(rui.InvokedBy),
			SessionContextJSON: sessionContextJSON,
		}
	}

	return rec, nil
}

func strPtr(s string) *string {
	if s == "" {
		return nil
	}
	return &s
}

// canonicalJSONPtr re-serializes raw into a canonical form (sorted object
// keys, compact separators, no HTML-escaping) instead of preserving the
// source's original byte-for-byte formatting. Without this, the
// escape-hatch JSON-string columns end up with different content between
// this Go path and the Python Beam pipeline's equivalent
// json.dumps(sort_keys=True, separators=(",", ":"), ensure_ascii=False) --
// a real, measured divergence (see LEARNINGS.md: full_row_materialize's
// character-count check differed by ~55,000 chars across tiers for
// otherwise-identical rows) even though both sides parse the same source
// JSON with the same logical content.
func canonicalJSONPtr(raw json.RawMessage) (*string, error) {
	if len(raw) == 0 || string(raw) == "null" {
		return nil, nil
	}
	var v any
	if err := json.Unmarshal(raw, &v); err != nil {
		return nil, fmt.Errorf("canonicalizing JSON: %w", err)
	}
	// json.Marshal sorts map[string]any keys alphabetically already; the
	// Encoder is used only to get SetEscapeHTML(false), matching Python's
	// ensure_ascii=False -- plain json.Marshal always HTML-escapes
	// '<','>','&' regardless of ensure_ascii/HTML concerns, which
	// json.dumps does not.
	var buf bytes.Buffer
	enc := json.NewEncoder(&buf)
	enc.SetEscapeHTML(false)
	if err := enc.Encode(v); err != nil {
		return nil, fmt.Errorf("canonicalizing JSON: %w", err)
	}
	s := strings.TrimRight(buf.String(), "\n")
	return &s, nil
}

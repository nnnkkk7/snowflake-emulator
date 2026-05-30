package handlers

import (
	"bytes"
	"compress/gzip"
	"encoding/json"
	"net/http"
	"net/http/httptest"
	"testing"
)

func TestParseQueryRequest_GzipBody(t *testing.T) {
	payload := []byte(`{"sqlText":"SELECT 1","describeOnly":false}`)

	var buf bytes.Buffer
	gz := gzip.NewWriter(&buf)
	if _, err := gz.Write(payload); err != nil {
		t.Fatalf("gzip write: %v", err)
	}
	if err := gz.Close(); err != nil {
		t.Fatalf("gzip close: %v", err)
	}

	req := httptest.NewRequest(http.MethodPost, "/queries/v1/query-request", bytes.NewReader(buf.Bytes()))
	req.Header.Set("Content-Encoding", "gzip")

	got, err := parseQueryRequest(req)
	if err != nil {
		t.Fatalf("parseQueryRequest() error = %v", err)
	}
	if got.SQLText != "SELECT 1" {
		t.Fatalf("SQLText = %q, want SELECT 1", got.SQLText)
	}
}

func TestParseQueryRequest_NestedData(t *testing.T) {
	body, err := json.Marshal(map[string]any{
		"data": map[string]any{
			"sqlText": "SHOW TABLES",
		},
	})
	if err != nil {
		t.Fatalf("marshal: %v", err)
	}

	req := httptest.NewRequest(http.MethodPost, "/queries/v1/query-request", bytes.NewReader(body))
	got, err := parseQueryRequest(req)
	if err != nil {
		t.Fatalf("parseQueryRequest() error = %v", err)
	}
	if got.SQLText != "SHOW TABLES" {
		t.Fatalf("SQLText = %q, want SHOW TABLES", got.SQLText)
	}
}

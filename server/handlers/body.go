package handlers

import (
	"compress/gzip"
	"encoding/json"
	"fmt"
	"io"
	"net/http"
	"strings"
)

func readRequestBody(r *http.Request) ([]byte, error) {
	defer func() { _ = r.Body.Close() }()

	if strings.EqualFold(r.Header.Get("Content-Encoding"), "gzip") {
		gz, err := gzip.NewReader(r.Body)
		if err != nil {
			return nil, fmt.Errorf("gzip decompress: %w", err)
		}
		defer func() { _ = gz.Close() }()
		return io.ReadAll(gz)
	}

	return io.ReadAll(r.Body)
}

func decodeJSONBody(r *http.Request, v any) error {
	body, err := readRequestBody(r)
	if err != nil {
		return err
	}
	if len(body) == 0 {
		return fmt.Errorf("empty body")
	}
	return json.Unmarshal(body, v)
}

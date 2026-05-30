package handlers

import (
	"encoding/json"
	"net/http"

	"github.com/nnnkkk7/snowflake-emulator/server/types"
)

func parseQueryRequest(r *http.Request) (types.QueryRequest, error) {
	body, err := readRequestBody(r)
	if err != nil {
		return types.QueryRequest{}, err
	}

	var req types.QueryRequest
	if err := json.Unmarshal(body, &req); err != nil {
		return types.QueryRequest{}, err
	}
	if req.SQLText != "" {
		return req, nil
	}

	var wrapped struct {
		Data types.QueryRequest `json:"data"`
	}
	if err := json.Unmarshal(body, &wrapped); err == nil && wrapped.Data.SQLText != "" {
		return wrapped.Data, nil
	}

	return req, nil
}

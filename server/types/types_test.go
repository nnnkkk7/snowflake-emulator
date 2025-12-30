package types

import (
	"encoding/json"
	"testing"

	"github.com/google/go-cmp/cmp"
)

func TestLoginRequestJSON(t *testing.T) {
	input := `{
		"data": {
			"CLIENT_APP_ID": "GoSnowflake",
			"CLIENT_APP_VERSION": "1.0.0",
			"ACCOUNT_NAME": "testaccount",
			"LOGIN_NAME": "testuser",
			"PASSWORD": "testpass",
			"databaseName": "TEST_DB",
			"schemaName": "PUBLIC"
		}
	}`

	var req LoginRequest
	if err := json.Unmarshal([]byte(input), &req); err != nil {
		t.Fatalf("Failed to unmarshal LoginRequest: %v", err)
	}

	if req.Data.LoginName != "testuser" {
		t.Errorf("Expected LoginName=testuser, got %s", req.Data.LoginName)
	}
	if req.Data.DatabaseName != "TEST_DB" {
		t.Errorf("Expected DatabaseName=TEST_DB, got %s", req.Data.DatabaseName)
	}
}

func TestLoginResponseJSON(t *testing.T) {
	resp := LoginResponse{
		Success: true,
		Data: &LoginSuccessData{
			Token:                   "abc123",
			MasterToken:             "def456",
			ValidityInSeconds:       3600,
			MasterValidityInSeconds: 14400,
			SessionID:               1234567890,
			Parameters: []ParameterBinding{
				{Name: "TIMEZONE", Value: "UTC"},
			},
			SessionInfo: SessionInfo{
				DatabaseName: "TEST_DB",
				SchemaName:   "PUBLIC",
			},
		},
	}

	data, err := json.Marshal(resp)
	if err != nil {
		t.Fatalf("Failed to marshal LoginResponse: %v", err)
	}

	var decoded LoginResponse
	if err := json.Unmarshal(data, &decoded); err != nil {
		t.Fatalf("Failed to unmarshal LoginResponse: %v", err)
	}

	if diff := cmp.Diff(resp, decoded); diff != "" {
		t.Errorf("LoginResponse mismatch (-want +got):\n%s", diff)
	}
}

func TestQueryRequestJSON(t *testing.T) {
	input := `{
		"sqlText": "SELECT * FROM test_table",
		"bindings": {
			"param1": "value1"
		}
	}`

	var req QueryRequest
	if err := json.Unmarshal([]byte(input), &req); err != nil {
		t.Fatalf("Failed to unmarshal QueryRequest: %v", err)
	}

	if req.SQLText != "SELECT * FROM test_table" {
		t.Errorf("Expected SQLText='SELECT * FROM test_table', got %s", req.SQLText)
	}
	if req.Bindings["param1"] != "value1" {
		t.Errorf("Expected bindings[param1]=value1, got %v", req.Bindings["param1"])
	}
}

func TestQueryResponseJSON(t *testing.T) {
	resp := QueryResponse{
		Success: true,
		Data: &QuerySuccessData{
			QueryID:         "01234567890-1234567890",
			SQLState:        "00000",
			StatementTypeID: 1,
			RowType: []ColumnMetadata{
				{Name: "ID", Type: "NUMBER", Nullable: false},
				{Name: "NAME", Type: "TEXT", Nullable: true},
			},
			RowSet: [][]string{
				{"1", "Alice"},
				{"2", "Bob"},
			},
			Total:             2,
			Returned:          2,
			QueryResultFormat: "json",
		},
	}

	data, err := json.Marshal(resp)
	if err != nil {
		t.Fatalf("Failed to marshal QueryResponse: %v", err)
	}

	var decoded QueryResponse
	if err := json.Unmarshal(data, &decoded); err != nil {
		t.Fatalf("Failed to unmarshal QueryResponse: %v", err)
	}

	if decoded.Data.QueryID != "01234567890-1234567890" {
		t.Errorf("Expected QueryID='01234567890-1234567890', got %s", decoded.Data.QueryID)
	}
	if decoded.Data.Total != 2 {
		t.Errorf("Expected Total=2, got %d", decoded.Data.Total)
	}
}

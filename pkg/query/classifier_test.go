package query

import (
	"testing"

	"github.com/nnnkkk7/snowflake-emulator/pkg/config"
)

func TestClassifier_StatementTypeIDs(t *testing.T) {
	tests := []struct {
		name            string
		sql             string
		expectedTypeID  config.StatementTypeID
		expectedIsQuery bool
		expectedIsDDL   bool
		expectedIsDML   bool
	}{
		{"SELECT", "SELECT * FROM t", config.StatementTypeSelect, true, false, false},
		{"SHOW", "SHOW TABLES", config.StatementTypeSelect, true, false, false},
		{"DESCRIBE", "DESCRIBE TABLE t", config.StatementTypeSelect, true, false, false},
		{"INSERT", "INSERT INTO t VALUES (1)", config.StatementTypeInsert, false, false, true},
		{"UPDATE", "UPDATE t SET x = 1", config.StatementTypeUpdate, false, false, true},
		{"DELETE", "DELETE FROM t WHERE id = 1", config.StatementTypeDelete, false, false, true},
		{"MERGE", "MERGE INTO t USING s ON t.id = s.id WHEN MATCHED THEN DELETE", config.StatementTypeMerge, false, false, true},
		{"COPY", "COPY INTO t FROM @stage", config.StatementTypeCopy, false, false, true},
		{"CREATE", "CREATE TABLE t (id INT)", config.StatementTypeDDL, false, true, false},
		{"DROP", "DROP TABLE t", config.StatementTypeDrop, false, true, false},
		{"ALTER", "ALTER TABLE t ADD COLUMN x INT", config.StatementTypeDDL, false, true, false},
		{"BEGIN", "BEGIN", config.StatementTypeBegin, false, false, false},
		{"START_TRANSACTION", "START TRANSACTION", config.StatementTypeBegin, false, false, false},
		{"COMMIT", "COMMIT", config.StatementTypeCommit, false, false, false},
		{"ROLLBACK", "ROLLBACK", config.StatementTypeDML, false, false, false},
	}

	classifier := NewClassifier()
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			result := classifier.Classify(tt.sql)
			if result.StatementTypeID != tt.expectedTypeID {
				t.Errorf("Classify(%q).StatementTypeID = %d, want %d", tt.sql, result.StatementTypeID, tt.expectedTypeID)
			}
			if result.IsQuery != tt.expectedIsQuery {
				t.Errorf("Classify(%q).IsQuery = %v, want %v", tt.sql, result.IsQuery, tt.expectedIsQuery)
			}
			if result.IsDDL != tt.expectedIsDDL {
				t.Errorf("Classify(%q).IsDDL = %v, want %v", tt.sql, result.IsDDL, tt.expectedIsDDL)
			}
			if result.IsDML != tt.expectedIsDML {
				t.Errorf("Classify(%q).IsDML = %v, want %v", tt.sql, result.IsDML, tt.expectedIsDML)
			}
		})
	}
}

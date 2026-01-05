package types

import (
	"testing"

	"github.com/google/go-cmp/cmp"
)

// TestSnowflakeType_ToDuckDBType tests the conversion from Snowflake types to DuckDB types.
func TestSnowflakeType_ToDuckDBType(t *testing.T) {
	tests := []struct {
		name string
		in   SnowflakeType
		want string
	}{
		// Core types
		{name: "NUMBER", in: TypeNumber, want: "DOUBLE"},
		{name: "INTEGER", in: TypeInteger, want: "BIGINT"},
		{name: "FLOAT", in: TypeFloat, want: "DOUBLE"},
		{name: "VARCHAR", in: TypeVarchar, want: "VARCHAR"},
		{name: "BOOLEAN", in: TypeBoolean, want: "BOOLEAN"},
		{name: "DATE", in: TypeDate, want: "DATE"},
		{name: "TIME", in: TypeTime, want: "TIME"},
		{name: "TIMESTAMP", in: TypeTimestamp, want: "TIMESTAMP"},

		// Timestamp variants
		{name: "TIMESTAMP_LTZ", in: TypeTimestampLTZ, want: "TIMESTAMPTZ"},
		{name: "TIMESTAMP_TZ", in: TypeTimestampTZ, want: "TIMESTAMPTZ"},

		// Semi-structured data
		{name: "VARIANT", in: TypeVariant, want: "JSON"},
		{name: "OBJECT", in: TypeObject, want: "JSON"},
		{name: "ARRAY", in: TypeArray, want: "JSON"},

		// Binary and Geography types
		{name: "BINARY", in: TypeBinary, want: "BLOB"},
		{name: "GEOGRAPHY", in: TypeGeography, want: "VARCHAR"},

		// Unknown type should return VARCHAR as default
		{name: "UNKNOWN", in: SnowflakeType("UNKNOWN"), want: "VARCHAR"},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			got := tt.in.ToDuckDBType()
			if diff := cmp.Diff(tt.want, got); diff != "" {
				t.Errorf("ToDuckDBType() mismatch (-want +got):\n%s", diff)
			}
		})
	}
}

// TestSnowflakeType_String tests the string representation of Snowflake types.
func TestSnowflakeType_String(t *testing.T) {
	tests := []struct {
		name string
		in   SnowflakeType
		want string
	}{
		{name: "NUMBER", in: TypeNumber, want: "NUMBER"},
		{name: "INTEGER", in: TypeInteger, want: "INTEGER"},
		{name: "VARCHAR", in: TypeVarchar, want: "VARCHAR"},
		{name: "BOOLEAN", in: TypeBoolean, want: "BOOLEAN"},
		{name: "DATE", in: TypeDate, want: "DATE"},
		{name: "TIMESTAMP", in: TypeTimestamp, want: "TIMESTAMP"},
		{name: "VARIANT", in: TypeVariant, want: "VARIANT"},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			got := string(tt.in)
			if diff := cmp.Diff(tt.want, got); diff != "" {
				t.Errorf("String() mismatch (-want +got):\n%s", diff)
			}
		})
	}
}

// TestSnowflakeType_IsNumeric tests numeric type detection.
func TestSnowflakeType_IsNumeric(t *testing.T) {
	tests := []struct {
		name string
		in   SnowflakeType
		want bool
	}{
		{name: "NUMBER_is_numeric", in: TypeNumber, want: true},
		{name: "INTEGER_is_numeric", in: TypeInteger, want: true},
		{name: "FLOAT_is_numeric", in: TypeFloat, want: true},
		{name: "VARCHAR_not_numeric", in: TypeVarchar, want: false},
		{name: "BOOLEAN_not_numeric", in: TypeBoolean, want: false},
		{name: "DATE_not_numeric", in: TypeDate, want: false},
		{name: "TIMESTAMP_not_numeric", in: TypeTimestamp, want: false},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			got := tt.in.IsNumeric()
			if diff := cmp.Diff(tt.want, got); diff != "" {
				t.Errorf("IsNumeric() mismatch (-want +got):\n%s", diff)
			}
		})
	}
}

// TestSnowflakeType_IsString tests string type detection.
func TestSnowflakeType_IsString(t *testing.T) {
	tests := []struct {
		name string
		in   SnowflakeType
		want bool
	}{
		{name: "VARCHAR_is_string", in: TypeVarchar, want: true},
		{name: "NUMBER_not_string", in: TypeNumber, want: false},
		{name: "INTEGER_not_string", in: TypeInteger, want: false},
		{name: "BOOLEAN_not_string", in: TypeBoolean, want: false},
		{name: "BINARY_not_string", in: TypeBinary, want: false},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			got := tt.in.IsString()
			if diff := cmp.Diff(tt.want, got); diff != "" {
				t.Errorf("IsString() mismatch (-want +got):\n%s", diff)
			}
		})
	}
}

// TestSnowflakeType_IsTemporal tests temporal type detection (date/time types).
func TestSnowflakeType_IsTemporal(t *testing.T) {
	tests := []struct {
		name string
		in   SnowflakeType
		want bool
	}{
		{name: "DATE_is_temporal", in: TypeDate, want: true},
		{name: "TIME_is_temporal", in: TypeTime, want: true},
		{name: "TIMESTAMP_is_temporal", in: TypeTimestamp, want: true},
		{name: "TIMESTAMP_LTZ_is_temporal", in: TypeTimestampLTZ, want: true},
		{name: "TIMESTAMP_TZ_is_temporal", in: TypeTimestampTZ, want: true},
		{name: "VARCHAR_not_temporal", in: TypeVarchar, want: false},
		{name: "NUMBER_not_temporal", in: TypeNumber, want: false},
		{name: "BOOLEAN_not_temporal", in: TypeBoolean, want: false},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			got := tt.in.IsTemporal()
			if diff := cmp.Diff(tt.want, got); diff != "" {
				t.Errorf("IsTemporal() mismatch (-want +got):\n%s", diff)
			}
		})
	}
}

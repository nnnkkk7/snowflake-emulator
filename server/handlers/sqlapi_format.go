package handlers

import (
	"fmt"
	"math/big"
	"strconv"
	"strings"
	"time"
)

// SQL API v2 type names that need more than lower-casing the internal name.
const (
	sqlAPITypeFixed = "fixed"
	sqlAPITypeReal  = "real"
)

// sqlAPIType maps the emulator's upper-case Snowflake type names to the lower-case names the
// SQL API v2 uses in resultSetMetaData.rowType (https://docs.snowflake.com/en/developer-guide/sql-api/handling-responses).
func sqlAPIType(t string) string {
	switch strings.ToUpper(t) {
	case "NUMBER":
		return sqlAPITypeFixed
	case "FLOAT":
		return sqlAPITypeReal
	default:
		return strings.ToLower(t)
	}
}

var unixEpoch = time.Date(1970, time.January, 1, 0, 0, 0, 0, time.UTC)

// sqlAPICell serializes a DuckDB value the way the SQL API does: every cell is a string or null,
// dates are days since the epoch, timestamps use the default output formats. colType is the
// SQL API type name of the column as returned by sqlAPIType.
func sqlAPICell(colType string, v interface{}) interface{} {
	switch x := v.(type) {
	case nil:
		return nil
	case string:
		return x
	case []byte:
		return string(x)
	case bool:
		return strconv.FormatBool(x)
	case time.Time:
		return sqlAPITime(colType, x)
	case *big.Float:
		return x.Text('f', -1)
	case fmt.Stringer:
		return x.String()
	default:
		return sqlAPINumber(x)
	}
}

func sqlAPITime(colType string, t time.Time) string {
	switch colType {
	case "date":
		return strconv.FormatInt(int64(t.Sub(unixEpoch).Hours()/24), 10)
	case "timestamp_tz", "timestamp_ltz":
		return t.Format("2006-01-02 15:04:05.000 -0700")
	default:
		return t.UTC().Format("2006-01-02 15:04:05.000")
	}
}

func sqlAPINumber(v interface{}) string {
	switch x := v.(type) {
	case int:
		return strconv.Itoa(x)
	case int8:
		return strconv.FormatInt(int64(x), 10)
	case int16:
		return strconv.FormatInt(int64(x), 10)
	case int32:
		return strconv.FormatInt(int64(x), 10)
	case int64:
		return strconv.FormatInt(x, 10)
	case uint8:
		return strconv.FormatUint(uint64(x), 10)
	case uint16:
		return strconv.FormatUint(uint64(x), 10)
	case uint32:
		return strconv.FormatUint(uint64(x), 10)
	case uint64:
		return strconv.FormatUint(x, 10)
	case float32:
		return strconv.FormatFloat(float64(x), 'f', -1, 32)
	case float64:
		return strconv.FormatFloat(x, 'f', -1, 64)
	default:
		return fmt.Sprintf("%v", x)
	}
}

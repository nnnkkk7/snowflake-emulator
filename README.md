# Snowflake Emulator

A lightweight, open-source Snowflake emulator built with Go and DuckDB, designed for local development and testing.

[![CI](https://github.com/nnnkkk7/snowflake-emulator/workflows/CI/badge.svg)](https://github.com/nnnkkk7/snowflake-emulator/actions)
[![Go Report Card](https://goreportcard.com/badge/github.com/nnnkkk7/snowflake-emulator)](https://goreportcard.com/report/github.com/nnnkkk7/snowflake-emulator)
[![License: MIT](https://img.shields.io/badge/License-MIT-yellow.svg)](https://opensource.org/licenses/MIT)

## Overview

Snowflake Emulator provides a Snowflake-compatible SQL interface backed by DuckDB, enabling developers to:

- **Test locally** - Run Snowflake SQL queries without cloud costs
- **CI/CD integration** - Automated testing with realistic Snowflake syntax
- **Rapid development** - No network latency, instant query feedback
- **gosnowflake compatible** - Works with the official Go driver out of the box

## Features

### Core Capabilities

- **gosnowflake Driver Support** - Full compatibility with the official Snowflake Go driver
- **REST API v2** - SQL statements API for language-agnostic access
- **SQL Translation** - Automatic Snowflake SQL to DuckDB conversion
- **COPY INTO Support** - Load data from internal stages (CSV, JSON)
- **MERGE INTO Support** - Upsert operations with native DuckDB MERGE or decomposed statements
- **Metadata Management** - Database, Schema, Table, Stage, Warehouse tracking
- **Session Management** - Token-based authentication (development mode)

### Supported SQL Functions

| Snowflake | DuckDB | Description |
|-----------|--------|-------------|
| `IFF(cond, t, f)` | `IF(cond, t, f)` | Conditional expression |
| `NVL(a, b)` | `COALESCE(a, b)` | Null value substitution |
| `NVL2(a, b, c)` | `IF(a IS NOT NULL, b, c)` | Null conditional |
| `IFNULL(a, b)` | `COALESCE(a, b)` | Null value substitution |
| `DATEADD(part, n, date)` | `date + INTERVAL n part` | Date arithmetic |
| `DATEDIFF(part, start, end)` | `DATE_DIFF('part', start, end)` | Date difference |
| `TO_VARIANT(x)` | `CAST(x AS JSON)` | Convert to variant |
| `PARSE_JSON(str)` | `CAST(str AS JSON)` | Parse JSON string |
| `OBJECT_CONSTRUCT(...)` | `json_object(...)` | Build JSON object |
| `LISTAGG(col, sep)` | `STRING_AGG(col, sep)` | String aggregation |
| `FLATTEN(...)` | `UNNEST(...)` | Array expansion |

### Supported Data Types

| Snowflake Type | DuckDB Type |
|----------------|-------------|
| NUMBER, NUMERIC, DECIMAL | DOUBLE / DECIMAL(p,s) |
| INTEGER, BIGINT, SMALLINT, TINYINT | INTEGER / BIGINT |
| FLOAT, DOUBLE, REAL | DOUBLE |
| VARCHAR, STRING, TEXT, CHAR | VARCHAR |
| BOOLEAN | BOOLEAN |
| DATE | DATE |
| TIME | TIME |
| TIMESTAMP, TIMESTAMP_NTZ | TIMESTAMP |
| TIMESTAMP_LTZ, TIMESTAMP_TZ | TIMESTAMPTZ |
| VARIANT, OBJECT | JSON |
| ARRAY | JSON |
| BINARY, VARBINARY | BLOB |
| GEOGRAPHY, GEOMETRY | VARCHAR (WKT) |

## Quick Start

### Prerequisites

- Go 1.24+
- GCC (for DuckDB CGO)

### Installation

```bash
git clone https://github.com/nnnkkk7/snowflake-emulator.git
cd snowflake-emulator
go build -o snowflake-emulator ./cmd/server
```

### Run the Server

```bash
# In-memory mode (default)
./snowflake-emulator

# With persistent storage
DB_PATH=/path/to/database.db ./snowflake-emulator

# Custom port
PORT=9090 ./snowflake-emulator
```

### Using with gosnowflake Driver

```go
package main

import (
    "database/sql"
    "fmt"
    "log"

    _ "github.com/snowflakedb/gosnowflake"
)

func main() {
    // Connect to local emulator
    dsn := "user:pass@localhost:8080/TEST_DB/PUBLIC?account=test&protocol=http"
    db, err := sql.Open("snowflake", dsn)
    if err != nil {
        log.Fatal(err)
    }
    defer db.Close()

    // Execute Snowflake SQL (automatically translated)
    rows, err := db.Query(`
        SELECT
            name,
            IFF(score >= 90, 'A', 'B') AS grade,
            NVL(email, 'no-email') AS email
        FROM users
    `)
    if err != nil {
        log.Fatal(err)
    }
    defer rows.Close()

    for rows.Next() {
        var name, grade, email string
        rows.Scan(&name, &grade, &email)
        fmt.Printf("%s: %s (%s)\n", name, grade, email)
    }
}
```

### Using REST API v2

```bash
# Submit a SQL statement
curl -X POST http://localhost:8080/api/v2/statements \
  -H "Content-Type: application/json" \
  -d '{
    "statement": "SELECT IFF(1 > 0, '\''yes'\'', '\''no'\'')",
    "database": "TEST_DB",
    "schema": "PUBLIC"
  }'

# Get statement result
curl http://localhost:8080/api/v2/statements/{handle}

# Create a database
curl -X POST http://localhost:8080/api/v2/databases \
  -H "Content-Type: application/json" \
  -d '{"name": "MY_DB"}'

# List warehouses
curl http://localhost:8080/api/v2/warehouses
```

## Configuration

### Environment Variables

| Variable | Default | Description |
|----------|---------|-------------|
| `PORT` | `8080` | HTTP server port |
| `DB_PATH` | `:memory:` | DuckDB database path (empty for in-memory) |
| `STAGE_DIR` | `./stages` | Directory for internal stage files |

## API Endpoints

### gosnowflake Protocol

| Endpoint | Method | Description |
|----------|--------|-------------|
| `/session/v1/login-request` | POST | Session login |
| `/session/token-request` | POST | Token refresh |
| `/session/heartbeat` | POST | Keep-alive |
| `/session/renew` | POST | Renew session |
| `/session/logout` | POST | Logout |
| `/session/use` | POST | USE DATABASE/SCHEMA |
| `/queries/v1/query-request` | POST | Execute SQL query |
| `/queries/v1/abort-request` | POST | Cancel query |

### REST API v2

| Endpoint | Method | Description |
|----------|--------|-------------|
| `/api/v2/statements` | POST | Submit SQL statement |
| `/api/v2/statements/{handle}` | GET | Get statement status/result |
| `/api/v2/statements/{handle}/cancel` | POST | Cancel statement |
| `/api/v2/databases` | GET, POST | List/Create databases |
| `/api/v2/databases/{db}` | GET, PUT, DELETE | Get/Alter/Drop database |
| `/api/v2/databases/{db}/schemas` | GET, POST | List/Create schemas |
| `/api/v2/databases/{db}/schemas/{schema}` | GET, DELETE | Get/Drop schema |
| `/api/v2/databases/{db}/schemas/{schema}/tables` | GET, POST | List/Create tables |
| `/api/v2/databases/{db}/schemas/{schema}/tables/{table}` | GET, PUT, DELETE | Get/Alter/Drop table |
| `/api/v2/warehouses` | GET, POST | List/Create warehouses |
| `/api/v2/warehouses/{wh}` | GET, DELETE | Get/Drop warehouse |
| `/api/v2/warehouses/{wh}:resume` | POST | Resume warehouse |
| `/api/v2/warehouses/{wh}:suspend` | POST | Suspend warehouse |
| `/health` | GET | Health check |

## Architecture

```text
snowflake-emulator/
├── cmd/server/              # Application entry point
├── pkg/
│   ├── config/              # Configuration constants
│   ├── connection/          # DuckDB connection manager (thread-safe)
│   ├── contentdata/         # Table content data operations
│   ├── metadata/            # Database/Schema/Table/Stage metadata
│   ├── query/
│   │   ├── executor.go      # Query execution engine with functional options
│   │   ├── translator.go    # Snowflake → DuckDB SQL translation (AST-based)
│   │   ├── classifier.go    # SQL statement classification
│   │   ├── result.go        # Result types (Result, ExecResult, CopyResult, MergeResult)
│   │   ├── table_naming.go  # DuckDB table name generation (DATABASE.SCHEMA_TABLE)
│   │   ├── copy_processor.go    # COPY INTO implementation
│   │   ├── merge_processor.go   # MERGE INTO implementation
│   │   ├── statement_manager.go # Statement lifecycle management
│   │   └── type_mapper.go   # DuckDB → Snowflake type mapping
│   ├── session/             # Session & token management
│   ├── stage/               # Internal stage file operations
│   ├── types/               # Snowflake type definitions
│   └── warehouse/           # Virtual warehouse management
├── server/
│   ├── apierror/            # Snowflake-compatible error responses
│   ├── handlers/            # HTTP request handlers
│   └── types/               # API request/response types
└── tests/
    ├── e2e/                 # End-to-end tests (gosnowflake driver)
    └── integration/         # Integration tests
```

### Layer Design

| Layer | Package | Responsibility |
|-------|---------|----------------|
| HTTP Server | `server/`, `server/handlers/` | REST endpoints, routing |
| Service | `pkg/query/`, `pkg/session/` | Business logic, SQL execution |
| Repository | `pkg/metadata/`, `pkg/contentdata/` | Data access abstraction |
| Storage | `pkg/connection/` | DuckDB connection management |

## Limitations

This emulator is designed for development and testing. The following features are not supported:

- Authentication/Authorization (skipped in dev mode)
- Distributed processing / Clustering
- Time Travel / Zero-Copy Cloning
- Streams, Tasks, Pipes
- External stages (S3, Azure, GCS)
- Stored procedures with JavaScript
- User-defined functions

## Contributing

Contributions are welcome! Please feel free to submit a Pull Request.

## License

[MIT](LICENSE)


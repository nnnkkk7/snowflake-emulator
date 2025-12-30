# Snowflake Emulator

A lightweight, open-source Snowflake emulator built with Go and DuckDB, designed for local development and testing.

[![Test and Coverage](https://github.com/nnnkkk7/snowflake-emurator/workflows/Test%20and%20Coverage/badge.svg)](https://github.com/nnnkkk7/snowflake-emurator/actions)
[![Go Report Card](https://goreportcard.com/badge/github.com/nnnkkk7/snowflake-emurator)](https://goreportcard.com/report/github.com/nnnkkk7/snowflake-emurator)

## Overview

Snowflake Emulator provides a Snowflake-compatible SQL interface backed by DuckDB, enabling developers to:

- Test Snowflake SQL queries locally without cloud costs
- Develop and debug applications using Snowflake syntax
- Run CI/CD pipelines with realistic test data
- Learn Snowflake SQL in a local environment


### Implemented Features

- ✅ Snowflake type system with DuckDB conversion
- ✅ Thread-safe connection management
- ✅ Metadata management (Database/Schema/Table)
- ✅ SQL translation (IFF, NVL, CONCAT functions)
- ✅ Query execution engine
- ✅ Session management with token authentication
- ✅ Snowflake-compatible error handling
- ✅ HTTP API handlers (login, query execution)
- ✅ Comprehensive test coverage
- ✅ CI/CD pipeline (GitHub Actions)
- ✅ Docker support

## Quick Start

### Docker

```bash
docker pull snowflake-emulator:phase1
docker run -p 8080:8080 snowflake-emulator:phase1
```

### Local Build

```bash
# Prerequisites: Go 1.24+, GCC
git clone https://github.com/nnnkkk7/snowflake-emurator.git
cd snowflake-emurator
go build -o snowflake-emulator ./cmd/server
./snowflake-emulator
```

### Usage Example

```bash
# 1. Login
curl -X POST http://localhost:8080/session/v1/login-request \
  -H "Content-Type: application/json" \
  -d '{
    "username": "testuser",
    "password": "testpass",
    "database": "TEST_DB",
    "schema": "PUBLIC"
  }'

# Response: {"success":true,"token":"abc123...","sessionId":"uuid",...}

# 2. Execute Query
curl -X POST http://localhost:8080/queries/v1/query-request \
  -H "Content-Type: application/json" \
  -H "Authorization: Snowflake Token=\"abc123...\"" \
  -d '{
    "statement": "SELECT IFF(value > 100, '\''High'\'', '\''Low'\'') FROM test_table"
  }'

# 3. Logout
curl -X POST http://localhost:8080/session/logout \
  -H "Content-Type: application/json" \
  -d '{"token":"abc123..."}'
```

## Architecture

```text
snowflake-emulator/
├── pkg/
│   ├── config/          # Configuration constants (NEW)
│   ├── connection/      # DuckDB connection manager
│   ├── contentdata/     # Table data management
│   ├── metadata/        # Database/Schema/Table repository
│   ├── query/           # SQL translator, executor, classifier, type_mapper
│   │   ├── executor.go      # Query execution engine
│   │   ├── translator.go    # Snowflake → DuckDB translation
│   │   ├── classifier.go    # SQL statement classification (NEW)
│   │   └── type_mapper.go   # DuckDB → Snowflake type mapping (NEW)
│   ├── session/         # Session & token management
│   ├── types/           # Snowflake type system with DuckDB mapping
│   └── warehouse/       # Virtual warehouse management
├── server/
│   ├── apierror/        # Snowflake error codes & responses
│   ├── handlers/        # HTTP request handlers
│   └── types/           # API request/response types
├── tests/
│   └── integration/     # Integration tests
└── cmd/server/          # Main application
```

## Supported SQL Features

### Functions

- `IFF(condition, true_value, false_value)` → `CASE WHEN`
- `NVL(value, default)` → `COALESCE`
- `CONCAT(str1, str2, ...)` → `||` operator

### Data Types

- INTEGER, BIGINT, SMALLINT
- FLOAT, DOUBLE, DECIMAL, NUMBER
- VARCHAR, STRING, TEXT
- BOOLEAN
- DATE, TIMESTAMP, TIME

### DDL Operations

- CREATE/DROP DATABASE
- CREATE/DROP SCHEMA
- CREATE/DROP TABLE

### DML Operations

- SELECT queries
- INSERT statements
- UPDATE statements
- DELETE statements

## Testing

### Run All Tests

```bash
go test ./...
```

### Run with Coverage

```bash
go test -coverprofile=coverage.out ./...
go tool cover -html=coverage.out
```

## License

MIT License - See [LICENSE](LICENSE) file for details


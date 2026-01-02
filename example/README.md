# Snowflake Emulator Examples

This directory contains examples demonstrating different ways to use the Snowflake Emulator.

## Examples

### 1. gosnowflake Driver (`gosnowflake/`)

Demonstrates using the emulator with the official [gosnowflake](https://github.com/snowflakedb/gosnowflake) driver.

**Prerequisites:** Start the emulator server first.

```bash
# Terminal 1: Start the emulator
go run ./cmd/server

# Terminal 2: Run the example
go run ./example/gosnowflake
```

**Features demonstrated:**
- Connecting with gosnowflake DSN
- Creating tables and inserting data
- Snowflake SQL functions (IFF, NVL, NVL2, DATEADD, DATEDIFF, LISTAGG)
- Combined function usage

### 2. REST API v2 (`restapi/`)

Demonstrates using the emulator via REST API v2 (HTTP/JSON).

**Prerequisites:** Start the emulator server first.

```bash
# Terminal 1: Start the emulator
go run ./cmd/server

# Terminal 2: Run the example
go run ./example/restapi
```

**Features demonstrated:**
- Creating databases and warehouses via REST API
- Submitting SQL statements
- Parameter bindings
- Listing resources
- Error handling

### 3. Docker (`docker/`)

Demonstrates using the emulator running in a Docker container.

**Prerequisites:** Docker must be installed and running.

```bash
# Terminal 1: Start the emulator in Docker
docker compose up -d

# Terminal 2: Run the example
go run ./example/docker

# Stop when done
docker compose down
```

**Features demonstrated:**

- Building and running emulator in Docker
- Health check and readiness waiting
- REST API communication with containerized emulator
- Snowflake SQL functions (IFF, DATEADD, LISTAGG)

## Configuration

### Environment Variables

| Variable | Default | Description |
|----------|---------|-------------|
| `PORT` | `8080` | Server port |
| `DB_PATH` | `:memory:` | DuckDB path (empty for in-memory) |
| `STAGE_DIR` | `./stages` | Internal stage directory |

### gosnowflake DSN Format

```
user:password@host:port/database/schema?account=name&protocol=http
```

Example:
```
testuser:testpass@localhost:8080/TEST_DB/PUBLIC?account=testaccount&protocol=http
```

## Running Examples

```bash
# Build all examples (syntax check)
go build ./example/...

# Run gosnowflake and restapi examples (requires server)
go run ./cmd/server &
sleep 2
go run ./example/gosnowflake
go run ./example/restapi

# Use custom port
PORT=9090 go run ./cmd/server &
sleep 2
SNOWFLAKE_HOST=localhost:9090 go run ./example/gosnowflake
SNOWFLAKE_HOST=localhost:9090 go run ./example/restapi

# Run with Docker
docker compose up -d
go run ./example/docker
docker compose down
```

## Environment Variables for Examples

| Variable | Default | Description |
|----------|---------|-------------|
| `SNOWFLAKE_HOST` | `localhost:8080` | Emulator host:port for gosnowflake/restapi examples |

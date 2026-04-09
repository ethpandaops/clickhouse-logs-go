# clickhouse-logs-go

Go client library for querying pod logs stored in ClickHouse. Uses [ch-go](https://github.com/ClickHouse/ch-go) native binary protocol for performance.

## Install

```bash
go get github.com/ethpandaops/clickhouse-logs-go
```

## Usage

### Create a client

```go
// Option 1: Client manages its own connection pool
client, err := clickhouselogs.New(&clickhouselogs.Config{
    Addr:     "clickhouse:9000",
    Username: "logs_admin",
    Password: "secret",
}, logger)
if err != nil {
    log.Fatal(err)
}
if err := client.Start(ctx); err != nil {
    log.Fatal(err)
}
defer client.Stop()

// Option 2: BYO connection pool
client := clickhouselogs.NewWithPool(existingPool, logger,
    clickhouselogs.WithQueryTimeout(30*time.Second),
)
```

### Fetch (bounded queries)

```go
entries, err := client.Fetch(ctx, clickhouselogs.NewQuery(clickhouselogs.Internal).
    From(time.Now().Add(-1*time.Hour)).
    To(time.Now()).
    IngressUser("sigma").
    Namespace("mainnet").
    Container("reth").
    Stream("stderr").
    Search(clickhouselogs.Token("ERROR")).
    Limit(1000),
)
```

### Scan (callback processing)

```go
err := client.Scan(ctx, clickhouselogs.NewQuery(clickhouselogs.Internal).
    From(time.Now().Add(-1*time.Hour)).
    To(time.Now()).
    Namespace("mainnet"),
    func(entry clickhouselogs.LogEntry) error {
        fmt.Println(entry.Timestamp, entry.Message)
        return nil
    },
)
```

### Stream (pull-based iterator)

```go
iter, err := client.Stream(ctx, clickhouselogs.NewQuery(clickhouselogs.Internal).
    From(time.Now().Add(-1*time.Hour)).
    To(time.Now()),
)
if err != nil {
    log.Fatal(err)
}
defer iter.Close()

for iter.Next() {
    entry := iter.Entry()
    fmt.Println(entry.Timestamp, entry.Message)
}
if err := iter.Err(); err != nil {
    log.Fatal(err)
}
```

## Query Builder

### Sources

- `clickhouselogs.Internal` — `logs_internal.logs`
- `clickhouselogs.External` — `logs_external.logs`

### Filters

| Method | Description |
|--------|-------------|
| `From(time.Time)` | Start time (inclusive, required) |
| `To(time.Time)` | End time (exclusive, required) |
| `IngressUser(string)` | Exact match |
| `Namespace(...string)` | IN clause |
| `Pod(string)` | Exact match |
| `PodLike(string)` | LIKE pattern |
| `Container(...string)` | IN clause |
| `Node(...string)` | IN clause |
| `Stream(...string)` | IN clause (stdout/stderr) |
| `Search(MessageSearch)` | Message content filter |
| `Limit(int)` | Max rows (required for Fetch) |
| `OrderAsc()` | ORDER BY Timestamp ASC |
| `OrderDesc()` | ORDER BY Timestamp DESC |
| `Unordered()` | No ORDER BY |

### Message Search

```go
// Token: hasToken — full_text index accelerated, case-sensitive, whole words
clickhouselogs.Token("ERROR")

// Tokens: hasAllTokens — all must be present, index accelerated
clickhouselogs.Tokens("ERROR", "timeout")

// Substring: LIKE — NOT index accelerated
clickhouselogs.Substring("panic:")

// Regex: match() — NOT index accelerated, use sparingly
clickhouselogs.Regex("error.*timeout")
```

## Retrieval Methods

| Method | Use Case | Limit Required | Retries | Default Order |
|--------|----------|----------------|---------|---------------|
| `Fetch` | Small bounded queries | Yes | Yes | `Timestamp DESC` |
| `Scan` | Pipeline processing | No | No | None |
| `Stream` | Large datasets | No | No | None |

## Testing

```bash
# Unit + integration tests (requires Docker)
go test -race ./...
```

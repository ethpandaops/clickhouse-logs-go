package clickhouselogs

import (
	"context"
	"fmt"
	"log/slog"
	"os"
	"runtime"
	"testing"
	"time"

	"github.com/ClickHouse/ch-go"
	"github.com/ClickHouse/ch-go/chpool"
	"github.com/ClickHouse/ch-go/proto"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
	"github.com/testcontainers/testcontainers-go"
	"github.com/testcontainers/testcontainers-go/wait"
)

// Mirrors the production OpenTelemetry log schema: k8s.* are materialized
// columns promoted from ResourceAttributes, the message lives in Body, and
// the stdout/stderr stream is only available as a LogAttributes entry.
const createTableSQL = "" +
	"CREATE TABLE IF NOT EXISTS internal.otel_logs (\n" +
	"    Timestamp              DateTime64(9),\n" +
	"    IngressUser            LowCardinality(String),\n" +
	"    ServiceName            LowCardinality(String),\n" +
	"    Body                   String,\n" +
	"    LogAttributes          Map(LowCardinality(String), String),\n" +
	"    ResourceAttributes     Map(LowCardinality(String), String),\n" +
	"    `k8s.namespace.name`   LowCardinality(String),\n" +
	"    `k8s.pod.name`         LowCardinality(String),\n" +
	"    `k8s.container.name`   LowCardinality(String),\n" +
	"    `k8s.node.name`        LowCardinality(String)\n" +
	") ENGINE = MergeTree()\n" +
	"ORDER BY (IngressUser, toStartOfFiveMinutes(Timestamp), ServiceName, Timestamp)\n" +
	"PARTITION BY toDate(Timestamp)\n"

// tenantShape selects which identity columns a test row populates. Rows from
// k8s workloads fill the k8s.* columns and leave ResourceAttributes empty;
// VM-hosted devnet nodes do the opposite. Both must decode identically.
type tenantShape int

const (
	shapeK8s tenantShape = iota
	shapeVM
)

var testPool *chpool.Pool //nolint:gochecknoglobals // shared test container
var testAddr string       //nolint:gochecknoglobals // shared test container address

func TestMain(m *testing.M) {
	ctx := context.Background()

	req := testcontainers.ContainerRequest{
		Image:        "clickhouse/clickhouse-server:26.2",
		ExposedPorts: []string{"9000/tcp"},
		Env: map[string]string{
			"CLICKHOUSE_DEFAULT_ACCESS_MANAGEMENT": "1",
		},
		WaitingFor: wait.ForListeningPort("9000/tcp").WithStartupTimeout(60 * time.Second),
	}

	container, err := testcontainers.GenericContainer(ctx, testcontainers.GenericContainerRequest{
		ContainerRequest: req,
		Started:          true,
	})
	if err != nil {
		fmt.Fprintf(os.Stderr, "failed to start clickhouse container: %v\n", err)
		os.Exit(1)
	}

	host, err := container.Host(ctx)
	if err != nil {
		fmt.Fprintf(os.Stderr, "failed to get host: %v\n", err)
		os.Exit(1)
	}

	port, err := container.MappedPort(ctx, "9000/tcp")
	if err != nil {
		fmt.Fprintf(os.Stderr, "failed to get port: %v\n", err)
		os.Exit(1)
	}

	testAddr = fmt.Sprintf("%s:%s", host, port.Port())

	testPool, err = chpool.Dial(ctx, chpool.Options{
		ClientOptions: ch.Options{
			Address:  testAddr,
			Database: "default",
		},
	})
	if err != nil {
		fmt.Fprintf(os.Stderr, "failed to dial clickhouse: %v\n", err)
		os.Exit(1)
	}

	if err := testPool.Do(ctx, ch.Query{Body: "CREATE DATABASE IF NOT EXISTS internal"}); err != nil {
		fmt.Fprintf(os.Stderr, "failed to create database: %v\n", err)
		os.Exit(1)
	}

	if err := testPool.Do(ctx, ch.Query{Body: createTableSQL}); err != nil {
		fmt.Fprintf(os.Stderr, "failed to create table: %v\n", err)
		os.Exit(1)
	}

	code := m.Run()

	testPool.Close()

	if err := testcontainers.TerminateContainer(container); err != nil {
		fmt.Fprintf(os.Stderr, "failed to terminate container: %v\n", err)
	}

	os.Exit(code)
}

func insertTestRows(t *testing.T, entries []LogEntry) {
	t.Helper()

	insertTestRowsAs(t, entries, shapeK8s)
}

func insertTestRowsAs(t *testing.T, entries []LogEntry, shape tenantShape) {
	t.Helper()

	ctx := context.Background()

	// Truncate first so tests are isolated.
	require.NoError(t, testPool.Do(ctx, ch.Query{Body: "TRUNCATE TABLE internal.otel_logs"}))

	colTimestamp := new(proto.ColDateTime64).WithPrecision(proto.PrecisionNano).WithLocation(time.UTC)
	colIngressUser := new(proto.ColStr)
	colServiceName := new(proto.ColStr)
	colNamespace := new(proto.ColStr)
	colPod := new(proto.ColStr)
	colContainer := new(proto.ColStr)
	colNode := new(proto.ColStr)
	colBody := new(proto.ColStr)
	colLogAttributes := proto.NewMap[string, string](new(proto.ColStr).LowCardinality(), new(proto.ColStr))
	colResourceAttributes := proto.NewMap[string, string](new(proto.ColStr).LowCardinality(), new(proto.ColStr))

	for _, e := range entries {
		colTimestamp.Append(e.Timestamp)
		colIngressUser.Append(e.IngressUser)
		colBody.Append(e.Message)
		colLogAttributes.Append(map[string]string{"stream": e.Stream})

		// Each shape populates exactly one identity carrier and leaves the
		// other empty, mirroring how the two tenants actually store rows.
		switch shape {
		case shapeK8s:
			colServiceName.Append(e.Container)
			colNamespace.Append(e.Namespace)
			colPod.Append(e.Pod)
			colContainer.Append(e.Container)
			colNode.Append(e.Node)
			colResourceAttributes.Append(map[string]string{})
		case shapeVM:
			colServiceName.Append(e.Container)
			colNamespace.Append("")
			colPod.Append("")
			colContainer.Append("")
			colNode.Append("")
			colResourceAttributes.Append(map[string]string{
				"network":   e.Namespace,
				"host.name": e.Pod,
			})
		}
	}

	err := testPool.Do(ctx, ch.Query{
		Body: "INSERT INTO internal.otel_logs VALUES",
		Input: proto.Input{
			{Name: "Timestamp", Data: colTimestamp},
			{Name: "IngressUser", Data: colIngressUser},
			{Name: "ServiceName", Data: colServiceName},
			{Name: "Body", Data: colBody},
			{Name: "LogAttributes", Data: colLogAttributes},
			{Name: "ResourceAttributes", Data: colResourceAttributes},
			{Name: "k8s.namespace.name", Data: colNamespace},
			{Name: "k8s.pod.name", Data: colPod},
			{Name: "k8s.container.name", Data: colContainer},
			{Name: "k8s.node.name", Data: colNode},
		},
	})
	require.NoError(t, err)
}

func testLogger() *slog.Logger {
	return slog.New(slog.NewTextHandler(os.Stderr, &slog.HandlerOptions{Level: slog.LevelDebug}))
}

func testEntries() []LogEntry {
	base := time.Date(2025, 6, 15, 10, 0, 0, 0, time.UTC)

	return []LogEntry{
		{
			Timestamp: base, IngressUser: "sigma", Namespace: "mainnet",
			Pod: "reth-0", Container: "reth", Node: "node-1",
			Stream: "stderr", Message: "ERROR: block validation failed",
		},
		{
			Timestamp: base.Add(time.Second), IngressUser: "sigma", Namespace: "mainnet",
			Pod: "reth-0", Container: "reth", Node: "node-1",
			Stream: "stdout", Message: "INFO: syncing block 12345",
		},
		{
			Timestamp: base.Add(2 * time.Second), IngressUser: "sigma", Namespace: "testnet",
			Pod: "lighthouse-0", Container: "lighthouse", Node: "node-2",
			Stream: "stderr", Message: "WARN: peer timeout after 30s",
		},
		{
			Timestamp: base.Add(3 * time.Second), IngressUser: "alpha", Namespace: "mainnet",
			Pod: "prysm-0", Container: "prysm", Node: "node-1",
			Stream: "stdout", Message: "ERROR: attestation timeout occurred",
		},
	}
}

func TestIntegration_FetchBasic(t *testing.T) {
	entries := testEntries()
	insertTestRows(t, entries)

	client := NewWithPool(testPool, testLogger())

	from := entries[0].Timestamp.Add(-time.Minute)
	to := entries[len(entries)-1].Timestamp.Add(time.Minute)

	result, err := client.Fetch(context.Background(), NewQuery(Internal).
		From(from).To(to).Limit(10))
	require.NoError(t, err)
	assert.Len(t, result, 4)
}

func TestIntegration_FetchRequiresLimit(t *testing.T) {
	client := NewWithPool(testPool, testLogger())

	_, err := client.Fetch(context.Background(), NewQuery(Internal).
		From(time.Now().Add(-time.Hour)).To(time.Now()))
	require.Error(t, err)
	assert.Contains(t, err.Error(), "Limit")
}

func TestIntegration_FetchDefaultOrderDesc(t *testing.T) {
	entries := testEntries()
	insertTestRows(t, entries)

	client := NewWithPool(testPool, testLogger())

	from := entries[0].Timestamp.Add(-time.Minute)
	to := entries[len(entries)-1].Timestamp.Add(time.Minute)

	result, err := client.Fetch(context.Background(), NewQuery(Internal).
		From(from).To(to).Limit(10))
	require.NoError(t, err)
	require.True(t, len(result) >= 2)

	// Default Fetch order is DESC — first entry should have the latest timestamp.
	assert.True(t, !result[0].Timestamp.Before(result[len(result)-1].Timestamp),
		"expected DESC order: first=%v last=%v", result[0].Timestamp, result[len(result)-1].Timestamp)
}

func TestIntegration_FetchWithFilters(t *testing.T) {
	entries := testEntries()
	insertTestRows(t, entries)

	client := NewWithPool(testPool, testLogger())

	from := entries[0].Timestamp.Add(-time.Minute)
	to := entries[len(entries)-1].Timestamp.Add(time.Minute)

	t.Run("IngressUser filter", func(t *testing.T) {
		result, err := client.Fetch(context.Background(), NewQuery(Internal).
			From(from).To(to).IngressUser("alpha").Limit(10))
		require.NoError(t, err)
		assert.Len(t, result, 1)
		assert.Equal(t, "alpha", result[0].IngressUser)
	})

	t.Run("Namespace filter", func(t *testing.T) {
		result, err := client.Fetch(context.Background(), NewQuery(Internal).
			From(from).To(to).Namespace("testnet").Limit(10))
		require.NoError(t, err)
		assert.Len(t, result, 1)
		assert.Equal(t, "testnet", result[0].Namespace)
	})

	t.Run("Container filter", func(t *testing.T) {
		result, err := client.Fetch(context.Background(), NewQuery(Internal).
			From(from).To(to).Container("reth").Limit(10))
		require.NoError(t, err)
		assert.Len(t, result, 2)
	})

	t.Run("Stream filter", func(t *testing.T) {
		result, err := client.Fetch(context.Background(), NewQuery(Internal).
			From(from).To(to).Stream("stderr").Limit(10))
		require.NoError(t, err)
		assert.Len(t, result, 2)
	})

	t.Run("Node filter", func(t *testing.T) {
		result, err := client.Fetch(context.Background(), NewQuery(Internal).
			From(from).To(to).Node("node-2").Limit(10))
		require.NoError(t, err)
		assert.Len(t, result, 1)
		assert.Equal(t, "lighthouse-0", result[0].Pod)
	})
}

func TestIntegration_DateTimeUTC(t *testing.T) {
	entries := testEntries()
	insertTestRows(t, entries)

	client := NewWithPool(testPool, testLogger())

	from := entries[0].Timestamp.Add(-time.Minute)
	to := entries[len(entries)-1].Timestamp.Add(time.Minute)

	result, err := client.Fetch(context.Background(), NewQuery(Internal).
		From(from).To(to).Limit(10))
	require.NoError(t, err)

	for _, entry := range result {
		assert.Equal(t, time.UTC, entry.Timestamp.Location(),
			"expected UTC location, got %v", entry.Timestamp.Location())
	}
}

func TestIntegration_DateTimePrecision(t *testing.T) {
	entries := testEntries()
	insertTestRows(t, entries)

	client := NewWithPool(testPool, testLogger())

	from := entries[0].Timestamp.Add(-time.Minute)
	to := entries[len(entries)-1].Timestamp.Add(time.Minute)

	result, err := client.Fetch(context.Background(), NewQuery(Internal).
		From(from).To(to).Limit(10).OrderAsc())
	require.NoError(t, err)
	require.Len(t, result, 4)

	// Verify millisecond precision is preserved in the round-trip.
	for i, entry := range result {
		assert.Equal(t, entries[i].Timestamp, entry.Timestamp,
			"timestamp mismatch at index %d", i)
	}
}

func TestIntegration_TokenSearch(t *testing.T) {
	entries := testEntries()
	insertTestRows(t, entries)

	client := NewWithPool(testPool, testLogger())

	from := entries[0].Timestamp.Add(-time.Minute)
	to := entries[len(entries)-1].Timestamp.Add(time.Minute)

	t.Run("hasToken matches whole word", func(t *testing.T) {
		result, err := client.Fetch(context.Background(), NewQuery(Internal).
			From(from).To(to).Search(Token("ERROR")).Limit(10))
		require.NoError(t, err)
		assert.Len(t, result, 2)
	})

	t.Run("hasToken case sensitive", func(t *testing.T) {
		result, err := client.Fetch(context.Background(), NewQuery(Internal).
			From(from).To(to).Search(Token("error")).Limit(10))
		require.NoError(t, err)
		assert.Empty(t, result)
	})
}

func TestIntegration_TokensSearch(t *testing.T) {
	entries := testEntries()
	insertTestRows(t, entries)

	client := NewWithPool(testPool, testLogger())

	from := entries[0].Timestamp.Add(-time.Minute)
	to := entries[len(entries)-1].Timestamp.Add(time.Minute)

	result, err := client.Fetch(context.Background(), NewQuery(Internal).
		From(from).To(to).Search(Tokens("ERROR", "timeout")).Limit(10))
	require.NoError(t, err)
	assert.Len(t, result, 1)
	assert.Contains(t, result[0].Message, "timeout")
}

func TestIntegration_SubstringSearch(t *testing.T) {
	entries := testEntries()
	insertTestRows(t, entries)

	client := NewWithPool(testPool, testLogger())

	from := entries[0].Timestamp.Add(-time.Minute)
	to := entries[len(entries)-1].Timestamp.Add(time.Minute)

	result, err := client.Fetch(context.Background(), NewQuery(Internal).
		From(from).To(to).Search(Substring("block")).Limit(10))
	require.NoError(t, err)
	assert.Len(t, result, 2)
}

func TestIntegration_Scan(t *testing.T) {
	entries := testEntries()
	insertTestRows(t, entries)

	client := NewWithPool(testPool, testLogger())

	from := entries[0].Timestamp.Add(-time.Minute)
	to := entries[len(entries)-1].Timestamp.Add(time.Minute)

	var scanned []LogEntry

	err := client.Scan(context.Background(), NewQuery(Internal).
		From(from).To(to), func(entry LogEntry) error {
		scanned = append(scanned, entry)

		return nil
	})
	require.NoError(t, err)
	assert.Len(t, scanned, 4)
}

func TestIntegration_Stream(t *testing.T) {
	entries := testEntries()
	insertTestRows(t, entries)

	client := NewWithPool(testPool, testLogger())

	from := entries[0].Timestamp.Add(-time.Minute)
	to := entries[len(entries)-1].Timestamp.Add(time.Minute)

	iter, err := client.Stream(context.Background(), NewQuery(Internal).
		From(from).To(to))
	require.NoError(t, err)

	defer iter.Close()

	var streamed []LogEntry

	for iter.Next() {
		streamed = append(streamed, iter.Entry())
	}

	require.NoError(t, iter.Err())
	assert.Len(t, streamed, 4)
}

func TestIntegration_StreamCloseNoLeak(t *testing.T) {
	entries := testEntries()
	insertTestRows(t, entries)

	client := NewWithPool(testPool, testLogger())

	from := entries[0].Timestamp.Add(-time.Minute)
	to := entries[len(entries)-1].Timestamp.Add(time.Minute)

	goroutinesBefore := runtime.NumGoroutine()

	iter, err := client.Stream(context.Background(), NewQuery(Internal).
		From(from).To(to))
	require.NoError(t, err)

	if iter.Next() {
		_ = iter.Entry()
	}

	iter.Close()

	time.Sleep(100 * time.Millisecond)

	goroutinesAfter := runtime.NumGoroutine()
	assert.InDelta(t, goroutinesBefore, goroutinesAfter, 3, "goroutine leak detected")
}

func TestIntegration_LowCardinalityDecodeAsString(t *testing.T) {
	entries := testEntries()
	insertTestRows(t, entries)

	client := NewWithPool(testPool, testLogger())

	from := entries[0].Timestamp.Add(-time.Minute)
	to := entries[len(entries)-1].Timestamp.Add(time.Minute)

	result, err := client.Fetch(context.Background(), NewQuery(Internal).
		From(from).To(to).Limit(10))
	require.NoError(t, err)

	for _, entry := range result {
		assert.NotEmpty(t, entry.IngressUser)
		assert.NotEmpty(t, entry.Namespace)
		assert.NotEmpty(t, entry.Container)
		assert.NotEmpty(t, entry.Node)
		assert.NotEmpty(t, entry.Stream)
	}
}

func TestIntegration_OwnPoolLifecycle(t *testing.T) {
	entries := testEntries()
	insertTestRows(t, entries)

	client, err := New(&Config{Addr: testAddr}, testLogger())
	require.NoError(t, err)

	require.NoError(t, client.Start(context.Background()))

	defer func() {
		require.NoError(t, client.Stop())
	}()

	from := entries[0].Timestamp.Add(-time.Minute)
	to := entries[len(entries)-1].Timestamp.Add(time.Minute)

	result, err := client.Fetch(context.Background(), NewQuery(Internal).
		From(from).To(to).Limit(10))
	require.NoError(t, err)
	assert.Len(t, result, 4)

	// Start is idempotent.
	require.NoError(t, client.Start(context.Background()))
}

// TestIntegration_VMTenantIdentity covers rows from VM-hosted devnet nodes,
// which leave every k8s.* column empty and describe themselves through
// ResourceAttributes instead. Without the fallbacks these decode blank, which
// silently strips pod/namespace/node from every result.
func TestIntegration_VMTenantIdentity(t *testing.T) {
	entries := []LogEntry{
		{
			Timestamp:   time.Date(2025, 6, 15, 10, 0, 0, 0, time.UTC),
			IngressUser: "sigma", Namespace: "glamsterdam-devnet-7",
			Pod: "prysm-geth-1", Container: "beacon", Node: "ignored-for-vm",
			Stream: "stdout", Message: "tysm_hook_exec fired",
		},
	}
	insertTestRowsAs(t, entries, shapeVM)

	client := NewWithPool(testPool, testLogger())

	got, err := client.Fetch(context.Background(), NewQuery(Internal).
		From(time.Date(2025, 6, 15, 9, 0, 0, 0, time.UTC)).
		To(time.Date(2025, 6, 15, 11, 0, 0, 0, time.UTC)).
		Limit(10))
	require.NoError(t, err)
	require.Len(t, got, 1)

	assert.Equal(t, "glamsterdam-devnet-7", got[0].Namespace, "Namespace should fall back to ResourceAttributes['network']")
	assert.Equal(t, "prysm-geth-1", got[0].Pod, "Pod should fall back to ResourceAttributes['host.name']")
	assert.Equal(t, "prysm-geth-1", got[0].Node, "Node should fall back to the host, since the VM is the machine")
	assert.Equal(t, "beacon", got[0].Container, "Container should fall back to ServiceName")
	assert.Equal(t, "tysm_hook_exec fired", got[0].Message)

	// Filtering has to resolve through the same fallback, or the activation
	// detail view finds nothing.
	filtered, err := client.Fetch(context.Background(), NewQuery(Internal).
		From(time.Date(2025, 6, 15, 9, 0, 0, 0, time.UTC)).
		To(time.Date(2025, 6, 15, 11, 0, 0, 0, time.UTC)).
		Pod("prysm-geth-1").
		Namespace("glamsterdam-devnet-7").
		Limit(10))
	require.NoError(t, err)
	assert.Len(t, filtered, 1, "filtering by pod/namespace must match VM-tenant rows")

	pods, err := client.Distinct(context.Background(), NewQuery(Internal).
		From(time.Date(2025, 6, 15, 9, 0, 0, 0, time.UTC)).
		To(time.Date(2025, 6, 15, 11, 0, 0, 0, time.UTC)), "Pod")
	require.NoError(t, err)
	assert.Equal(t, []string{"prysm-geth-1"}, pods)
}

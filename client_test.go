package clickhouselogs

import (
	"context"
	"log/slog"
	"os"
	"testing"
	"time"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

func devLogger() *slog.Logger {
	return slog.New(slog.NewTextHandler(os.Stderr, &slog.HandlerOptions{Level: slog.LevelDebug}))
}

func TestNew_ValidConfig(t *testing.T) {
	client, err := New(&Config{Addr: "localhost:9000"}, devLogger())
	require.NoError(t, err)
	assert.NotNil(t, client)
	assert.True(t, client.ownsPool)
}

func TestNew_InvalidConfig(t *testing.T) {
	_, err := New(&Config{}, devLogger())
	require.Error(t, err)
	assert.Contains(t, err.Error(), "invalid config")
}

func TestNew_SetsDefaults(t *testing.T) {
	client, err := New(&Config{Addr: "localhost:9000"}, devLogger())
	require.NoError(t, err)
	assert.Equal(t, 3, client.config.MaxRetries)
	assert.Equal(t, 60*time.Second, client.config.QueryTimeout)
}

func TestNewWithPool_Options(t *testing.T) {
	client := NewWithPool(nil, devLogger(),
		WithQueryTimeout(30*time.Second),
		WithMaxBlockSize(5000),
	)

	assert.False(t, client.ownsPool)
	assert.Equal(t, 30*time.Second, client.config.QueryTimeout)
	assert.Equal(t, 5000, client.maxBlockSize)
}

func TestNewWithPool_StartIsNoop(t *testing.T) {
	client := NewWithPool(nil, devLogger())

	require.NoError(t, client.Start(context.Background()))
}

func TestNewWithPool_StopIsNoop(t *testing.T) {
	client := NewWithPool(nil, devLogger())

	require.NoError(t, client.Stop())
}

func TestClient_QuerySettings(t *testing.T) {
	t.Run("no max block size", func(t *testing.T) {
		client := NewWithPool(nil, devLogger())
		assert.Nil(t, client.querySettings())
	})

	t.Run("with max block size", func(t *testing.T) {
		client := NewWithPool(nil, devLogger(), WithMaxBlockSize(1000))
		settings := client.querySettings()
		require.Len(t, settings, 1)
		assert.Equal(t, "max_block_size", settings[0].Key)
		assert.Equal(t, "1000", settings[0].Value)
	})
}

func TestClient_FetchValidation(t *testing.T) {
	client := NewWithPool(nil, devLogger())

	t.Run("missing time range", func(t *testing.T) {
		_, err := client.Fetch(context.Background(), NewQuery(Internal).Limit(10))
		require.Error(t, err)
		assert.Contains(t, err.Error(), "invalid query")
	})

	t.Run("missing limit", func(t *testing.T) {
		_, err := client.Fetch(context.Background(), NewQuery(Internal).
			From(time.Now().Add(-time.Hour)).To(time.Now()))
		require.Error(t, err)
		assert.Contains(t, err.Error(), "Limit")
	})
}

func TestClient_ScanValidation(t *testing.T) {
	client := NewWithPool(nil, devLogger())

	_, err := client.Fetch(context.Background(), NewQuery(Internal))
	require.Error(t, err)
	assert.Contains(t, err.Error(), "invalid query")
}

func TestClient_StreamValidation(t *testing.T) {
	client := NewWithPool(nil, devLogger())

	_, err := client.Stream(context.Background(), NewQuery(Internal))
	require.Error(t, err)
	assert.Contains(t, err.Error(), "invalid query")
}

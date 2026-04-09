package clickhouselogs

import (
	"context"
	"fmt"
	"testing"
	"time"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

func TestMockClient_Fetch(t *testing.T) {
	mock := NewMockClient()

	expected := []LogEntry{{Message: "test"}}

	mock.FetchFunc = func(ctx context.Context, q *Query) ([]LogEntry, error) {
		return expected, nil
	}

	result, err := mock.Fetch(context.Background(), NewQuery(Internal).From(time.Now()).To(time.Now()).Limit(10))
	require.NoError(t, err)
	assert.Equal(t, expected, result)
	assert.True(t, mock.WasCalled("Fetch"))
	assert.Equal(t, 1, mock.GetCallCount("Fetch"))
}

func TestMockClient_Scan(t *testing.T) {
	mock := NewMockClient()

	err := mock.Scan(context.Background(), NewQuery(Internal), func(entry LogEntry) error {
		return nil
	})
	require.NoError(t, err)
	assert.True(t, mock.WasCalled("Scan"))
}

func TestMockClient_Stream(t *testing.T) {
	mock := NewMockClient()

	iter, err := mock.Stream(context.Background(), NewQuery(Internal))
	require.NoError(t, err)
	assert.False(t, iter.Next()) // default returns empty iterator
	iter.Close()
	assert.True(t, mock.WasCalled("Stream"))
}

func TestMockClient_StartStop(t *testing.T) {
	mock := NewMockClient()

	require.NoError(t, mock.Start(context.Background()))
	require.NoError(t, mock.Stop())
	assert.Equal(t, 1, mock.GetCallCount("Start"))
	assert.Equal(t, 1, mock.GetCallCount("Stop"))
}

func TestMockClient_Reset(t *testing.T) {
	mock := NewMockClient()

	_, _ = mock.Fetch(context.Background(), NewQuery(Internal))

	assert.True(t, mock.WasCalled("Fetch"))

	mock.Reset()

	assert.False(t, mock.WasCalled("Fetch"))
}

func TestMockClient_ErrorFunc(t *testing.T) {
	mock := NewMockClient()

	mock.FetchFunc = func(ctx context.Context, q *Query) ([]LogEntry, error) {
		return nil, fmt.Errorf("fetch error")
	}

	_, err := mock.Fetch(context.Background(), NewQuery(Internal))
	require.Error(t, err)
	assert.Contains(t, err.Error(), "fetch error")
}

package clickhouselogs

import (
	"fmt"
	"runtime"
	"testing"
	"time"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

func makeEntries(n int) []LogEntry {
	entries := make([]LogEntry, 0, n)

	for i := 0; i < n; i++ {
		entries = append(entries, LogEntry{
			Timestamp: time.Date(2025, 1, 1, 0, 0, i, 0, time.UTC),
			Message:   fmt.Sprintf("msg-%d", i),
		})
	}

	return entries
}

func TestIterator_BasicIteration(t *testing.T) {
	ch := make(chan iteratorEntry, 2)

	ch <- iteratorEntry{entries: makeEntries(3)}

	ch <- iteratorEntry{entries: makeEntries(2)}

	close(ch)

	it := &Iterator{
		ch:     ch,
		cancel: func() {},
		idx:    -1,
	}

	var collected []string

	for it.Next() {
		collected = append(collected, it.Entry().Message)
	}

	require.NoError(t, it.Err())
	assert.Equal(t, []string{"msg-0", "msg-1", "msg-2", "msg-0", "msg-1"}, collected)
}

func TestIterator_EmptyResults(t *testing.T) {
	ch := make(chan iteratorEntry)
	close(ch)

	it := &Iterator{
		ch:     ch,
		cancel: func() {},
		idx:    -1,
	}

	assert.False(t, it.Next())
	require.NoError(t, it.Err())
}

func TestIterator_Error(t *testing.T) {
	ch := make(chan iteratorEntry, 2)

	ch <- iteratorEntry{entries: makeEntries(1)}

	ch <- iteratorEntry{err: fmt.Errorf("query failed")}

	close(ch)

	it := &Iterator{
		ch:     ch,
		cancel: func() {},
		idx:    -1,
	}

	assert.True(t, it.Next())
	assert.Equal(t, "msg-0", it.Entry().Message)

	assert.False(t, it.Next())
	require.Error(t, it.Err())
	assert.Contains(t, it.Err().Error(), "query failed")
}

func TestIterator_EarlyClose(t *testing.T) {
	ch := make(chan iteratorEntry, 10)
	for i := 0; i < 10; i++ {
		ch <- iteratorEntry{entries: makeEntries(1)}
	}

	close(ch)

	cancelled := false

	it := &Iterator{
		ch:     ch,
		cancel: func() { cancelled = true },
		idx:    -1,
	}

	assert.True(t, it.Next())
	it.Close()

	assert.True(t, cancelled)
	assert.False(t, it.Next())
}

func TestIterator_CloseIdempotent(t *testing.T) {
	ch := make(chan iteratorEntry)
	close(ch)

	callCount := 0

	it := &Iterator{
		ch:     ch,
		cancel: func() { callCount++ },
		idx:    -1,
	}

	it.Close()
	it.Close()
	it.Close()

	assert.Equal(t, 1, callCount)
}

func TestIterator_EmptyBatchSkipped(t *testing.T) {
	ch := make(chan iteratorEntry, 3)

	ch <- iteratorEntry{entries: []LogEntry{}}

	ch <- iteratorEntry{entries: makeEntries(1)}

	close(ch)

	it := &Iterator{
		ch:     ch,
		cancel: func() {},
		idx:    -1,
	}

	assert.True(t, it.Next())
	assert.Equal(t, "msg-0", it.Entry().Message)
	assert.False(t, it.Next())
	require.NoError(t, it.Err())
}

func TestIterator_CloseUnblocksProducer(t *testing.T) {
	ch := make(chan iteratorEntry, 1)
	goroutinesBefore := runtime.NumGoroutine()

	done := make(chan struct{})

	go func() {
		defer close(done)

		// Simulate a producer that sends many batches.
		for i := 0; i < 100; i++ {
			ch <- iteratorEntry{entries: makeEntries(1)}
		}

		close(ch)
	}()

	it := &Iterator{
		ch:     ch,
		cancel: func() {},
		idx:    -1,
	}

	// Read one entry then close.
	it.Next()
	it.Close()

	// Wait for producer to finish (Close drains the channel).
	<-done

	// Give goroutines time to clean up.
	time.Sleep(50 * time.Millisecond)

	goroutinesAfter := runtime.NumGoroutine()
	assert.InDelta(t, goroutinesBefore, goroutinesAfter, 2, "goroutine leak detected")
}

package clickhouselogs

import (
	"context"
	"fmt"
	"io"
	"net"
	"syscall"
	"testing"
	"time"

	"github.com/ClickHouse/ch-go"
	"github.com/ClickHouse/ch-go/compress"
	"github.com/ClickHouse/ch-go/proto"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

func TestIsRetryableError(t *testing.T) {
	tests := []struct {
		name      string
		err       error
		retryable bool
	}{
		{
			name:      "nil error",
			err:       nil,
			retryable: false,
		},
		{
			name:      "context canceled",
			err:       context.Canceled,
			retryable: false,
		},
		{
			name:      "context deadline exceeded",
			err:       context.DeadlineExceeded,
			retryable: false,
		},
		{
			name:      "wrapped context canceled",
			err:       fmt.Errorf("query failed: %w", context.Canceled),
			retryable: false,
		},
		{
			name:      "client closed",
			err:       ch.ErrClosed,
			retryable: false,
		},
		{
			name:      "ch-go timeout exceeded",
			err:       &ch.Exception{Code: proto.ErrTimeoutExceeded},
			retryable: true,
		},
		{
			name:      "ch-go too many simultaneous queries",
			err:       &ch.Exception{Code: proto.ErrTooManySimultaneousQueries},
			retryable: true,
		},
		{
			name:      "ch-go network error",
			err:       &ch.Exception{Code: proto.ErrNetworkError},
			retryable: true,
		},
		{
			name:      "ch-go syntax error",
			err:       &ch.Exception{Code: proto.ErrSyntaxError},
			retryable: false,
		},
		{
			name:      "corrupted data",
			err:       &compress.CorruptedDataErr{},
			retryable: false,
		},
		{
			name:      "connection reset",
			err:       syscall.ECONNRESET,
			retryable: true,
		},
		{
			name:      "connection refused",
			err:       syscall.ECONNREFUSED,
			retryable: true,
		},
		{
			name:      "broken pipe",
			err:       syscall.EPIPE,
			retryable: true,
		},
		{
			name:      "EOF",
			err:       io.EOF,
			retryable: true,
		},
		{
			name:      "unexpected EOF",
			err:       io.ErrUnexpectedEOF,
			retryable: true,
		},
		{
			name:      "network timeout",
			err:       &net.DNSError{IsTimeout: true},
			retryable: true,
		},
		{
			name:      "unknown error",
			err:       fmt.Errorf("something unexpected"),
			retryable: false,
		},
		{
			name:      "error message contains timeout",
			err:       fmt.Errorf("operation timeout after 30s"),
			retryable: true,
		},
		{
			name:      "error message contains connection reset",
			err:       fmt.Errorf("read tcp: connection reset by peer"),
			retryable: true,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			assert.Equal(t, tt.retryable, isRetryableError(tt.err))
		})
	}
}

func TestDoWithRetry_Success(t *testing.T) {
	cfg := &Config{MaxRetries: 3, RetryBaseDelay: time.Millisecond, RetryMaxDelay: 10 * time.Millisecond}

	calls := 0

	err := doWithRetry(context.Background(), cfg, "test", func(ctx context.Context) error {
		calls++

		return nil
	})

	require.NoError(t, err)
	assert.Equal(t, 1, calls)
}

func TestDoWithRetry_RetryThenSuccess(t *testing.T) {
	cfg := &Config{MaxRetries: 3, RetryBaseDelay: time.Millisecond, RetryMaxDelay: 10 * time.Millisecond}

	calls := 0

	err := doWithRetry(context.Background(), cfg, "test", func(ctx context.Context) error {
		calls++
		if calls < 3 {
			return io.EOF // retryable
		}

		return nil
	})

	require.NoError(t, err)
	assert.Equal(t, 3, calls)
}

func TestDoWithRetry_NonRetryableError(t *testing.T) {
	cfg := &Config{MaxRetries: 3, RetryBaseDelay: time.Millisecond, RetryMaxDelay: 10 * time.Millisecond}

	calls := 0

	err := doWithRetry(context.Background(), cfg, "test", func(ctx context.Context) error {
		calls++

		return fmt.Errorf("syntax error")
	})

	require.Error(t, err)
	assert.Equal(t, 1, calls)
	assert.Contains(t, err.Error(), "syntax error")
}

func TestDoWithRetry_MaxRetriesExceeded(t *testing.T) {
	cfg := &Config{MaxRetries: 2, RetryBaseDelay: time.Millisecond, RetryMaxDelay: 10 * time.Millisecond}

	calls := 0

	err := doWithRetry(context.Background(), cfg, "fetch", func(ctx context.Context) error {
		calls++

		return io.EOF
	})

	require.Error(t, err)
	assert.Equal(t, 3, calls) // initial + 2 retries
	assert.Contains(t, err.Error(), "max retries (2) exceeded")
	assert.Contains(t, err.Error(), "fetch")
}

func TestDoWithRetry_ContextCancellation(t *testing.T) {
	cfg := &Config{MaxRetries: 10, RetryBaseDelay: time.Second, RetryMaxDelay: 10 * time.Second}

	ctx, cancel := context.WithCancel(context.Background())

	calls := 0

	err := doWithRetry(ctx, cfg, "test", func(ctx context.Context) error {
		calls++

		cancel() // cancel after first attempt

		return io.EOF
	})

	require.Error(t, err)
	assert.Equal(t, 1, calls)
	assert.ErrorIs(t, err, context.Canceled)
}

func TestDoWithRetry_QueryTimeout(t *testing.T) {
	cfg := &Config{
		MaxRetries:     1,
		RetryBaseDelay: time.Millisecond,
		RetryMaxDelay:  10 * time.Millisecond,
		QueryTimeout:   50 * time.Millisecond,
	}

	err := doWithRetry(context.Background(), cfg, "test", func(ctx context.Context) error {
		deadline, ok := ctx.Deadline()
		require.True(t, ok, "expected context to have deadline")
		assert.WithinDuration(t, time.Now().Add(50*time.Millisecond), deadline, 20*time.Millisecond)

		return nil
	})

	require.NoError(t, err)
}

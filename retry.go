package clickhouselogs

import (
	"context"
	"errors"
	"fmt"
	"io"
	"net"
	"strings"
	"syscall"
	"time"

	"github.com/ClickHouse/ch-go"
	"github.com/ClickHouse/ch-go/compress"
	"github.com/ClickHouse/ch-go/proto"
)

// isRetryableError checks if an error is transient and can be retried.
func isRetryableError(err error) bool {
	if err == nil {
		return false
	}

	// Context errors should not be retried.
	if errors.Is(err, context.Canceled) || errors.Is(err, context.DeadlineExceeded) {
		return false
	}

	// Client permanently closed.
	if errors.Is(err, ch.ErrClosed) {
		return false
	}

	// ch-go server exceptions.
	if exc, ok := ch.AsException(err); ok {
		return exc.IsCode(
			proto.ErrTimeoutExceeded,
			proto.ErrNoFreeConnection,
			proto.ErrTooManySimultaneousQueries,
			proto.ErrSocketTimeout,
			proto.ErrNetworkError,
		)
	}

	// Data corruption — never retry.
	var corruptedErr *compress.CorruptedDataErr
	if errors.As(err, &corruptedErr) {
		return false
	}

	// Connection reset/refused/broken pipe.
	if errors.Is(err, syscall.ECONNRESET) ||
		errors.Is(err, syscall.ECONNREFUSED) ||
		errors.Is(err, syscall.EPIPE) ||
		errors.Is(err, io.EOF) ||
		errors.Is(err, io.ErrUnexpectedEOF) {
		return true
	}

	// Network timeout errors.
	var netErr net.Error
	if errors.As(err, &netErr) && netErr.Timeout() {
		return true
	}

	// Fallback: transient error message patterns.
	errStr := strings.ToLower(err.Error())
	transientPatterns := []string{
		"connection reset",
		"connection refused",
		"broken pipe",
		"eof",
		"timeout",
		"temporary failure",
		"server is overloaded",
		"too many connections",
	}

	for _, pattern := range transientPatterns {
		if strings.Contains(errStr, pattern) {
			return true
		}
	}

	return false
}

// doWithRetry executes a function with exponential backoff retry logic.
func doWithRetry(ctx context.Context, cfg *Config, operation string, fn func(ctx context.Context) error) error {
	var lastErr error

	for attempt := 0; attempt <= cfg.MaxRetries; attempt++ {
		if attempt > 0 {
			delay := cfg.RetryBaseDelay * time.Duration(1<<(attempt-1))
			if cfg.RetryMaxDelay > 0 && delay > cfg.RetryMaxDelay {
				delay = cfg.RetryMaxDelay
			}

			select {
			case <-ctx.Done():
				return ctx.Err()
			case <-time.After(delay):
			}
		}

		attemptCtx, cancel := withQueryTimeout(ctx, cfg.QueryTimeout)
		err := fn(attemptCtx)

		cancel()

		if err == nil {
			return nil
		}

		lastErr = err

		if !isRetryableError(err) {
			return err
		}
	}

	return fmt.Errorf("%s: max retries (%d) exceeded: %w", operation, cfg.MaxRetries, lastErr)
}

// withQueryTimeout returns a context with the configured query timeout applied.
// If the context already has a deadline or timeout is zero, the original context is returned.
func withQueryTimeout(ctx context.Context, timeout time.Duration) (context.Context, context.CancelFunc) {
	if timeout == 0 {
		return ctx, func() {}
	}

	if _, hasDeadline := ctx.Deadline(); hasDeadline {
		return ctx, func() {}
	}

	return context.WithTimeout(ctx, timeout)
}

package clickhouselogs

import (
	"testing"
	"time"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

func TestConfig_Validate(t *testing.T) {
	tests := []struct {
		name    string
		config  Config
		wantErr bool
	}{
		{
			name:    "missing addr",
			config:  Config{},
			wantErr: true,
		},
		{
			name:    "valid minimal config",
			config:  Config{Addr: "localhost:9000"},
			wantErr: false,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			err := tt.config.Validate()
			if tt.wantErr {
				require.Error(t, err)
			} else {
				require.NoError(t, err)
			}
		})
	}
}

func TestConfig_SetDefaults(t *testing.T) {
	cfg := Config{}
	cfg.SetDefaults()

	assert.Equal(t, int32(10), cfg.MaxConns)
	assert.Equal(t, int32(2), cfg.MinConns)
	assert.Equal(t, time.Hour, cfg.ConnMaxLifetime)
	assert.Equal(t, 30*time.Minute, cfg.ConnMaxIdleTime)
	assert.Equal(t, time.Minute, cfg.HealthCheckPeriod)
	assert.Equal(t, 10*time.Second, cfg.DialTimeout)
	assert.Equal(t, "lz4", cfg.Compression)
	assert.Equal(t, 3, cfg.MaxRetries)
	assert.Equal(t, 100*time.Millisecond, cfg.RetryBaseDelay)
	assert.Equal(t, 10*time.Second, cfg.RetryMaxDelay)
	assert.Equal(t, 60*time.Second, cfg.QueryTimeout)
}

func TestConfig_SetDefaults_PreservesExisting(t *testing.T) {
	cfg := Config{
		MaxConns:     20,
		QueryTimeout: 30 * time.Second,
		Compression:  "zstd",
	}
	cfg.SetDefaults()

	assert.Equal(t, int32(20), cfg.MaxConns)
	assert.Equal(t, 30*time.Second, cfg.QueryTimeout)
	assert.Equal(t, "zstd", cfg.Compression)
	// Defaults still applied to unset fields
	assert.Equal(t, int32(2), cfg.MinConns)
	assert.Equal(t, 3, cfg.MaxRetries)
}

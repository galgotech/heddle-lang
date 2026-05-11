package worker

import (
	"testing"

	"github.com/stretchr/testify/assert"
)

func TestValidateSHMPath(t *testing.T) {
	tests := []struct {
		name    string
		path    string
		wantErr bool
	}{
		{"Valid path", "/dev/shm/heddle-123.arrow", false},
		{"Outside /dev/shm", "/tmp/heddle-123.arrow", true},
		{"Path traversal", "/dev/shm/../../etc/passwd", true},
		{"Empty path", "", true},
		{"Just /dev/shm/", "/dev/shm/", false},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			err := validateSHMPath(tt.path)
			if tt.wantErr {
				assert.Error(t, err)
			} else {
				assert.NoError(t, err)
			}
		})
	}
}

package plugin

import (
	"bytes"
	"context"
	"testing"

	"github.com/stretchr/testify/assert"
)

func TestOutputWriterContext(t *testing.T) {
	ctx := context.Background()

	// Initially, GetOutputWriter should return nil
	assert.Nil(t, GetOutputWriter(ctx))
	assert.Nil(t, GetOutputWriter(nil))

	// Inject a custom writer
	buf := new(bytes.Buffer)
	ctxWithWriter := WithOutputWriter(ctx, buf)

	// Retrieve and verify
	w := GetOutputWriter(ctxWithWriter)
	assert.NotNil(t, w)
	assert.Equal(t, buf, w)

	// Write to retrieve writer and check contents
	_, err := w.Write([]byte("hello step log"))
	assert.NoError(t, err)
	assert.Equal(t, "hello step log", buf.String())
}

package transport

import (
	"context"
	"errors"
	"io"
	"testing"
	"time"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
	"google.golang.org/grpc/metadata"
)

// mockServer is a mock implementation of Server for testing.
type mockServer struct {
	doActionFunc   func(ctx context.Context, action *Action, stream ServerStream) error
	doExchangeFunc func(ctx context.Context, stream ExchangeStream) error
}

func (m *mockServer) DoAction(ctx context.Context, action *Action, stream ServerStream) error {
	if m.doActionFunc != nil {
		return m.doActionFunc(ctx, action, stream)
	}
	return nil
}

func (m *mockServer) DoExchange(ctx context.Context, stream ExchangeStream) error {
	if m.doExchangeFunc != nil {
		return m.doExchangeFunc(ctx, stream)
	}
	return nil
}

func TestNewInMemory_and_SetServer(t *testing.T) {
	srv1 := &mockServer{}
	srv2 := &mockServer{}

	inMemory := NewInMemory(srv1)
	defer inMemory.Close()
	assert.Equal(t, srv1, inMemory.getServer())

	inMemory.SetServer(srv2)
	assert.Equal(t, srv2, inMemory.getServer())

	inMemory.SetServer(nil)
	assert.Nil(t, inMemory.getServer())
}

func TestInMemory_Start(t *testing.T) {
	t.Run("nil server returns error", func(t *testing.T) {
		inMemory := NewInMemory(nil)
		defer inMemory.Close()
		ctx := context.Background()
		err := inMemory.Start(ctx)
		assert.Error(t, err)
		assert.Contains(t, err.Error(), "in-memory server not registered")
	})

	t.Run("blocks until context is done", func(t *testing.T) {
		srv := &mockServer{}
		inMemory := NewInMemory(srv)
		defer inMemory.Close()

		ctx, cancel := context.WithTimeout(context.Background(), 50*time.Millisecond)
		defer cancel()

		startErr := make(chan error, 1)
		go func() {
			startErr <- inMemory.Start(ctx)
		}()

		select {
		case err := <-startErr:
			assert.NoError(t, err) // returns nil on context cancellation
		case <-time.After(200 * time.Millisecond):
			t.Fatal("Start did not return after context cancellation")
		}
	})
}

func TestInMemory_DoAction(t *testing.T) {
	t.Run("success path with metadata propagation", func(t *testing.T) {
		srv := &mockServer{}
		inMemory := NewInMemory(srv)
		defer inMemory.Close()

		expectedAction := &Action{
			Type: "test-type",
			Body: []byte("test-body"),
		}

		expectedResult1 := &Result{Body: []byte("res-1")}
		expectedResult2 := &Result{Body: []byte("res-2")}

		var capturedCtx context.Context
		var capturedAction *Action

		srv.doActionFunc = func(ctx context.Context, action *Action, stream ServerStream) error {
			capturedCtx = ctx
			capturedAction = action

			err := stream.Send(expectedResult1)
			if err != nil {
				return err
			}
			err = stream.Send(expectedResult2)
			if err != nil {
				return err
			}
			return nil
		}

		// Set outgoing metadata to test convertContextMetadata
		md := metadata.Pairs("key1", "val1")
		ctx := metadata.NewOutgoingContext(context.Background(), md)

		stream, err := inMemory.DoAction(ctx, expectedAction)
		require.NoError(t, err)
		require.NotNil(t, stream)

		// Read first result
		res, err := stream.Recv()
		assert.NoError(t, err)
		assert.Equal(t, expectedResult1, res)

		// Read second result
		res, err = stream.Recv()
		assert.NoError(t, err)
		assert.Equal(t, expectedResult2, res)

		// Read EOF
		res, err = stream.Recv()
		assert.ErrorIs(t, err, io.EOF)
		assert.Nil(t, res)

		// Assert context conversions
		require.NotNil(t, capturedCtx)
		incomingMD, ok := metadata.FromIncomingContext(capturedCtx)
		assert.True(t, ok)
		assert.Equal(t, []string{"val1"}, incomingMD.Get("key1"))

		// Assert correct action was received
		assert.Equal(t, expectedAction, capturedAction)
	})

	t.Run("server error propagation", func(t *testing.T) {
		srv := &mockServer{}
		inMemory := NewInMemory(srv)
		defer inMemory.Close()

		expectedErr := errors.New("something went wrong on server")
		srv.doActionFunc = func(ctx context.Context, action *Action, stream ServerStream) error {
			return expectedErr
		}

		stream, err := inMemory.DoAction(context.Background(), &Action{})
		require.NoError(t, err)
		require.NotNil(t, stream)

		res, err := stream.Recv()
		assert.Error(t, err)
		assert.Contains(t, err.Error(), expectedErr.Error())
		assert.Nil(t, res)
	})

	t.Run("nil server during execution", func(t *testing.T) {
		inMemory := NewInMemory(nil)
		defer inMemory.Close()

		stream, err := inMemory.DoAction(context.Background(), &Action{})
		require.NoError(t, err)
		require.NotNil(t, stream)

		res, err := stream.Recv()
		assert.Error(t, err)
		assert.Contains(t, err.Error(), "in-memory server not registered")
		assert.Nil(t, res)
	})
}

func TestInMemory_DoExchange(t *testing.T) {
	t.Run("success path", func(t *testing.T) {
		srv := &mockServer{}
		inMemory := NewInMemory(srv)
		defer inMemory.Close()

		var serverRecvData []*FlightData
		srv.doExchangeFunc = func(ctx context.Context, stream ExchangeStream) error {
			// Receive 2 flight datas from client
			for i := 0; i < 2; i++ {
				data, err := stream.Recv()
				if err != nil {
					return err
				}
				serverRecvData = append(serverRecvData, data)
			}

			// Send 1 flight data back to client
			err := stream.Send(&FlightData{
				AppMetadata: []byte("resp-meta"),
				DataBody:    []byte("resp-body"),
			})
			if err != nil {
				return err
			}
			return nil
		}

		clientStream, err := inMemory.DoExchange(context.Background())
		require.NoError(t, err)
		defer clientStream.(*inMemoryExchangeStream).Close()

		// Send 2 datasets from client
		err = clientStream.Send(&FlightData{AppMetadata: []byte("c-meta-1"), DataBody: []byte("c-body-1")})
		assert.NoError(t, err)

		err = clientStream.Send(&FlightData{AppMetadata: []byte("c-meta-2"), DataBody: []byte("c-body-2")})
		assert.NoError(t, err)

		// Receive response on client
		resp, err := clientStream.Recv()
		assert.NoError(t, err)
		assert.Equal(t, []byte("resp-meta"), resp.AppMetadata)
		assert.Equal(t, []byte("resp-body"), resp.DataBody)

		// Server should have received correct datasets
		assert.Len(t, serverRecvData, 2)
		assert.Equal(t, []byte("c-meta-1"), serverRecvData[0].AppMetadata)
		assert.Equal(t, []byte("c-meta-2"), serverRecvData[1].AppMetadata)
	})

	t.Run("nil server handles gracefully", func(t *testing.T) {
		inMemory := NewInMemory(nil)
		defer inMemory.Close()

		clientStream, err := inMemory.DoExchange(context.Background())
		require.NoError(t, err)
		defer clientStream.(*inMemoryExchangeStream).Close()

		resp, err := clientStream.Recv()
		assert.Error(t, err)
		assert.Nil(t, resp)
	})

	t.Run("context cancellation during exchange", func(t *testing.T) {
		srv := &mockServer{}
		inMemory := NewInMemory(srv)
		defer inMemory.Close()

		ctx, cancel := context.WithCancel(context.Background())

		srv.doExchangeFunc = func(ctx context.Context, stream ExchangeStream) error {
			<-ctx.Done()
			return ctx.Err()
		}

		clientStream, err := inMemory.DoExchange(ctx)
		require.NoError(t, err)
		defer clientStream.(*inMemoryExchangeStream).Close()

		cancel()

		resp, err := clientStream.Recv()
		assert.Error(t, err)
		assert.Nil(t, resp)
	})
}

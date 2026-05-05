package transport

import (
	"context"

	"github.com/apache/arrow/go/v18/arrow/flight"
	"google.golang.org/grpc"
)

// NetworkTransport defines the abstraction for Arrow Flight and gRPC communication.
// This allows the Worker to be tested by mocking the network layer, enabling
// deterministic interception and faking of Arrow data streams.
type NetworkTransport interface {
	// DoAction executes a Flight Action (e.g., registration, heartbeats).
	DoAction(ctx context.Context, action *flight.Action, opts ...grpc.CallOption) (flight.FlightService_DoActionClient, error)

	// DoExchange opens a bidirectional stream for control signals and task updates.
	DoExchange(ctx context.Context, opts ...grpc.CallOption) (flight.FlightService_DoExchangeClient, error)

	// DoGet requests a specific Arrow RecordBatch from a remote peer.
	DoGet(ctx context.Context, ticket *flight.Ticket, opts ...grpc.CallOption) (flight.FlightService_DoGetClient, error)

	// Close terminates the underlying network connection.
	Close() error
}

// FlightTransport is the production implementation of NetworkTransport using actual gRPC/Flight.
type FlightTransport struct {
	client flight.Client
	conn   *grpc.ClientConn
}

func NewFlightTransport(conn *grpc.ClientConn) *FlightTransport {
	return &FlightTransport{
		client: flight.NewClientFromConn(conn, nil),
		conn:   conn,
	}
}

func (t *FlightTransport) DoAction(ctx context.Context, action *flight.Action, opts ...grpc.CallOption) (flight.FlightService_DoActionClient, error) {
	return t.client.DoAction(ctx, action, opts...)
}

func (t *FlightTransport) DoExchange(ctx context.Context, opts ...grpc.CallOption) (flight.FlightService_DoExchangeClient, error) {
	return t.client.DoExchange(ctx, opts...)
}

func (t *FlightTransport) DoGet(ctx context.Context, ticket *flight.Ticket, opts ...grpc.CallOption) (flight.FlightService_DoGetClient, error) {
	return t.client.DoGet(ctx, ticket, opts...)
}

func (t *FlightTransport) Close() error {
	return t.conn.Close()
}

package transport

import (
	"context"
)

// Action represents a request sent by a client.
type Action struct {
	Type string
	Body []byte
}

// Result represents the result of an action.
type Result struct {
	Body []byte
}

// FlightData represents data sent/received over a stream.
type FlightData struct {
	AppMetadata []byte
	DataBody    []byte
}

// ExchangeStream abstracts bi-directional stream communication.
type ExchangeStream interface {
	Send(*FlightData) error
	Recv() (*FlightData, error)
}

// ResultStream abstracts result streams from actions.
type ResultStream interface {
	Recv() (*Result, error)
}

// Client abstracts a control plane client.
type Client interface {
	DoAction(ctx context.Context, action *Action) (ResultStream, error)
	DoExchange(ctx context.Context) (ExchangeStream, error)
}

// ServerStream abstracts server-side stream for action results.
type ServerStream interface {
	Send(*Result) error
}

// Server abstracts the control plane server.
type Server interface {
	DoAction(ctx context.Context, action *Action, stream ServerStream) error
	DoExchange(ctx context.Context, stream ExchangeStream) error
}

// ServerTransport abstracts a transport mechanism that binds to a Server.
type ServerTransport interface {
	SetServer(srv Server)
}


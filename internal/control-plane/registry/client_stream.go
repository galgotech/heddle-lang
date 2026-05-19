package registry

import (
	"github.com/apache/arrow/go/v18/arrow/flight"
)

type ClientStream struct {
	stream flight.FlightService_DoExchangeServer
}

func NewClientStream(stream flight.FlightService_DoExchangeServer) *ClientStream {
	return &ClientStream{
		stream: stream,
	}
}

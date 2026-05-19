package registry

import (
	"github.com/apache/arrow/go/v18/arrow/flight"

	"github.com/galgotech/heddle-lang/internal/models"
)

type ClientStream struct {
	stream flight.FlightService_DoActionServer
}

func (c *ClientStream) Send(result *models.TaskResult) error {
	return c.stream.Send(&flight.Result{Value: result.ToBytes()})
}

func NewClientStream(stream flight.FlightService_DoExchangeServer) *ClientStream {
	return &ClientStream{
		stream: stream,
	}
}

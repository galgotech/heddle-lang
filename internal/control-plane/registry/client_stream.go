package registry

import (
	"github.com/galgotech/heddle-lang/pkg/transport"
)

type ClientStream struct {
	stream transport.ExchangeStream
}

func NewClientStream(stream transport.ExchangeStream) *ClientStream {
	return &ClientStream{
		stream: stream,
	}
}

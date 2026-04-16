package natsrpc

import "fmt"

type RemoteError struct {
	Code    int32
	Message string
}

func (e *RemoteError) Error() string {
	if e == nil {
		return ""
	}
	if e.Code == 0 {
		return e.Message
	}
	return fmt.Sprintf("nats rpc remote error(code=%d): %s", e.Code, e.Message)
}

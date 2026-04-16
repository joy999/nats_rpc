package natsrpc

import "testing"

func TestRemoteErrorError(t *testing.T) {
	tests := []struct {
		name string
		err  *RemoteError
		want string
	}{
		{name: "nil", err: nil, want: ""},
		{name: "message only", err: &RemoteError{Message: "boom"}, want: "boom"},
		{name: "code and message", err: &RemoteError{Code: 9, Message: "boom"}, want: "nats rpc remote error(code=9): boom"},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			if got := tt.err.Error(); got != tt.want {
				t.Fatalf("RemoteError.Error() = %q, want %q", got, tt.want)
			}
		})
	}
}

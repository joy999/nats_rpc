package natsrpc

import (
	"context"
	"errors"
	"testing"

	natsrpcv1 "server/nats_rpc/proto/natsrpc/v1"
	"server/nats_rpc/registry"

	"google.golang.org/protobuf/proto"
	"google.golang.org/protobuf/types/known/wrapperspb"
)

func mustMarshal(t *testing.T, msg proto.Message) []byte {
	t.Helper()
	b, err := proto.Marshal(msg)
	if err != nil {
		t.Fatalf("marshal failed: %v", err)
	}
	return b
}

func TestRegisterNotifyAndHandleNotify(t *testing.T) {
	r := NewRouter(registry.New())
	const msgID = 100
	called := false
	want := "hello"

	err := r.RegisterNotify(msgID, &wrapperspb.StringValue{}, func(_ context.Context, message proto.Message) error {
		called = true
		sv, ok := message.(*wrapperspb.StringValue)
		if !ok {
			t.Fatalf("decoded message type = %T", message)
		}
		if got := sv.Value; got != want {
			t.Fatalf("decoded value = %q, want %q", got, want)
		}
		return nil
	})
	if err != nil {
		t.Fatalf("RegisterNotify failed: %v", err)
	}

	env := &natsrpcv1.Envelope{MsgId: msgID, Body: mustMarshal(t, &wrapperspb.StringValue{Value: want})}
	if err = r.HandleNotify(context.Background(), env); err != nil {
		t.Fatalf("HandleNotify failed: %v", err)
	}
	if !called {
		t.Fatal("notify handler was not called")
	}
}

func TestRegisterRPCAndHandleRPC(t *testing.T) {
	r := NewRouter(registry.New())
	const reqID = 200
	const rspID = 201

	err := r.RegisterRPC(reqID, &wrapperspb.StringValue{}, rspID, &wrapperspb.Int32Value{}, func(_ context.Context, request proto.Message) (proto.Message, error) {
		req, ok := request.(*wrapperspb.StringValue)
		if !ok {
			t.Fatalf("decoded request type = %T", request)
		}
		return &wrapperspb.Int32Value{Value: int32(len(req.Value))}, nil
	})
	if err != nil {
		t.Fatalf("RegisterRPC failed: %v", err)
	}

	reqEnv := &natsrpcv1.Envelope{MsgId: reqID, RpcId: 22, TraceId: "t-1", Body: mustMarshal(t, &wrapperspb.StringValue{Value: "ping"})}
	respEnv, err := r.HandleRPC(context.Background(), reqEnv)
	if err != nil {
		t.Fatalf("HandleRPC failed: %v", err)
	}
	if respEnv.MsgId != rspID || respEnv.RpcId != 22 || respEnv.TraceId != "t-1" {
		t.Fatalf("unexpected response envelope metadata: %+v", respEnv)
	}
	respPayload := &wrapperspb.Int32Value{}
	if err = proto.Unmarshal(respEnv.Body, respPayload); err != nil {
		t.Fatalf("unmarshal response body failed: %v", err)
	}
	if got := respPayload.Value; got != 4 {
		t.Fatalf("response value = %d, want %d", got, 4)
	}
}

func TestHandleRPCReturnsEmptyBodyWhenHandlerReturnsNilMessage(t *testing.T) {
	r := NewRouter(registry.New())
	const reqID = 300
	const rspID = 301

	err := r.RegisterRPC(reqID, &wrapperspb.StringValue{}, rspID, &wrapperspb.Int32Value{}, func(context.Context, proto.Message) (proto.Message, error) {
		return nil, nil
	})
	if err != nil {
		t.Fatalf("RegisterRPC failed: %v", err)
	}

	reqEnv := &natsrpcv1.Envelope{MsgId: reqID, RpcId: 99, TraceId: "trace", Body: mustMarshal(t, &wrapperspb.StringValue{Value: "x"})}
	respEnv, err := r.HandleRPC(context.Background(), reqEnv)
	if err != nil {
		t.Fatalf("HandleRPC failed: %v", err)
	}
	if len(respEnv.Body) != 0 {
		t.Fatalf("response body length = %d, want 0", len(respEnv.Body))
	}
}

func TestRouterErrors(t *testing.T) {
	r := NewRouter(registry.New())
	if err := r.RegisterNotify(1, &wrapperspb.StringValue{}, nil); err == nil {
		t.Fatal("expected error for nil notify handler")
	}
	if err := r.RegisterRPC(2, &wrapperspb.StringValue{}, 3, &wrapperspb.Int32Value{}, nil); err == nil {
		t.Fatal("expected error for nil rpc handler")
	}

	if err := r.HandleNotify(context.Background(), &natsrpcv1.Envelope{MsgId: 42}); err == nil {
		t.Fatal("expected missing notify handler error")
	}
	if _, err := r.HandleRPC(context.Background(), &natsrpcv1.Envelope{MsgId: 42}); err == nil {
		t.Fatal("expected missing rpc handler error")
	}

	const dupNotifyID = 50
	if err := r.RegisterNotify(dupNotifyID, &wrapperspb.StringValue{}, func(context.Context, proto.Message) error { return nil }); err != nil {
		t.Fatalf("first RegisterNotify failed: %v", err)
	}
	if err := r.RegisterNotify(dupNotifyID, &wrapperspb.StringValue{}, func(context.Context, proto.Message) error { return nil }); err == nil {
		t.Fatal("expected duplicate notify registration error")
	}

	const dupRPCID = 60
	if err := r.RegisterRPC(dupRPCID, &wrapperspb.BoolValue{}, 61, &wrapperspb.Int32Value{}, func(context.Context, proto.Message) (proto.Message, error) {
		return nil, errors.New("x")
	}); err != nil {
		t.Fatalf("first RegisterRPC failed: %v", err)
	}
	if err := r.RegisterRPC(dupRPCID, &wrapperspb.BoolValue{}, 61, &wrapperspb.Int32Value{}, func(context.Context, proto.Message) (proto.Message, error) {
		return nil, nil
	}); err == nil {
		t.Fatal("expected duplicate rpc registration error")
	}
}

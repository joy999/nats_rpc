package natsrpc

import (
	"context"
	"testing"

	natsrpcv1 "server/nats_rpc/proto/natsrpc/v1"
	"server/nats_rpc/registry"

	"google.golang.org/protobuf/proto"
	"google.golang.org/protobuf/types/known/wrapperspb"
)

func TestRPCOneRequestOneResponse(t *testing.T) {
	reg := registry.New()
	r := NewRouter(reg)

	const reqID uint32 = 8001
	const rspID uint32 = 8002

	if err := r.RegisterRPC(reqID, &wrapperspb.StringValue{}, rspID, &wrapperspb.Int32Value{}, func(_ context.Context, request proto.Message) (proto.Message, error) {
		req := request.(*wrapperspb.StringValue)
		return &wrapperspb.Int32Value{Value: int32(len(req.Value))}, nil
	}); err != nil {
		t.Fatalf("register rpc failed: %v", err)
	}

	reqEnv := &natsrpcv1.Envelope{
		MsgId:   reqID,
		RpcId:   77,
		TraceId: "trace-77",
		Body:    mustMarshal(t, &wrapperspb.StringValue{Value: "abcdef"}),
	}

	respEnv, err := r.HandleRPC(context.Background(), reqEnv)
	if err != nil {
		t.Fatalf("handle rpc failed: %v", err)
	}

	if respEnv.MsgId != rspID {
		t.Fatalf("response msg_id = %d, want %d", respEnv.MsgId, rspID)
	}
	if respEnv.RpcId != reqEnv.RpcId {
		t.Fatalf("response rpc_id = %d, want %d", respEnv.RpcId, reqEnv.RpcId)
	}
	if respEnv.TraceId != reqEnv.TraceId {
		t.Fatalf("response trace_id = %q, want %q", respEnv.TraceId, reqEnv.TraceId)
	}

	respBody := &wrapperspb.Int32Value{}
	if err = proto.Unmarshal(respEnv.Body, respBody); err != nil {
		t.Fatalf("unmarshal rpc response body failed: %v", err)
	}
	if respBody.Value != 6 {
		t.Fatalf("response value = %d, want 6", respBody.Value)
	}
}

func TestNotifyOneWayNoResponse(t *testing.T) {
	reg := registry.New()
	r := NewRouter(reg)

	const notifyID uint32 = 9001
	received := make(chan string, 1)
	if err := r.RegisterNotify(notifyID, &wrapperspb.StringValue{}, func(_ context.Context, message proto.Message) error {
		received <- message.(*wrapperspb.StringValue).Value
		return nil
	}); err != nil {
		t.Fatalf("register notify failed: %v", err)
	}

	notifyEnv := &natsrpcv1.Envelope{
		MsgId: notifyID,
		Body:  mustMarshal(t, &wrapperspb.StringValue{Value: "one-way"}),
	}

	if err := r.HandleNotify(context.Background(), notifyEnv); err != nil {
		t.Fatalf("handle notify failed: %v", err)
	}

	select {
	case got := <-received:
		if got != "one-way" {
			t.Fatalf("notify payload = %q, want %q", got, "one-way")
		}
	default:
		t.Fatal("notify handler was not invoked")
	}
}

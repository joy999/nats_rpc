package natsrpc

import (
	"context"
	"fmt"

	natsrpcv1 "server/nats_rpc/proto/natsrpc/v1"
	"server/nats_rpc/registry"

	"google.golang.org/protobuf/proto"
	"google.golang.org/protobuf/types/known/wrapperspb"
)

func ExampleRouter_rpcRequestReply() {
	reg := registry.New()
	r := NewRouter(reg)

	_ = r.RegisterRPC(1, &wrapperspb.StringValue{}, 2, &wrapperspb.Int32Value{}, func(_ context.Context, request proto.Message) (proto.Message, error) {
		msg := request.(*wrapperspb.StringValue)
		return &wrapperspb.Int32Value{Value: int32(len(msg.Value))}, nil
	})

	req := &natsrpcv1.Envelope{MsgId: 1, RpcId: 101, Body: mustMarshalForExample(&wrapperspb.StringValue{Value: "abcd"})}
	resp, _ := r.HandleRPC(context.Background(), req)

	out := &wrapperspb.Int32Value{}
	_ = proto.Unmarshal(resp.Body, out)
	fmt.Println(out.Value)
	// Output: 4
}

func ExampleRouter_notifyOneWay() {
	reg := registry.New()
	r := NewRouter(reg)

	received := ""
	_ = r.RegisterNotify(10, &wrapperspb.StringValue{}, func(_ context.Context, message proto.Message) error {
		received = message.(*wrapperspb.StringValue).Value
		return nil
	})

	notify := &natsrpcv1.Envelope{MsgId: 10, Body: mustMarshalForExample(&wrapperspb.StringValue{Value: "hello-notify"})}
	_ = r.HandleNotify(context.Background(), notify)

	fmt.Println(received)
	// Output: hello-notify
}

func mustMarshalForExample(msg proto.Message) []byte {
	b, _ := proto.Marshal(msg)
	return b
}

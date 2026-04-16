package goframe

import (
	"context"

	"github.com/gogf/gf/v2/net/gtrace"
	"github.com/gogf/gf/v2/os/gctx"

	natsrpc "server/nats_rpc"
	natsrpcv1 "server/nats_rpc/proto/natsrpc/v1"
)

var _ natsrpc.TracePropagator = (*TracePropagator)(nil)

type TracePropagator struct{}

func NewTracePropagator() *TracePropagator {
	return &TracePropagator{}
}

func (t *TracePropagator) Inject(ctx context.Context, env *natsrpcv1.Envelope) {
	if env == nil {
		return
	}
	if traceID := gctx.CtxId(ctx); traceID != "" {
		env.TraceId = traceID
	}
}

func (t *TracePropagator) Extract(ctx context.Context, env *natsrpcv1.Envelope) (context.Context, error) {
	if env == nil || env.TraceId == "" {
		return ctx, nil
	}
	return gtrace.WithTraceID(ctx, env.TraceId)
}

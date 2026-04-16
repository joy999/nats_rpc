package natsrpc

import (
	"context"
	"fmt"

	natsrpcv1 "github.com/joy999/nats_rpc/proto/natsrpc/v1"

	"github.com/nats-io/nats.go"
	"google.golang.org/protobuf/proto"
)

type NotifyHandler func(ctx context.Context, env *natsrpcv1.Envelope) error

type RPCHandler func(ctx context.Context, env *natsrpcv1.Envelope) (*natsrpcv1.Envelope, error)

type Server struct {
	conn *nats.Conn
	cfg  config
}

func NewServer(conn *nats.Conn, opts ...Option) *Server {
	cfg := defaultConfig()
	for _, opt := range opts {
		opt(&cfg)
	}
	return &Server{
		conn: conn,
		cfg:  cfg,
	}
}

func (s *Server) SubscribeNotify(ctx context.Context, subject string, handler NotifyHandler) (*Subscription, error) {
	return s.subscribeNotify(ctx, subject, "", handler)
}

func (s *Server) QueueSubscribeNotify(ctx context.Context, subject, queue string, handler NotifyHandler) (*Subscription, error) {
	return s.subscribeNotify(ctx, subject, queue, handler)
}

func (s *Server) SubscribeRPC(ctx context.Context, subject string, handler RPCHandler) (*Subscription, error) {
	return s.subscribeRPC(ctx, subject, "", handler)
}

func (s *Server) QueueSubscribeRPC(ctx context.Context, subject, queue string, handler RPCHandler) (*Subscription, error) {
	return s.subscribeRPC(ctx, subject, queue, handler)
}

func (s *Server) Close() error {
	return s.cfg.subscriptionSet.unsubscribeAll()
}

func (s *Server) subscribeNotify(ctx context.Context, subject, queue string, handler NotifyHandler) (*Subscription, error) {
	if handler == nil {
		return nil, fmt.Errorf("subscribe notify failed: nil handler")
	}

	cb := func(msg *nats.Msg) {
		env := &natsrpcv1.Envelope{}
		if err := proto.Unmarshal(msg.Data, env); err != nil {
			s.cfg.asyncError(ctx, err)
			return
		}

		callCtx := ctx
		if s.cfg.trace != nil {
			nextCtx, err := s.cfg.trace.Extract(ctx, env)
			if err != nil {
				s.cfg.asyncError(ctx, err)
				return
			}
			callCtx = nextCtx
		}

		if err := handler(callCtx, env); err != nil {
			s.cfg.asyncError(callCtx, err)
		}
	}

	var (
		raw *nats.Subscription
		err error
	)
	if queue != "" {
		raw, err = s.conn.QueueSubscribe(subject, queue, cb)
	} else {
		raw, err = s.conn.Subscribe(subject, cb)
	}
	if err != nil {
		return nil, err
	}
	sub := &Subscription{inner: raw, set: s.cfg.subscriptionSet}
	s.cfg.subscriptionSet.add(sub)
	return sub, nil
}

func (s *Server) subscribeRPC(ctx context.Context, subject, queue string, handler RPCHandler) (*Subscription, error) {
	if handler == nil {
		return nil, fmt.Errorf("subscribe rpc failed: nil handler")
	}

	cb := func(msg *nats.Msg) {
		req := &natsrpcv1.Envelope{}
		if err := proto.Unmarshal(msg.Data, req); err != nil {
			s.cfg.asyncError(ctx, err)
			_ = msg.Respond(s.mustMarshalErrorResponse(req, err))
			return
		}

		callCtx := ctx
		if s.cfg.trace != nil {
			nextCtx, err := s.cfg.trace.Extract(ctx, req)
			if err != nil {
				s.cfg.asyncError(ctx, err)
				_ = msg.Respond(s.mustMarshalErrorResponse(req, err))
				return
			}
			callCtx = nextCtx
		}

		resp, err := handler(callCtx, req)
		if err != nil {
			_ = msg.Respond(s.mustMarshalErrorResponse(req, err))
			return
		}
		if resp == nil {
			resp = &natsrpcv1.Envelope{}
		}
		if resp.RpcId == 0 {
			resp.RpcId = req.RpcId
		}
		if resp.TraceId == "" {
			resp.TraceId = req.TraceId
		}
		data, marshalErr := proto.Marshal(resp)
		if marshalErr != nil {
			s.cfg.asyncError(callCtx, marshalErr)
			_ = msg.Respond(s.mustMarshalErrorResponse(req, marshalErr))
			return
		}
		if respondErr := msg.Respond(data); respondErr != nil {
			s.cfg.asyncError(callCtx, respondErr)
		}
	}

	var (
		raw *nats.Subscription
		err error
	)
	if queue != "" {
		raw, err = s.conn.QueueSubscribe(subject, queue, cb)
	} else {
		raw, err = s.conn.Subscribe(subject, cb)
	}
	if err != nil {
		return nil, err
	}
	sub := &Subscription{inner: raw, set: s.cfg.subscriptionSet}
	s.cfg.subscriptionSet.add(sub)
	return sub, nil
}

func (s *Server) mustMarshalErrorResponse(req *natsrpcv1.Envelope, err error) []byte {
	code, message := s.cfg.errorEncoder(err)
	resp := &natsrpcv1.Envelope{
		Code:    code,
		Message: message,
	}
	if req != nil {
		resp.MsgId = req.MsgId
		resp.RpcId = req.RpcId
		resp.TraceId = req.TraceId
	}
	data, marshalErr := proto.Marshal(resp)
	if marshalErr == nil {
		return data
	}
	fallback := &natsrpcv1.Envelope{
		Code:    1,
		Message: marshalErr.Error(),
	}
	if req != nil {
		fallback.MsgId = req.MsgId
		fallback.RpcId = req.RpcId
		fallback.TraceId = req.TraceId
	}
	data, _ = proto.Marshal(fallback)
	return data
}

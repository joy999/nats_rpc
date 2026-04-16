package natsrpc

import (
	"context"
	"fmt"
	"sync"

	natsrpcv1 "server/nats_rpc/proto/natsrpc/v1"
	"server/nats_rpc/registry"

	"google.golang.org/protobuf/proto"
)

type NotifyMessageHandler func(ctx context.Context, message proto.Message) error

type RPCMessageHandler func(ctx context.Context, request proto.Message) (proto.Message, error)

type Router struct {
	registry *registry.Registry

	mu             sync.RWMutex
	notifyHandlers map[uint32]notifyRoute
	rpcHandlers    map[uint32]rpcRoute
}

type notifyRoute struct {
	handler NotifyMessageHandler
}

type rpcRoute struct {
	responseID uint32
	handler    RPCMessageHandler
}

func NewRouter(reg *registry.Registry) *Router {
	if reg == nil {
		reg = registry.New()
	}
	return &Router{
		registry:       reg,
		notifyHandlers: make(map[uint32]notifyRoute),
		rpcHandlers:    make(map[uint32]rpcRoute),
	}
}

func (r *Router) Registry() *registry.Registry {
	return r.registry
}

func (r *Router) RegisterNotify(msgID uint32, prototype proto.Message, handler NotifyMessageHandler) error {
	if handler == nil {
		return fmt.Errorf("register notify failed: nil handler")
	}
	if err := r.registry.Register(msgID, prototype); err != nil {
		return err
	}

	r.mu.Lock()
	defer r.mu.Unlock()
	if _, ok := r.notifyHandlers[msgID]; ok {
		return fmt.Errorf("register notify failed: msg_id %d already exists", msgID)
	}
	r.notifyHandlers[msgID] = notifyRoute{handler: handler}
	return nil
}

func (r *Router) RegisterNotifyMessage(prototype proto.Message, handler NotifyMessageHandler) error {
	msgID, ok := r.registry.MessageID(prototype)
	if !ok {
		return fmt.Errorf("register notify message failed: message %T is not registered", prototype)
	}
	return r.RegisterNotify(msgID, prototype, handler)
}

func (r *Router) RegisterRPC(requestID uint32, requestPrototype proto.Message, responseID uint32, responsePrototype proto.Message, handler RPCMessageHandler) error {
	if handler == nil {
		return fmt.Errorf("register rpc failed: nil handler")
	}
	if err := r.registry.Register(requestID, requestPrototype); err != nil {
		return err
	}
	if responsePrototype != nil {
		if err := r.registry.Register(responseID, responsePrototype); err != nil {
			return err
		}
	}

	r.mu.Lock()
	defer r.mu.Unlock()
	if _, ok := r.rpcHandlers[requestID]; ok {
		return fmt.Errorf("register rpc failed: msg_id %d already exists", requestID)
	}
	r.rpcHandlers[requestID] = rpcRoute{
		responseID: responseID,
		handler:    handler,
	}
	return nil
}

func (r *Router) RegisterRPCMessage(requestPrototype proto.Message, responsePrototype proto.Message, handler RPCMessageHandler) error {
	requestID, ok := r.registry.MessageID(requestPrototype)
	if !ok {
		return fmt.Errorf("register rpc message failed: request %T is not registered", requestPrototype)
	}
	responseID, ok := r.registry.MessageID(responsePrototype)
	if !ok {
		return fmt.Errorf("register rpc message failed: response %T is not registered", responsePrototype)
	}
	return r.RegisterRPC(requestID, requestPrototype, responseID, responsePrototype, handler)
}

func (r *Router) HandleNotify(ctx context.Context, env *natsrpcv1.Envelope) error {
	msg, route, err := r.decodeNotify(env)
	if err != nil {
		return err
	}
	return route.handler(ctx, msg)
}

func (r *Router) HandleRPC(ctx context.Context, env *natsrpcv1.Envelope) (*natsrpcv1.Envelope, error) {
	msg, route, err := r.decodeRPC(env)
	if err != nil {
		return nil, err
	}

	respMessage, err := route.handler(ctx, msg)
	if err != nil {
		return nil, err
	}
	if respMessage == nil {
		return &natsrpcv1.Envelope{
			MsgId:   route.responseID,
			RpcId:   env.GetRpcId(),
			TraceId: env.GetTraceId(),
		}, nil
	}

	body, err := proto.Marshal(respMessage)
	if err != nil {
		return nil, err
	}
	return &natsrpcv1.Envelope{
		MsgId:   route.responseID,
		RpcId:   env.GetRpcId(),
		Body:    body,
		TraceId: env.GetTraceId(),
	}, nil
}

func (r *Router) decodeNotify(env *natsrpcv1.Envelope) (proto.Message, notifyRoute, error) {
	r.mu.RLock()
	route, ok := r.notifyHandlers[env.GetMsgId()]
	r.mu.RUnlock()
	if !ok {
		return nil, notifyRoute{}, fmt.Errorf("notify handler not found: msg_id %d", env.GetMsgId())
	}

	msg, err := r.registry.NewMessage(env.GetMsgId())
	if err != nil {
		return nil, notifyRoute{}, err
	}
	if err = proto.Unmarshal(env.GetBody(), msg); err != nil {
		return nil, notifyRoute{}, err
	}
	return msg, route, nil
}

func (r *Router) decodeRPC(env *natsrpcv1.Envelope) (proto.Message, rpcRoute, error) {
	r.mu.RLock()
	route, ok := r.rpcHandlers[env.GetMsgId()]
	r.mu.RUnlock()
	if !ok {
		return nil, rpcRoute{}, fmt.Errorf("rpc handler not found: msg_id %d", env.GetMsgId())
	}

	msg, err := r.registry.NewMessage(env.GetMsgId())
	if err != nil {
		return nil, rpcRoute{}, err
	}
	if err = proto.Unmarshal(env.GetBody(), msg); err != nil {
		return nil, rpcRoute{}, err
	}
	return msg, route, nil
}

package natsrpc

import (
	"context"
	"fmt"

	natsrpcv1 "server/nats_rpc/proto/natsrpc/v1"

	"github.com/nats-io/nats.go"
	"google.golang.org/protobuf/proto"
)

type Client struct {
	conn *nats.Conn
	cfg  config
}

func NewClient(conn *nats.Conn, opts ...Option) *Client {
	cfg := defaultConfig()
	for _, opt := range opts {
		opt(&cfg)
	}
	return &Client{
		conn: conn,
		cfg:  cfg,
	}
}

func (c *Client) PublishEnvelope(ctx context.Context, subject string, env *natsrpcv1.Envelope) error {
	if env == nil {
		return fmt.Errorf("publish envelope failed: nil envelope")
	}
	if c.cfg.trace != nil {
		c.cfg.trace.Inject(ctx, env)
	}
	data, err := proto.Marshal(env)
	if err != nil {
		return err
	}
	return c.conn.Publish(subject, data)
}

func (c *Client) Notify(ctx context.Context, subject string, msgID uint32, message proto.Message) error {
	body, err := proto.Marshal(message)
	if err != nil {
		return err
	}
	return c.PublishEnvelope(ctx, subject, &natsrpcv1.Envelope{
		MsgId: msgID,
		Body:  body,
	})
}

func (c *Client) NotifyMessage(ctx context.Context, subject string, message proto.Message) error {
	if c.cfg.registry == nil {
		return fmt.Errorf("notify message failed: registry is nil")
	}
	msgID, ok := c.cfg.registry.MessageID(message)
	if !ok {
		return fmt.Errorf("notify message failed: message %T is not registered", message)
	}
	return c.Notify(ctx, subject, msgID, message)
}

func (c *Client) RequestEnvelope(ctx context.Context, subject string, env *natsrpcv1.Envelope) (*natsrpcv1.Envelope, error) {
	if env == nil {
		return nil, fmt.Errorf("request envelope failed: nil envelope")
	}
	if c.cfg.trace != nil {
		c.cfg.trace.Inject(ctx, env)
	}

	data, err := proto.Marshal(env)
	if err != nil {
		return nil, err
	}

	reqCtx, cancel := context.WithTimeout(ctx, c.cfg.timeout)
	defer cancel()

	msg, err := c.conn.RequestWithContext(reqCtx, subject, data)
	if err != nil {
		return nil, err
	}

	resp := &natsrpcv1.Envelope{}
	if err = proto.Unmarshal(msg.Data, resp); err != nil {
		return nil, err
	}
	return resp, nil
}

func (c *Client) Request(ctx context.Context, subject string, msgID uint32, req proto.Message, rsp proto.Message) error {
	body, err := proto.Marshal(req)
	if err != nil {
		return err
	}

	env, err := c.RequestEnvelope(ctx, subject, &natsrpcv1.Envelope{
		MsgId: msgID,
		Body:  body,
	})
	if err != nil {
		return err
	}
	if env.Code != 0 {
		return &RemoteError{
			Code:    env.Code,
			Message: env.Message,
		}
	}
	if rsp == nil || len(env.Body) == 0 {
		return nil
	}
	return proto.Unmarshal(env.Body, rsp)
}

func (c *Client) Call(ctx context.Context, subject string, req proto.Message, rsp proto.Message) error {
	if c.cfg.registry == nil {
		return fmt.Errorf("call failed: registry is nil")
	}
	msgID, ok := c.cfg.registry.MessageID(req)
	if !ok {
		return fmt.Errorf("call failed: message %T is not registered", req)
	}
	return c.Request(ctx, subject, msgID, req, rsp)
}

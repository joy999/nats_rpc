package natsrpc

import (
	"context"
	"time"

	natsrpcv1 "server/nats_rpc/proto/natsrpc/v1"
	"server/nats_rpc/registry"
)

type TracePropagator interface {
	Inject(ctx context.Context, env *natsrpcv1.Envelope)
	Extract(ctx context.Context, env *natsrpcv1.Envelope) (context.Context, error)
}

type AsyncErrorHandler func(ctx context.Context, err error)

type ErrorEncoder func(err error) (code int32, message string)

type Option func(*config)

type config struct {
	timeout         time.Duration
	registry        *registry.Registry
	trace           TracePropagator
	asyncError      AsyncErrorHandler
	errorEncoder    ErrorEncoder
	subscriptionSet *subscriptionSet
}

func defaultConfig() config {
	return config{
		timeout:         30 * time.Second,
		registry:        nil,
		trace:           nil,
		asyncError:      func(context.Context, error) {},
		errorEncoder:    defaultErrorEncoder,
		subscriptionSet: newSubscriptionSet(),
	}
}

func WithTimeout(timeout time.Duration) Option {
	return func(cfg *config) {
		if timeout > 0 {
			cfg.timeout = timeout
		}
	}
}

func WithRegistry(reg *registry.Registry) Option {
	return func(cfg *config) {
		cfg.registry = reg
	}
}

func WithTracePropagator(trace TracePropagator) Option {
	return func(cfg *config) {
		cfg.trace = trace
	}
}

func WithAsyncErrorHandler(handler AsyncErrorHandler) Option {
	return func(cfg *config) {
		if handler != nil {
			cfg.asyncError = handler
		}
	}
}

func WithErrorEncoder(encoder ErrorEncoder) Option {
	return func(cfg *config) {
		if encoder != nil {
			cfg.errorEncoder = encoder
		}
	}
}

func defaultErrorEncoder(err error) (code int32, message string) {
	if err == nil {
		return 0, ""
	}
	return 1, err.Error()
}

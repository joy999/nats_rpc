package natsrpc

import (
	"context"
	"errors"
	"testing"
	"time"

	natsrpcv1 "github.com/joy999/nats_rpc/proto/natsrpc/v1"
	"github.com/joy999/nats_rpc/registry"
)

type mockTrace struct{}

func (m mockTrace) Inject(context.Context, *natsrpcv1.Envelope) {}
func (m mockTrace) Extract(ctx context.Context, _ *natsrpcv1.Envelope) (context.Context, error) {
	return ctx, nil
}

func TestDefaultConfig(t *testing.T) {
	cfg := defaultConfig()
	if cfg.timeout != 30*time.Second {
		t.Fatalf("default timeout = %s, want 30s", cfg.timeout)
	}
	if cfg.asyncError == nil {
		t.Fatal("default asyncError should not be nil")
	}
	if cfg.errorEncoder == nil {
		t.Fatal("default errorEncoder should not be nil")
	}
	if cfg.subscriptionSet == nil {
		t.Fatal("default subscriptionSet should not be nil")
	}
}

func TestOptionsApply(t *testing.T) {
	cfg := defaultConfig()
	reg := registry.New()
	trace := mockTrace{}
	called := false
	async := func(context.Context, error) { called = true }
	encoder := func(err error) (int32, string) {
		if err == nil {
			return 0, ""
		}
		return 7, "encoded: " + err.Error()
	}

	WithTimeout(2 * time.Second)(&cfg)
	WithRegistry(reg)(&cfg)
	WithTracePropagator(trace)(&cfg)
	WithAsyncErrorHandler(async)(&cfg)
	WithErrorEncoder(encoder)(&cfg)

	if cfg.timeout != 2*time.Second {
		t.Fatalf("timeout = %s, want 2s", cfg.timeout)
	}
	if cfg.registry != reg {
		t.Fatal("registry option not applied")
	}
	if cfg.trace != trace {
		t.Fatal("trace option not applied")
	}

	cfg.asyncError(context.Background(), errors.New("x"))
	if !called {
		t.Fatal("async error handler was not called")
	}

	code, msg := cfg.errorEncoder(errors.New("x"))
	if code != 7 || msg != "encoded: x" {
		t.Fatalf("error encoder = (%d, %q), want (7, %q)", code, msg, "encoded: x")
	}
}

func TestWithTimeoutIgnoresNonPositive(t *testing.T) {
	cfg := defaultConfig()
	orig := cfg.timeout
	WithTimeout(0)(&cfg)
	WithTimeout(-time.Second)(&cfg)
	if cfg.timeout != orig {
		t.Fatalf("timeout changed to %s, want %s", cfg.timeout, orig)
	}
}

func TestNilOptionInputsDoNotOverrideDefaults(t *testing.T) {
	cfg := defaultConfig()
	origAsync := cfg.asyncError
	origEncoder := cfg.errorEncoder

	WithAsyncErrorHandler(nil)(&cfg)
	WithErrorEncoder(nil)(&cfg)

	if cfg.asyncError == nil || cfg.errorEncoder == nil {
		t.Fatal("nil options should not clear defaults")
	}
	_ = origAsync
	_ = origEncoder
}

func TestDefaultErrorEncoder(t *testing.T) {
	if code, msg := defaultErrorEncoder(nil); code != 0 || msg != "" {
		t.Fatalf("defaultErrorEncoder(nil) = (%d, %q), want (0, \"\")", code, msg)
	}
	err := errors.New("boom")
	if code, msg := defaultErrorEncoder(err); code != 1 || msg != "boom" {
		t.Fatalf("defaultErrorEncoder(err) = (%d, %q), want (1, \"boom\")", code, msg)
	}
}

package natsrpc

import (
	"context"
	"testing"
	"time"
)

func TestServerRegisterServiceAndStop(t *testing.T) {
	d := newDiscoveryWithKV(newMemKV(), AllowAllAuthenticator{})
	s := &Server{cfg: defaultConfig()}

	reg, err := s.RegisterService(context.Background(), d, ServiceInstance{
		Service:    "svc.mail",
		InstanceID: "m1",
		Subject:    "svc.mail.m1",
	}, time.Minute, 0)
	if err != nil {
		t.Fatalf("register service failed: %v", err)
	}

	if _, err = d.FindInstance(context.Background(), "svc.mail", "m1"); err != nil {
		t.Fatalf("find registered instance failed: %v", err)
	}

	if err = reg.Stop(context.Background()); err != nil {
		t.Fatalf("stop registration failed: %v", err)
	}
	if _, err = d.FindInstance(context.Background(), "svc.mail", "m1"); err == nil {
		t.Fatal("expected instance removed after stop")
	}
}

func TestServerRegisterServiceValidation(t *testing.T) {
	s := &Server{cfg: defaultConfig()}
	if _, err := s.RegisterService(context.Background(), nil, ServiceInstance{}, time.Second, time.Millisecond); err == nil {
		t.Fatal("expected nil discovery error")
	}
	if _, err := s.RegisterService(context.Background(), newDiscoveryWithKV(newMemKV(), AllowAllAuthenticator{}), ServiceInstance{Service: "s"}, time.Second, time.Millisecond); err == nil {
		t.Fatal("expected invalid instance error")
	}
}

package registry

import (
	"reflect"
	"testing"

	natsrpcv1 "server/nats_rpc/proto/natsrpc/v1"
)

func TestRegistryRegisterAndLookup(t *testing.T) {
	r := New()
	const msgID = 10
	if err := r.Register(msgID, &natsrpcv1.Envelope{}); err != nil {
		t.Fatalf("Register failed: %v", err)
	}

	gotID, ok := r.MessageID(&natsrpcv1.Envelope{})
	if !ok || gotID != msgID {
		t.Fatalf("MessageID = (%d, %t), want (%d, true)", gotID, ok, msgID)
	}

	msg, err := r.NewMessage(msgID)
	if err != nil {
		t.Fatalf("NewMessage failed: %v", err)
	}
	if _, ok = msg.(*natsrpcv1.Envelope); !ok {
		t.Fatalf("NewMessage type = %T, want *natsrpcv1.Envelope", msg)
	}

	typ, ok := r.Type(msgID)
	if !ok || typ != reflect.TypeOf(natsrpcv1.Envelope{}) {
		t.Fatalf("Type = (%v, %t), want (%v, true)", typ, ok, reflect.TypeOf(natsrpcv1.Envelope{}))
	}
}

func TestRegistryErrorsAndMustRegister(t *testing.T) {
	r := New()
	if err := r.Register(1, nil); err == nil {
		t.Fatal("expected error for nil prototype")
	}
	if _, ok := r.MessageID(nil); ok {
		t.Fatal("MessageID(nil) should return ok=false")
	}
	if _, err := r.NewMessage(999); err == nil {
		t.Fatal("expected error for unknown msgID")
	}

	r.MustRegister(2, &natsrpcv1.Envelope{})
	defer func() {
		if recover() == nil {
			t.Fatal("MustRegister should panic on conflicting registration")
		}
	}()
	r.MustRegister(3, &natsrpcv1.Envelope{})
}

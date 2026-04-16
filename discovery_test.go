package natsrpc

import (
	"context"
	"errors"
	"sort"
	"sync"
	"testing"
	"time"
)

type memKVEntry struct{ value []byte }

func (e memKVEntry) Value() []byte { return e.value }

type memKV struct {
	mu   sync.Mutex
	data map[string][]byte
}

func newMemKV() *memKV { return &memKV{data: make(map[string][]byte)} }

func (m *memKV) Put(key string, value []byte) (uint64, error) {
	m.mu.Lock()
	defer m.mu.Unlock()
	cp := make([]byte, len(value))
	copy(cp, value)
	m.data[key] = cp
	return uint64(len(m.data)), nil
}

func (m *memKV) Get(key string) (keyValueEntry, error) {
	m.mu.Lock()
	defer m.mu.Unlock()
	v, ok := m.data[key]
	if !ok {
		return nil, errors.New("not found")
	}
	cp := make([]byte, len(v))
	copy(cp, v)
	return memKVEntry{value: cp}, nil
}

func (m *memKV) Delete(key string) error {
	m.mu.Lock()
	defer m.mu.Unlock()
	delete(m.data, key)
	return nil
}

func (m *memKV) Keys() ([]string, error) {
	m.mu.Lock()
	defer m.mu.Unlock()
	keys := make([]string, 0, len(m.data))
	for k := range m.data {
		keys = append(keys, k)
	}
	sort.Strings(keys)
	return keys, nil
}

type denyAuth struct{}

func (denyAuth) AuthorizeRegister(context.Context, ServiceInstance) error {
	return errors.New("denied")
}
func (denyAuth) AuthorizeResolve(context.Context, string) error { return errors.New("denied") }

func TestDiscoveryRegisterListPickAndFind(t *testing.T) {
	d := newDiscoveryWithKV(newMemKV(), AllowAllAuthenticator{})
	ctx := context.Background()

	if err := d.Register(ctx, ServiceInstance{Service: "svc.user", InstanceID: "a", Subject: "svc.user.a"}, 2*time.Minute); err != nil {
		t.Fatalf("register a failed: %v", err)
	}
	if err := d.Register(ctx, ServiceInstance{Service: "svc.user", InstanceID: "b", Subject: "svc.user.b"}, 2*time.Minute); err != nil {
		t.Fatalf("register b failed: %v", err)
	}

	list, err := d.List(ctx, "svc.user")
	if err != nil {
		t.Fatalf("list failed: %v", err)
	}
	if len(list) != 2 {
		t.Fatalf("list size = %d, want 2", len(list))
	}
	if list[0].InstanceID != "a" || list[1].InstanceID != "b" {
		t.Fatalf("unexpected list order: %+v", list)
	}

	picked1, err := d.Pick(ctx, "svc.user")
	if err != nil {
		t.Fatalf("pick1 failed: %v", err)
	}
	picked2, err := d.Pick(ctx, "svc.user")
	if err != nil {
		t.Fatalf("pick2 failed: %v", err)
	}
	if picked1.InstanceID == picked2.InstanceID {
		t.Fatalf("round-robin should rotate instances, got %q twice", picked1.InstanceID)
	}

	ins, err := d.FindInstance(ctx, "svc.user", "a")
	if err != nil {
		t.Fatalf("find instance failed: %v", err)
	}
	if ins.Subject != "svc.user.a" {
		t.Fatalf("instance subject = %q, want %q", ins.Subject, "svc.user.a")
	}
}

func TestDiscoveryHeartbeatAndDeregister(t *testing.T) {
	d := newDiscoveryWithKV(newMemKV(), AllowAllAuthenticator{})
	ctx := context.Background()

	if err := d.Register(ctx, ServiceInstance{Service: "svc.pay", InstanceID: "x", Subject: "svc.pay.x"}, time.Second); err != nil {
		t.Fatalf("register failed: %v", err)
	}
	before, err := d.FindInstance(ctx, "svc.pay", "x")
	if err != nil {
		t.Fatalf("find before heartbeat failed: %v", err)
	}

	time.Sleep(10 * time.Millisecond)
	if err = d.Heartbeat(ctx, "svc.pay", "x", 2*time.Second); err != nil {
		t.Fatalf("heartbeat failed: %v", err)
	}
	after, err := d.FindInstance(ctx, "svc.pay", "x")
	if err != nil {
		t.Fatalf("find after heartbeat failed: %v", err)
	}
	if !after.HeartbeatAt.After(before.HeartbeatAt) {
		t.Fatalf("heartbeat timestamp did not move forward: before=%v after=%v", before.HeartbeatAt, after.HeartbeatAt)
	}

	if err = d.Deregister(ctx, "svc.pay", "x"); err != nil {
		t.Fatalf("deregister failed: %v", err)
	}
	if _, err = d.FindInstance(ctx, "svc.pay", "x"); err == nil {
		t.Fatal("expected error after deregister")
	}
}

func TestDiscoveryExpiredInstanceFiltered(t *testing.T) {
	d := newDiscoveryWithKV(newMemKV(), AllowAllAuthenticator{})
	ctx := context.Background()
	if err := d.Register(ctx, ServiceInstance{Service: "svc.exp", InstanceID: "e1", Subject: "svc.exp.e1"}, 30*time.Millisecond); err != nil {
		t.Fatalf("register failed: %v", err)
	}
	time.Sleep(60 * time.Millisecond)
	list, err := d.List(ctx, "svc.exp")
	if err != nil {
		t.Fatalf("list failed: %v", err)
	}
	if len(list) != 0 {
		t.Fatalf("expired instance should be filtered, got %d", len(list))
	}
	if _, err = d.Pick(ctx, "svc.exp"); !errors.Is(err, ErrNoServiceInstances) {
		t.Fatalf("pick err = %v, want ErrNoServiceInstances", err)
	}
}

func TestDiscoveryAuth(t *testing.T) {
	d := newDiscoveryWithKV(newMemKV(), denyAuth{})
	ctx := context.Background()
	if err := d.Register(ctx, ServiceInstance{Service: "svc.sec", InstanceID: "i", Subject: "s"}, time.Minute); err == nil {
		t.Fatal("expected register denied")
	}
	if _, err := d.List(ctx, "svc.sec"); err == nil {
		t.Fatal("expected list denied")
	}
}

package natsrpc

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"sort"
	"strings"
	"sync"
	"time"

	"github.com/nats-io/nats.go"
)

const defaultDiscoveryBucket = "natsrpc_services"

var ErrNoServiceInstances = errors.New("no service instances available")

type ServiceAuthenticator interface {
	AuthorizeRegister(ctx context.Context, instance ServiceInstance) error
	AuthorizeResolve(ctx context.Context, service string) error
}

type AllowAllAuthenticator struct{}

func (AllowAllAuthenticator) AuthorizeRegister(context.Context, ServiceInstance) error { return nil }
func (AllowAllAuthenticator) AuthorizeResolve(context.Context, string) error           { return nil }

type ServiceInstance struct {
	Service     string            `json:"service"`
	InstanceID  string            `json:"instance_id"`
	Subject     string            `json:"subject"`
	Version     string            `json:"version,omitempty"`
	Metadata    map[string]string `json:"metadata,omitempty"`
	HeartbeatAt time.Time         `json:"heartbeat_at"`
	ExpiresAt   time.Time         `json:"expires_at,omitempty"`
}

type discoveryConfig struct {
	bucket        string
	authenticator ServiceAuthenticator
}

type DiscoveryOption func(*discoveryConfig)

func defaultDiscoveryConfig() discoveryConfig {
	return discoveryConfig{
		bucket:        defaultDiscoveryBucket,
		authenticator: AllowAllAuthenticator{},
	}
}

func WithDiscoveryBucket(bucket string) DiscoveryOption {
	return func(cfg *discoveryConfig) {
		if bucket != "" {
			cfg.bucket = bucket
		}
	}
}

func WithServiceAuthenticator(auth ServiceAuthenticator) DiscoveryOption {
	return func(cfg *discoveryConfig) {
		if auth != nil {
			cfg.authenticator = auth
		}
	}
}

type keyValueStore interface {
	Put(key string, value []byte) (uint64, error)
	Get(key string) (keyValueEntry, error)
	Delete(key string) error
	Keys() ([]string, error)
}

type keyValueEntry interface {
	Value() []byte
}

type natsKV struct{ inner nats.KeyValue }

type natsKVEntry struct{ inner nats.KeyValueEntry }

func (e natsKVEntry) Value() []byte { return e.inner.Value() }

func (k natsKV) Put(key string, value []byte) (uint64, error) { return k.inner.Put(key, value) }
func (k natsKV) Get(key string) (keyValueEntry, error) {
	entry, err := k.inner.Get(key)
	if err != nil {
		return nil, err
	}
	return natsKVEntry{inner: entry}, nil
}
func (k natsKV) Delete(key string) error { return k.inner.Delete(key) }
func (k natsKV) Keys() ([]string, error) { return k.inner.Keys() }

type Discovery struct {
	kv            keyValueStore
	authenticator ServiceAuthenticator

	mu      sync.Mutex
	rrIndex map[string]uint64
}

func NewDiscovery(conn *nats.Conn, opts ...DiscoveryOption) (*Discovery, error) {
	if conn == nil {
		return nil, fmt.Errorf("new discovery failed: nil conn")
	}
	cfg := defaultDiscoveryConfig()
	for _, opt := range opts {
		opt(&cfg)
	}

	js, err := conn.JetStream()
	if err != nil {
		return nil, err
	}
	kv, err := js.KeyValue(cfg.bucket)
	if err != nil {
		if !errors.Is(err, nats.ErrBucketNotFound) {
			return nil, err
		}
		kv, err = js.CreateKeyValue(&nats.KeyValueConfig{Bucket: cfg.bucket})
		if err != nil {
			return nil, err
		}
	}
	return newDiscoveryWithKV(natsKV{inner: kv}, cfg.authenticator), nil
}

func newDiscoveryWithKV(kv keyValueStore, auth ServiceAuthenticator) *Discovery {
	if auth == nil {
		auth = AllowAllAuthenticator{}
	}
	return &Discovery{
		kv:            kv,
		authenticator: auth,
		rrIndex:       make(map[string]uint64),
	}
}

func (d *Discovery) Register(ctx context.Context, instance ServiceInstance, ttl time.Duration) error {
	if err := d.authenticator.AuthorizeRegister(ctx, instance); err != nil {
		return err
	}
	if instance.Service == "" || instance.InstanceID == "" || instance.Subject == "" {
		return fmt.Errorf("register service failed: service/instance_id/subject are required")
	}
	now := time.Now().UTC()
	instance.HeartbeatAt = now
	if ttl > 0 {
		instance.ExpiresAt = now.Add(ttl)
	} else {
		instance.ExpiresAt = time.Time{}
	}
	return d.putInstance(instance)
}

func (d *Discovery) Heartbeat(ctx context.Context, service, instanceID string, ttl time.Duration) error {
	if service == "" || instanceID == "" {
		return fmt.Errorf("heartbeat failed: service and instance_id are required")
	}
	instance, err := d.FindInstance(ctx, service, instanceID)
	if err != nil {
		return err
	}
	if err = d.authenticator.AuthorizeRegister(ctx, instance); err != nil {
		return err
	}
	now := time.Now().UTC()
	instance.HeartbeatAt = now
	if ttl > 0 {
		instance.ExpiresAt = now.Add(ttl)
	}
	return d.putInstance(instance)
}

func (d *Discovery) Deregister(ctx context.Context, service, instanceID string) error {
	if service == "" || instanceID == "" {
		return fmt.Errorf("deregister failed: service and instance_id are required")
	}
	if err := d.authenticator.AuthorizeResolve(ctx, service); err != nil {
		return err
	}
	return d.kv.Delete(instanceKey(service, instanceID))
}

func (d *Discovery) FindInstance(ctx context.Context, service, instanceID string) (ServiceInstance, error) {
	if service == "" || instanceID == "" {
		return ServiceInstance{}, fmt.Errorf("find instance failed: service and instance_id are required")
	}
	if err := d.authenticator.AuthorizeResolve(ctx, service); err != nil {
		return ServiceInstance{}, err
	}
	entry, err := d.kv.Get(instanceKey(service, instanceID))
	if err != nil {
		return ServiceInstance{}, err
	}
	instance := ServiceInstance{}
	if err = json.Unmarshal(entry.Value(), &instance); err != nil {
		return ServiceInstance{}, err
	}
	if isExpired(instance) {
		return ServiceInstance{}, ErrNoServiceInstances
	}
	return instance, nil
}

func (d *Discovery) List(ctx context.Context, service string) ([]ServiceInstance, error) {
	if service == "" {
		return nil, fmt.Errorf("list service failed: service is required")
	}
	if err := d.authenticator.AuthorizeResolve(ctx, service); err != nil {
		return nil, err
	}
	keys, err := d.kv.Keys()
	if err != nil {
		return nil, err
	}
	prefix := service + "/"
	instances := make([]ServiceInstance, 0)
	for _, key := range keys {
		if !strings.HasPrefix(key, prefix) {
			continue
		}
		entry, getErr := d.kv.Get(key)
		if getErr != nil {
			continue
		}
		instance := ServiceInstance{}
		if unmarshalErr := json.Unmarshal(entry.Value(), &instance); unmarshalErr != nil {
			continue
		}
		if !isExpired(instance) {
			instances = append(instances, instance)
		}
	}
	sort.Slice(instances, func(i, j int) bool {
		if instances[i].Service != instances[j].Service {
			return instances[i].Service < instances[j].Service
		}
		return instances[i].InstanceID < instances[j].InstanceID
	})
	return instances, nil
}

func (d *Discovery) Pick(ctx context.Context, service string) (ServiceInstance, error) {
	instances, err := d.List(ctx, service)
	if err != nil {
		return ServiceInstance{}, err
	}
	if len(instances) == 0 {
		return ServiceInstance{}, ErrNoServiceInstances
	}
	d.mu.Lock()
	idx := d.rrIndex[service] % uint64(len(instances))
	d.rrIndex[service]++
	d.mu.Unlock()
	return instances[idx], nil
}

func (d *Discovery) putInstance(instance ServiceInstance) error {
	payload, err := json.Marshal(instance)
	if err != nil {
		return err
	}
	_, err = d.kv.Put(instanceKey(instance.Service, instance.InstanceID), payload)
	return err
}

func instanceKey(service, instanceID string) string {
	return service + "/" + instanceID
}

func isExpired(instance ServiceInstance) bool {
	return !instance.ExpiresAt.IsZero() && instance.ExpiresAt.Before(time.Now().UTC())
}

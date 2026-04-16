package natsrpc

import (
	"context"
	"fmt"
	"sync"
	"time"
)

type ServiceRegistration struct {
	discovery  *Discovery
	service    string
	instanceID string
	cancel     context.CancelFunc
	once       sync.Once
}

func (r *ServiceRegistration) Stop(ctx context.Context) error {
	if r == nil {
		return nil
	}
	var err error
	r.once.Do(func() {
		if r.cancel != nil {
			r.cancel()
		}
		if r.discovery != nil && r.service != "" && r.instanceID != "" {
			err = r.discovery.Deregister(ctx, r.service, r.instanceID)
		}
	})
	return err
}

func (s *Server) RegisterService(ctx context.Context, discovery *Discovery, instance ServiceInstance, ttl time.Duration, heartbeatInterval time.Duration) (*ServiceRegistration, error) {
	if discovery == nil {
		return nil, fmt.Errorf("register service failed: discovery is nil")
	}
	if instance.Service == "" || instance.InstanceID == "" || instance.Subject == "" {
		return nil, fmt.Errorf("register service failed: service/instance_id/subject are required")
	}
	if err := discovery.Register(ctx, instance, ttl); err != nil {
		return nil, err
	}

	bgCtx, cancel := context.WithCancel(context.Background())
	reg := &ServiceRegistration{
		discovery:  discovery,
		service:    instance.Service,
		instanceID: instance.InstanceID,
		cancel:     cancel,
	}

	if ttl > 0 {
		if heartbeatInterval <= 0 || heartbeatInterval >= ttl {
			heartbeatInterval = ttl / 2
			if heartbeatInterval <= 0 {
				heartbeatInterval = time.Second
			}
		}
		go func() {
			ticker := time.NewTicker(heartbeatInterval)
			defer ticker.Stop()
			for {
				select {
				case <-bgCtx.Done():
					return
				case <-ticker.C:
					if err := discovery.Heartbeat(bgCtx, instance.Service, instance.InstanceID, ttl); err != nil {
						s.cfg.asyncError(ctx, err)
					}
				}
			}
		}()
	}
	return reg, nil
}

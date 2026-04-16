package natsrpc

import (
	"sync"

	"github.com/nats-io/nats.go"
)

type Subscription struct {
	inner *nats.Subscription
	set   *subscriptionSet
}

func (s *Subscription) Unsubscribe() error {
	if s == nil || s.inner == nil {
		return nil
	}
	err := s.inner.Unsubscribe()
	if s.set != nil {
		s.set.remove(s)
	}
	return err
}

type subscriptionSet struct {
	mu   sync.Mutex
	subs map[*Subscription]struct{}
}

func newSubscriptionSet() *subscriptionSet {
	return &subscriptionSet{
		subs: make(map[*Subscription]struct{}),
	}
}

func (s *subscriptionSet) add(sub *Subscription) {
	s.mu.Lock()
	defer s.mu.Unlock()
	s.subs[sub] = struct{}{}
}

func (s *subscriptionSet) remove(sub *Subscription) {
	s.mu.Lock()
	defer s.mu.Unlock()
	delete(s.subs, sub)
}

func (s *subscriptionSet) unsubscribeAll() error {
	s.mu.Lock()
	subs := make([]*Subscription, 0, len(s.subs))
	for sub := range s.subs {
		subs = append(subs, sub)
	}
	s.mu.Unlock()

	var firstErr error
	for _, sub := range subs {
		if err := sub.Unsubscribe(); err != nil && firstErr == nil {
			firstErr = err
		}
	}
	return firstErr
}

package registry

import (
	"fmt"
	"reflect"
	"sync"

	"google.golang.org/protobuf/proto"
)

type Registry struct {
	mu     sync.RWMutex
	byID   map[uint32]reflect.Type
	byType map[string]uint32
}

func New() *Registry {
	return &Registry{
		byID:   make(map[uint32]reflect.Type),
		byType: make(map[string]uint32),
	}
}

func (r *Registry) Register(msgID uint32, prototype proto.Message) error {
	if prototype == nil {
		return fmt.Errorf("registry register failed: nil prototype")
	}
	rt := reflect.TypeOf(prototype)
	for rt.Kind() == reflect.Pointer {
		rt = rt.Elem()
	}
	if rt.Kind() != reflect.Struct {
		return fmt.Errorf("registry register failed: prototype %T is not a struct message", prototype)
	}

	key := typeKey(rt)

	r.mu.Lock()
	defer r.mu.Unlock()

	if old, ok := r.byID[msgID]; ok && old != rt {
		return fmt.Errorf("registry register failed: msg_id %d already registered to %s", msgID, typeKey(old))
	}
	if oldID, ok := r.byType[key]; ok && oldID != msgID {
		return fmt.Errorf("registry register failed: message %s already registered to msg_id %d", key, oldID)
	}

	r.byID[msgID] = rt
	r.byType[key] = msgID
	return nil
}

func (r *Registry) MustRegister(msgID uint32, prototype proto.Message) {
	if err := r.Register(msgID, prototype); err != nil {
		panic(err)
	}
}

func (r *Registry) MessageID(message proto.Message) (uint32, bool) {
	if message == nil {
		return 0, false
	}
	rt := reflect.TypeOf(message)
	for rt.Kind() == reflect.Pointer {
		rt = rt.Elem()
	}

	r.mu.RLock()
	defer r.mu.RUnlock()
	msgID, ok := r.byType[typeKey(rt)]
	return msgID, ok
}

func (r *Registry) NewMessage(msgID uint32) (proto.Message, error) {
	r.mu.RLock()
	rt, ok := r.byID[msgID]
	r.mu.RUnlock()
	if !ok {
		return nil, fmt.Errorf("registry new message failed: msg_id %d is not registered", msgID)
	}

	msg, ok := reflect.New(rt).Interface().(proto.Message)
	if !ok {
		return nil, fmt.Errorf("registry new message failed: type %s is not a proto.Message", typeKey(rt))
	}
	return msg, nil
}

func (r *Registry) Type(msgID uint32) (reflect.Type, bool) {
	r.mu.RLock()
	defer r.mu.RUnlock()
	rt, ok := r.byID[msgID]
	return rt, ok
}

func typeKey(rt reflect.Type) string {
	if pkg := rt.PkgPath(); pkg != "" {
		return pkg + "." + rt.Name()
	}
	return rt.Name()
}

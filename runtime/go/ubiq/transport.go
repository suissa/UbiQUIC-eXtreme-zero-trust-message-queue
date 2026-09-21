package ubiq

import (
	"context"
	"errors"
	"fmt"
	"sort"
	"sync"
)

type Capabilities struct {
	Reliable            bool `json:"reliable"`
	Ordered             bool `json:"ordered"`
	Durable             bool `json:"durable"`
	Replay              bool `json:"replay"`
	Bidirectional       bool `json:"bidirectional"`
	ConsumerGroups      bool `json:"consumer_groups"`
	Streaming           bool `json:"streaming"`
	ExecutionSettlement bool `json:"execution_settlement"`
}

type Requirement struct {
	Reliable            bool
	Ordered             bool
	Durable             bool
	Replay              bool
	Bidirectional       bool
	ConsumerGroups      bool
	Streaming           bool
	ExecutionSettlement bool
}

func (c Capabilities) Satisfies(r Requirement) error {
	if r.Reliable && !c.Reliable { return errors.New("reliable delivery required") }
	if r.Ordered && !c.Ordered { return errors.New("ordering required") }
	if r.Durable && !c.Durable { return errors.New("durability required") }
	if r.Replay && !c.Replay { return errors.New("replay required") }
	if r.Bidirectional && !c.Bidirectional { return errors.New("bidirectional transport required") }
	if r.ConsumerGroups && !c.ConsumerGroups { return errors.New("consumer groups required") }
	if r.Streaming && !c.Streaming { return errors.New("streaming required") }
	if r.ExecutionSettlement && !c.ExecutionSettlement { return errors.New("execution settlement required") }
	return nil
}

type Handler func(context.Context, Envelope) error

type Subscription interface { Close() error }

type Transport interface {
	Name() string
	Capabilities() Capabilities
	Publish(context.Context, Envelope) error
	Subscribe(context.Context, string, Handler) (Subscription, error)
	Close() error
}

type Registry struct { mu sync.RWMutex; transports map[string]Transport }
func NewRegistry() *Registry { return &Registry{transports: make(map[string]Transport)} }
func (r *Registry) Register(t Transport) error {
	if t == nil || t.Name() == "" { return errors.New("transport with name required") }
	r.mu.Lock(); defer r.mu.Unlock()
	if _, exists := r.transports[t.Name()]; exists { return fmt.Errorf("transport %q already registered", t.Name()) }
	r.transports[t.Name()] = t; return nil
}
func (r *Registry) Get(name string) (Transport, error) {
	r.mu.RLock(); defer r.mu.RUnlock(); t, ok := r.transports[name]
	if !ok { return nil, fmt.Errorf("unknown transport %q", name) }; return t, nil
}
func (r *Registry) Close() error {
	r.mu.Lock(); defer r.mu.Unlock(); var first error
	for _, t := range r.transports { if err := t.Close(); err != nil && first == nil { first = err } }; return first
}

type Route struct { Ingress, Backbone, Egress string; Requires Requirement }
func (r *Registry) ValidateRoute(route Route) error {
	names := []string{route.Ingress, route.Backbone, route.Egress}; hasDurable, hasSettlement := false, false
	for _, name := range names {
		if name == "" { continue }; t, err := r.Get(name); if err != nil { return err }; c := t.Capabilities()
		hasDurable = hasDurable || c.Durable; hasSettlement = hasSettlement || c.ExecutionSettlement
	}
	if route.Requires.Durable && !hasDurable { return errors.New("route has no durable stage") }
	if route.Requires.ExecutionSettlement && !hasSettlement { return errors.New("route has no execution-settlement stage") }
	return nil
}
func (r *Registry) Names() []string {
	r.mu.RLock(); defer r.mu.RUnlock(); out := make([]string,0,len(r.transports)); for name := range r.transports { out=append(out,name) }; sort.Strings(out); return out
}

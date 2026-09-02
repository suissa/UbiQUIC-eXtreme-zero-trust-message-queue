// Code generated from schemas/universal-adapter.schema.yml. DO NOT EDIT.
package universaladapter

import "context"

type Health string

const (
	HealthHealthy   Health = "healthy"
	HealthDegraded  Health = "degraded"
	HealthUnhealthy Health = "unhealthy"
)

type Envelope struct {
	CanonicalEvent string
	MessageID      string
	InteractionID  string
	CorrelationID  string
	Payload        []byte
}

type Delivery struct {
	DeliveryID     string
	CanonicalEvent string
	TransportName  string
}

type Capabilities struct {
	Reliable     bool
	Ordered      bool
	Durable      bool
	Replay       bool
	RequestReply bool
	PubSub       bool
}

type UniversalAdapter interface {
	Open(ctx context.Context, configYAML []byte) error
	Close(ctx context.Context) error
	Publish(ctx context.Context, envelope Envelope) error
	Subscribe(ctx context.Context, canonicalEvent string, handler func(context.Context, Envelope, Delivery) error) error
	Ack(ctx context.Context, delivery Delivery) error
	Nack(ctx context.Context, delivery Delivery, reason string) error
	Health(ctx context.Context) Health
	Capabilities() Capabilities
	MapCanonicalToTransport(canonicalEvent string) (string, error)
	MapTransportToCanonical(transportName string) (string, error)
}

type AdapterFactory func() UniversalAdapter

type AdapterRegistry map[string]AdapterFactory

func AssertCanonicalRoundTrip(adapter UniversalAdapter, canonical string) error {
	mapped, err := adapter.MapCanonicalToTransport(canonical)
	if err != nil { return err }
	restored, err := adapter.MapTransportToCanonical(mapped)
	if err != nil { return err }
	if restored != canonical { return ErrMappingNotReversible{Canonical: canonical, Transport: mapped, Restored: restored} }
	return nil
}

type ErrMappingNotReversible struct { Canonical, Transport, Restored string }
func (e ErrMappingNotReversible) Error() string { return "universal adapter event mapping is not reversible" }

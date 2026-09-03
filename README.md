# UbiQ

UbiQ is a universal semantic messaging runtime for AllasCode/H2A2H. The same typed event can be received through one protocol, routed through a different durable backbone and delivered through another protocol without changing its semantic identity.

`REST -> JetStream -> WebSocket`

`Memory -> QUIC -> Kafka`

`NATS -> RabbitMQ -> SSE`

The transport is an implementation detail. Domain code sees only the canonical event.

## Canonical event language

The minimum canonical form is:

`{Entity}.{action_or_intent}.{state}`

where `state` is one of:

- `request`
- `ok`
- `error`

Examples:

- `Financial.createInvoice.request`
- `Financial.createInvoice.ok`
- `Financial.createInvoice.error`

The runtime chooses the internal pipeline from this nominal event type before inspecting the domain payload:

- `request` -> execution
- `ok` -> consume
- `error` -> self-healing

## Why UbiQ

UbiQ means ubiquitous messaging language. The canonical event is stable across:

- bounded in-memory messaging;
- TCP;
- QUIC;
- NATS;
- NATS JetStream;
- Kafka;
- Redpanda;
- RabbitMQ;
- REST/HTTP;
- WebSocket;
- SSE.

The existing Universal Adapter schema also leaves extension points for MQTT, gRPC, MCP, A2A, AP2, BullMQ and Redis Streams.

A transport adapter may map the event to a subject, topic, stream, queue or route, but the mapping must preserve the exact canonical event in the envelope. Kafka does not need one topic per domain event; it may use `ubiq.events` while the event identity remains `Financial.createInvoice.ok`.

## Execution Settlement

UbiQ deliberately separates transport receipt from proof of execution.

```text
PUBLISHED
   |
   v
LEASED(worker A)
   |
   v
RECEIVED(worker A)   <- transport/consumer ACK is not completion
   |
   v
worker executes effect
   |
   +------> SETTLED_OK
   |
   +------> SETTLED_ERROR
```

If the lease expires without settlement, the event becomes eligible for another worker.

For `processed` delivery, UbiQ targets:

`at-least-once delivery + idempotency + leases + explicit execution settlement`

It does not make a false transport-independent `exactly-once` claim.

## Bounded in-memory broker

`MemoryBroker(N)` is allocation-free after construction. Capacity is compile-time bounded and publication is idempotent by `idempotency_key`.

This is the same semantic contract used for local Actor-to-Actor delivery. Moving the receiver to another process/server changes the adapter, not the event language.

## Capability-aware bridges

Every adapter declares real capabilities such as:

- reliable
- ordered
- durable
- replay
- bidirectional
- native ACK
- consumer groups
- streaming

Routes are validated before use.

Example:

```text
REST ingress
    |
    v
JetStream durable backbone
    |
    v
WebSocket egress
```

A `processed` event can use this route because durability belongs to JetStream. REST and WebSocket are not incorrectly advertised as durable logs.

## eXtreme Zero Trust

The XZT policy layer models:

- mTLS peer authentication;
- replay defense;
- UbiQ Proof-of-Possession for transport-neutral messaging;
- HTTP DPoP only on HTTP-compatible boundaries;
- hybrid X25519 + ML-KEM policy;
- hybrid Ed25519 + ML-DSA policy;
- XChaCha20-Poly1305 / AES-GCM-SIV message protection policy;
- LinearAutoDestroy one-shot execution capabilities.

Cryptographic policy and cryptographic implementation are intentionally separate. The runtime no longer pretends that hashing a transcript is equivalent to Kyber/ML-KEM or a real DPoP proof. Production crypto must be supplied by a reviewed provider.

## LinearAutoDestroy

LinearAutoDestroy consumes execution authority, not audit history.

Once the one-shot capability has been used, the same capability cannot execute the event again. The EventStore/audit record may remain available indefinitely.

## Schemas

- `schemas/event-envelope.schema.yml`: typed UbiQ event envelope
- `schemas/universal-adapter.schema.yml`: Everything-as-Code adapter/security/capability configuration

The repository already includes generated Universal Adapter contracts for Zig, Rust, TypeScript and Go under `generated/`.

## Runtime source

- `src/ubiq/event.zig`
- `src/ubiq/capability.zig`
- `src/ubiq/delivery.zig`
- `src/ubiq/security.zig`
- `src/ubiq/mapping.zig`
- `src/ubiq/runtime.zig`

See `docs/ARCHITECTURE.md` for the complete model.

## Zig 0.16

This branch targets the stable Zig 0.16.0 release.

```bash
zig build
zig build test
zig build run
```

CI pins Zig `0.16.0` and checks formatting, build, tests and the semantic runtime demo.

## Current implementation boundary

The semantic runtime, bounded memory broker, settlement state machine, capability negotiation, reversible transport mapping and security policy model are implemented in Zig.

Wire-level NATS/JetStream/Kafka/Redpanda/RabbitMQ/QUIC/HTTP/WebSocket/SSE clients remain adapter implementations behind the Universal Adapter SPI; they must use real client libraries and real cryptographic providers rather than protocol simulations.

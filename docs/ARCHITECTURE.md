# UbiQ Architecture

UbiQ is a transport-independent semantic messaging runtime. A domain event is defined once and may enter through one transport, be persisted/routed through another, and leave through a third without changing its semantic identity.

## Core invariant

`canonical event identity != physical transport address`

Example canonical event:

`Financial.createInvoice.ok`

Possible physical bindings:

- Memory: channel `ubiq.memory`, envelope type `Financial.createInvoice.ok`
- NATS: subject `Financial.createInvoice.ok`
- JetStream: stream `UBIQ_EVENTS`, envelope type `Financial.createInvoice.ok`
- Kafka/Redpanda: topic `ubiq.events`, envelope type/header `Financial.createInvoice.ok`
- RabbitMQ: exchange/queue `ubiq.events`, routing metadata `Financial.createInvoice.ok`
- REST: `POST /v1/events`, envelope type `Financial.createInvoice.ok`
- WebSocket: `/v1/events/ws`, frame type `Financial.createInvoice.ok`
- SSE: `/v1/events/sse`, event type `Financial.createInvoice.ok`
- QUIC/TCP: UbiQ frame envelope type `Financial.createInvoice.ok`

The adapter MUST be able to restore the exact canonical identity after transport mapping.

## Planes

### Semantic Plane

Owns canonical names, schemas, nominal event identity and the `request | ok | error` state.

The event state chooses the internal Agent/Actor pipeline before domain payload processing:

- `request` -> execution pipeline
- `ok` -> normal consumption pipeline
- `error` -> self-healing pipeline

### Routing Plane

Resolves ingress, durable backbone and egress independently. This allows combinations such as:

`REST -> JetStream -> WebSocket`

or:

`Memory -> QUIC -> Kafka`

The route is rejected when declared requirements cannot be met. UbiQ never upgrades a weak transport by merely renaming its capabilities.

### Reliability Plane

Delivery state is intentionally independent from domain event state:

`published -> leased -> received -> settled_ok | settled_error`

If a lease expires before settlement:

`received -> expired -> leased by another worker`

`received` means only that a worker accepted delivery. It is not evidence that the requested effect happened.

### Security Plane

The XZT contract is algorithm-agile and separates policy from cryptographic implementation.

Current policy model includes:

- mTLS peer authentication;
- replay defense;
- generic UbiQ Proof-of-Possession for non-HTTP transports;
- RFC 9449 DPoP only at HTTP-compatible boundaries;
- hybrid X25519 + ML-KEM key-establishment policy;
- Ed25519/ML-DSA hybrid signature policy;
- XChaCha20-Poly1305 or AES-GCM-SIV message AEAD policy;
- LinearAutoDestroy one-shot execution capabilities.

The runtime explicitly refuses to model a SHA-256 label/hash as if it were ML-KEM, ML-DSA or a valid DPoP proof. Production cryptographic operations belong behind a reviewed `CryptoProvider` implementation.

### Transport Plane

Adapters implement the transport SPI. The semantic runtime knows only capability metadata and reversible mappings.

### Storage Coordination Plane

Database sharding, replication and placement should be orchestrated by UbiQ events and policies, not hard-coded into the messaging kernel. That keeps database-specific consistency rules outside transport adapters.

## Delivery guarantees

UbiQ exposes three application-level requirements:

### best_effort

No durable processing guarantee.

### received

The route must provide reliable delivery to a receiver.

### processed

The route must include a durable backbone and UbiQ Execution Settlement.

The universal guarantee is intentionally not called generic `exactly-once`.

The target model is:

`at-least-once delivery + durable idempotency + leases + explicit execution settlement`

This yields an effectively-once business effect when the application effect and idempotency record are committed consistently.

## UbiQ Execution Settlement Protocol

Control-plane states:

1. `PUBLISHED`
2. `LEASED(worker, until)`
3. `RECEIVED(worker)`
4. worker executes the effect
5. `SETTLED_OK` or `SETTLED_ERROR`

If step 5 does not happen before lease expiration, the event becomes eligible for another worker.

These are control-plane delivery states, not domain events such as `Financial.createInvoice.ok`.

## LinearAutoDestroy

LinearAutoDestroy destroys execution authority, not history.

A one-shot capability contains:

- event identity;
- holder identity;
- expiration;
- consumed bit.

After successful consumption, that capability cannot authorize a second execution. EventStore/audit evidence may remain permanently available.

## Bounded memory runtime

`MemoryBroker(N)` is allocation-free after construction and has a compile-time maximum record capacity. Caller-owned slices are used for envelope data. This makes memory growth explicit and prevents an unbounded in-memory queue from silently exhausting a Supervisor process.

## Current code

- `src/ubiq/event.zig`: canonical event and pipeline typing
- `src/ubiq/capability.zig`: transport capability registry and route negotiation
- `src/ubiq/delivery.zig`: bounded broker, leases, ACK/settlement separation and idempotency
- `src/ubiq/security.zig`: XZT policy, CryptoProvider capability contract and LinearAutoDestroy
- `src/ubiq/mapping.zig`: reversible transport mapping
- `src/ubiq/runtime.zig`: bridge planner
- `schemas/event-envelope.schema.yml`: wire-neutral event contract
- `schemas/universal-adapter.schema.yml`: adapter configuration contract

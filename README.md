# UbiQUIC UniversalServer

UbiQUIC is the H2A2H reference for a transport-neutral **UniversalServer** with eXtreme Zero Trust (XZT), canonical event preservation and LinearAutoDestroy semantics.

The original QUIC broker remains the reference low-latency backend, but the architecture no longer makes QUIC a domain requirement. NATS, JetStream, Kafka, Redpanda, RabbitMQ, BullMQ, Redis Streams, MQTT, gRPC, HTTP, MCP, A2A, AP2 and future technologies are selected through adapters declared in `config.yml`.

## Core invariant

Domain code sees one event identity regardless of adapter.

For example:

`Financial.SellMachine.SaleIdentified`

must remain exactly that canonical H2A2H event to the runtime. An adapter may internally map it to a NATS subject, Kafka topic, RabbitMQ routing key, BullMQ queue or another technology-specific name, but the mapping must be reversible:

`canonical -> transport -> canonical`

If round-trip mapping does not reproduce the exact canonical event, the adapter is non-conformant.

## Everything as Code

The canonical interface is defined in:

`schemas/universal-adapter.schema.yml`

It is JSON Schema 2020-12 serialized as YAML. The schema defines technologies, endpoint/port configuration, capabilities, delivery semantics, security profiles and reversible event mapping.

`codegen/generate.mjs` compiles schema-controlled enums and metadata for Zig, Rust, TypeScript and Go. The shared SPI contracts live under `generated/`.

Transport selection belongs in `config.yml`, never in an Entity/Agent's domain behavior.

## Universal Adapter operations

Every conformant adapter exposes the same semantic operations:

- `open(config)`
- `close()`
- `publish(envelope)`
- `subscribe(canonical_event, handler)`
- `ack(delivery)`
- `nack(delivery, reason)`
- `health()`
- `capabilities()`
- `mapCanonicalToTransport(canonical_event)`
- `mapTransportToCanonical(transport_name)`

Unsupported broker guarantees must be reported as capability mismatches. They must not be silently weakened.

## XZT security profile

`h2a2h.security.xzt.v1` composes independently:

- mTLS peer authentication;
- DPoP/proof-of-possession;
- replay/freshness defense;
- passwordless Human authority where applicable (WebAuthn/passkeys);
- algorithm-agile hybrid post-quantum key-establishment policy (reference: X25519 + ML-KEM);
- message-level integrity policy;
- bounded OpenDelegation authority;
- LinearAutoDestroy for ephemeral secrets and one-shot capability material.

The implementation must declare the actual algorithms. QUIC/TLS alone is not considered "quantum secure".

## Current QUIC broker capabilities

- subscriber-aware redelivery;
- ACK TTL;
- DLQ and Outbox flows;
- event sourcing;
- subscriber registry;
- per-emission proof modeling;
- mTLS/post-quantum envelope experimentation.

The current cryptographic prototype records local transcript evidence; production ML-KEM integration must use a reviewed cryptographic implementation rather than treating a SHA-256 transcript as a KEM.

## Configuration

The repository includes a multi-adapter `config.yml`. Changing the selected backend is intended to be configuration-only when both adapters satisfy the same required semantic capabilities.

Example:

```yaml
universal_server:
  default_adapter: nats

adapters:
  nats:
    technology: nats
    enabled: true
    endpoint:
      host: 127.0.0.1
      port: 4222
    security_profile: xzt
```

## Code generation

```bash
cd codegen
npm install
npm run generate
```

Generated bindings target:

- Zig 0.16
- Rust
- TypeScript
- Go

## Running the existing Zig broker

```bash
zig build run
```

## Tests

```bash
zig build test
```

The next implementation layer is the concrete backend registry: each technology implements the generated Universal Adapter SPI while the UniversalServer validates `config.yml`, checks capability/security compatibility and injects the chosen adapter at runtime.

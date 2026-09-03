# UbiQ Transport Bindings

UbiQ treats transport as framing, not domain semantics. The canonical event name and envelope survive every binding unchanged.

## Canonical identity

Minimum event shape:

```text
{Entity}.{action_or_intent}.{state}
```

Examples:

```text
Financial.createInvoice.request
Financial.createInvoice.ok
Financial.createInvoice.error
```

The final segment selects the Agent/Actor pipeline before payload inspection:

- `request` -> execution
- `ok` -> consume
- `error` -> self-healing

## Binary UbiQ Wire Envelope

TCP, QUIC and WebSocket binary messages share `src/ubiq/wire.zig`.

The v1 frame contains:

1. `UBIQ` magic
2. wire version
3. semantic event state
4. requested delivery guarantee
5. creation timestamp
6. bounded lengths for identity/metadata fields
7. payload length
8. reserved extension bits
9. canonical envelope fields and payload

Decoding is zero-copy. The returned envelope points into the caller-owned frame buffer.

A frame is rejected when the serialized state disagrees with the final state segment of the canonical name. This prevents a transport from presenting `Financial.createInvoice.ok` while marking the envelope as `error`.

## TCP

TCP uses:

```text
[u32 big-endian frame length][UbiQ wire envelope]
```

The transport does not rename the event. `src/ubiq/tcp_binding.zig` exposes functions over `std.Io.Reader`, `std.Io.Writer` and `std.net.Stream`.

TCP receipt is not execution settlement.

## REST / HTTP

Canonical ingress route:

```text
POST /v1/events/{Entity}.{action_or_intent}.{state}
```

Example:

```text
POST /v1/events/Financial.createInvoice.request
```

Required metadata headers:

```text
X-UbiQ-Id
X-UbiQ-Correlation-Id
X-UbiQ-Causation-Id
X-UbiQ-Idempotency-Key
X-UbiQ-Schema-Id
X-UbiQ-Guarantee: best_effort | received | processed
```

The request body is the event payload. The reference binding is bounded to 1 MiB by default and performs no heap allocation while reading the body.

A successful HTTP response can only confirm transport receipt. For `processed`, the event remains unsettled until the worker emits the UbiQ Execution Settlement control frame.

## WebSocket

WebSocket upgrades use Zig's HTTP/WebSocket implementation. Event messages are binary WebSocket frames containing the exact UbiQ wire envelope.

Control messages such as `RECEIVED` may be sent as small text frames, but they remain control-plane messages rather than domain events.

## SSE

SSE is egress-only in the initial binding:

```text
id: evt-1
event: Financial.createInvoice.ok
data: {"invoice_id":"INV-1"}
```

The `event:` value is always the canonical UbiQ event identity.

## Cross-protocol invariant

A conformant bridge must satisfy:

```text
canonical event
    -> ingress binding
    -> UbiQ envelope
    -> durable/volatile backbone
    -> egress binding
    -> same canonical event
```

For example:

```text
REST /v1/events/Financial.createInvoice.request
    -> Financial.createInvoice.request
    -> JetStream
    -> WebSocket binary UbiQ frame
    -> Financial.createInvoice.request
```

The physical NATS subject, Kafka topic, HTTP path or queue name is never allowed to replace the canonical domain identity.

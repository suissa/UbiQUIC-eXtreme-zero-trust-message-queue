# UbiQ NATS Core and JetStream Binding

This binding is implemented directly against the NATS client protocol in Zig 0.16. It deliberately keeps NATS/JetStream transport state separate from UbiQ execution settlement.

## Canonical identity

For NATS Core, the canonical event can be used directly as the subject when it is a valid NATS subject:

```text
Financial.createInvoice.ok
```

The payload remains the UbiQ binary wire envelope, so canonical identity is still carried inside the message and verified after decoding.

For JetStream pull delivery, the physical NATS subject is an inbox used by the pull request. The canonical identity is recovered from the UbiQ wire envelope. Therefore physical delivery subjects never become domain identity.

## NATS protocol implemented

`src/ubiq/nats_protocol.zig` implements bounded encoding/parsing for:

```text
INFO
CONNECT
+OK
-ERR
PING
PONG
PUB
HPUB
SUB
UNSUB
MSG
HMSG
```

`src/ubiq/nats_client.zig` builds an allocation-free client over caller-owned `std.Io.Reader` and `std.Io.Writer` instances.

## JetStream publication

A durable UbiQ publication uses `HPUB` and includes:

```text
Nats-Msg-Id: <UbiQ idempotency_key>
Nats-Expected-Stream: <configured stream>
```

`Nats-Msg-Id` allows JetStream to identify duplicate publications within its configured duplicate window. `Nats-Expected-Stream` prevents accidental publication into an unexpected stream.

A successful JetStream PubAck means:

```text
publication persisted by JetStream
```

It does **not** mean:

```text
worker executed the Action
```

and it never creates `UbiQ SETTLED_OK`.

## Durable pull consumer

UbiQ creates an explicit-ack durable pull consumer through:

```text
$JS.API.CONSUMER.DURABLE.CREATE.<stream>.<consumer>
```

Messages are requested through:

```text
$JS.API.CONSUMER.MSG.NEXT.<stream>.<consumer>
```

JetStream supplies a `$JS.ACK...` reply subject with each delivery.

The recommended UbiQ sequence is:

```text
JetStream delivers UbiQ envelope
        |
        v
UbiQ RECEIVED
        |
        +---- long Action? ----> JetStream +WPI
        |
        v
execute idempotent Action
        |
        v
persist UbiQ SETTLED_OK / SETTLED_ERROR
        |
        v
JetStream +ACK
```

The order is intentional. If a worker crashes after the business effect and UbiQ settlement but before JetStream receives `+ACK`, JetStream may redeliver. UbiQ uses durable idempotency/settlement state to avoid repeating the business effect, then acknowledges the broker delivery.

This produces an effectively-once business effect model from:

```text
at-least-once delivery
+ durable idempotency
+ execution settlement
+ fencing/leases where required
```

It does not claim generic transport-independent exactly-once execution.

## Work-in-progress

For Actions that exceed JetStream's `ack_wait`, UbiQ can publish:

```text
+WPI
```

to the JetStream ack subject. This only extends the broker processing window. It does not modify UbiQ's execution state.

## Live CI conformance

The repository CI starts an actual NATS Server with JetStream enabled and validates:

1. NATS Core connection/handshake;
2. canonical event round-trip over NATS Core;
3. JetStream stream creation;
4. durable publication;
5. duplicate publication detection using the same UbiQ idempotency key;
6. explicit durable pull consumer creation;
7. pull delivery of the canonical UbiQ envelope;
8. `+WPI` while UbiQ remains only `RECEIVED`;
9. explicit UbiQ `SETTLED_OK` transition;
10. JetStream `+ACK` only after settlement.

The live test currently runs against NATS Server 2.14.5 in Docker.

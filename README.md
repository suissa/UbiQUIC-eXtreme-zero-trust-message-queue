# QUICMQ

QUICMQ is a Zig prototype of a QUIC-oriented event broker with event sourcing, subscriber-aware redelivery, DLQ and Outbox flows.

## Implemented broker capabilities

- **mTLS + Kyber envelope simulation**: enabled by `security.mtls_kyber_enabled` in `config.yml`. For each delivery attempt the broker records a client-certificate/Kyber transcript envelope before sending the message. The current implementation models the cryptographic envelope locally with a SHA-256 transcript because Zig stdlib does not ship a Kyber KEM primitive.
- **Subscriber address registry**: controlled by `subscribers.register_addresses`. When enabled, every configured subscriber endpoint is registered and audited on startup.
- **DPoP per emitted event**: controlled by `security.dpop_enabled`. Every delivery attempt gets a fresh proof derived from event id, subscriber id and emission timestamp.
- **ACK TTL and cyclic redelivery**: `broker.ack_ttl_ms` defines the ACK wait window. Delivery attempts are ordered by subscribers that have waited the longest since their last successful delivery; each subscriber is tried until one ACKs.
- **DLQ and Outbox**: if no subscriber ACKs, the event goes to the in-memory DLQ and is persisted in the audit stream. After `broker.dlq_to_outbox_after_ms` (one day by default) the DLQ item is promoted to the Outbox.
- **Event sourcing**: all successful actions are appended to `data/events-success.qmes`, while failed actions are appended to `data/events-error.qmes`. Records use a compact binary format with magic/version, action/status ids, timestamp, event/subscriber hashes and payload length.

## Configuration

The repository includes a runnable `config.yml`:

```yaml
broker:
  ack_ttl_ms: 500
  dlq_to_outbox_after_ms: 86400000
  data_dir: data
security:
  mtls_kyber_enabled: true
  dpop_enabled: true
subscribers:
  register_addresses: true
  endpoints:
    - id: billing
      address: quic://127.0.0.1:9001
```

## Running

```bash
zig build run
```

## Tests

```bash
zig build test
```

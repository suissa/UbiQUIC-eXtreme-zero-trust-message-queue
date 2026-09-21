# RFC runtime implementation

This branch turns the semantic requirements from UbiQ-RFCs and the transport boundary exercised by mcp-mt-nats-quic into executable Zig contracts.

## Implemented

| Area | Module | RFCs |
| --- | --- | --- |
| Canonical adapter descriptor, ABI and failure normalization | src/ubiq/adapter.zig | 005, 026, 030 |
| Deterministic capability/security/deadline/cost/latency selection | src/ubiq/router.zig | 006, 008, 015, 032–036 |
| Bounded replay window and idempotency outcomes | src/ubiq/security_runtime.zig | 009, 010, 018–021, 045 |
| Request/response correlation and schema checks | src/ubiq/contracts.zig | 012, 023, 024, 038 |
| Resumable streams and semantic lifecycle | src/ubiq/contracts.zig | 014, 017, 039 |
| Broadcast/competing/durable/flow-bound subscriptions | src/ubiq/contracts.zig | 013, 040 |
| Bounded semantic queue, leases, retry count and quarantine | src/ubiq/contracts.zig | 014, 027, 041, 043 |
| Binary envelope, NATS Core and JetStream bindings | existing wire, nats_protocol, nats_client, jetstream modules | 001–005, 007, 009–012, 028, 044, 045 |

All new modules are exported from src/ubiq/root.zig and covered by the repository's Zig 0.16 CI.

## Relationship to mcp-mt-nats-quic

The Go MCP server remains an application-facing NATS tool server. Its mTLS, DPoP policy and MOQT/QUIC HTTP boundary are represented here as adapter descriptor, security requirement and normalized failure contracts. The Zig runtime does not fake a QUIC or post-quantum implementation: a production deployment must provide a reviewed QUIC/MOQT adapter and cryptographic provider behind the ABI.

This preserves RFC-005, RFC-021 and RFC-030: transport code moves canonical envelopes, while policy and domain execution remain in the UbiQ runtime.

## Verification

The pull request CI runs:

- zig fmt --check build.zig src
- zig build
- zig build test
- semantic runtime demo
- live NATS Core + JetStream integration when Docker is available

# Runtime feature configuration

The runtime feature registry is the source of truth for feature selection. Profiles are presets; they do not imply that every optional production driver or cryptographic provider is present.

## Runtime APIs

- `features.Config`: typed profile and enabled-feature view with fail-closed dependency validation.
- `runtime_config.loadJson(allocator, source, overrides)`: JSON configuration parser, profile defaults, host-supplied overrides, and startup validation. The caller owns the returned `Loaded` value and must call `deinit()`.
- `dependency_graph.validate(config, edges)`: required/optional/conflicting dependency edges and cycle detection.
- `supervisor.Tree`: policy/lifecycle state for root, security, transport, sharder, and healing supervisors, including health, restart, and failure counters.
- `transports.Registry`: feature-gated lifecycle slots for QUIC, HTTP, gRPC, WebSocket, and NATS, including publish/failure/reconnect counters.
- `cluster_control.Membership(N)`: authenticated-join gate, membership epochs, heartbeat expiry, and fencing-generation validation.
- `shard_migration.Migration`: adapter capability contract and a fenced, checkpointed migration state machine requiring verification before cutover.\n- `settlement_journal.Store`: bounded append-only JSONL snapshots with FNV-1a checksums, file sync after each append, replay in order, and safe ignoring of an incomplete trailing line.

## Configuration precedence and overrides

The JSON loader accepts `profile` and optional `enabled` feature-name array. If `enabled` is omitted, the selected profile defaults are used. Host code supplies overrides as values; no ambient process environment is read by the pure loader.

Supported override names:
- `UBIQ_PROFILE`: one of `minimal`, `secure`, `full`, `custom`.
- `UBIQ_FEATURES`: comma-separated feature names, replacing the selected feature list.

Unknown override names, unknown feature names, malformed JSON, and invalid feature combinations fail closed.

## Format support and operational boundaries

JSON is implemented. YAML and TOML decoders remain outstanding; they must decode into the same typed contract rather than be guessed or silently accepted as JSON.

Supervisor and transport modules are lifecycle registries, not process managers or network drivers. They do not start OS processes or open listeners. Production health must be driven by the real adapter's successful health signal.

Cluster membership accepts a boolean authenticated-join decision from its caller; it does not implement cryptographic authentication itself. The fencing generation is an in-process control-plane model and must be persisted/consensus-backed before use as a distributed production authority.

The shard migration state machine is storage-neutral. It does not claim that PostgreSQL, MongoDB, SQLite, or document stores share the same consistency guarantees, and it does not implement their wire/database drivers.\n\nThe settlement journal is the first durable persistence layer, not a complete broker integration. The caller must append each state transition before acknowledging it, replay snapshots into an idempotency/lease projection, and enforce fencing at the effect boundary. JSONL is bounded by a configured byte limit; retention/compaction, outbox coordination, atomic payload-store transactions, and exhaustive crash-injection tests remain follow-up work. FNV-1a detects accidental corruption but is not a cryptographic integrity mechanism.

## Validation

The repository CI pins Zig 0.16.0. Run:

```sh
zig fmt --check build.zig src
zig build
zig build test
zig build run
zig build nats-integration
```

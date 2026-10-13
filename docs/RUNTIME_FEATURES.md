# Runtime feature configuration

This document defines the first implementation slice for issues #12–#17. Feature selection is a control-plane concern and must not change canonical event identity or silently weaken route guarantees.

## Profiles

- `minimal`: UbiQ core and bounded in-memory execution only.
- `secure`: minimal plus crypto policy, mTLS, replay guard, and supervisor. A production crypto provider is still required; a profile does not manufacture cryptographic capability.
- `full`: enables all features that are compiled in and whose dependencies validate. It does not imply that every optional wire driver is implemented or installed.
- `custom`: explicit feature map; unspecified feature values must be rejected or resolved by documented defaults, never guessed.

## Dependency policy

The runtime must validate the complete configuration before starting listeners or accepting events. At minimum:

- Any remote transport requires `ubiq`, `supervisor`, and `crypto`.
- HTTP DPoP requires the HTTP transport and a configured proof verifier.
- mTLS requires a configured identity/trust provider.
- Replay Guard requires the security/crypto verification path.
- Predictive self-healing requires self-healing.
- Projection requires event sourcing.
- Sharding requires UbiQ and supervisor control.
- Disabling a transport must be a normal startup state, not a startup error.

Conflicts and cycles must be detected from the declared dependency graph before bootstrap. Diagnostics should report the feature and violated edge; do not log secrets or credential material.

## Configuration sources and precedence

Recommended precedence, lowest to highest:

1. built-in profile defaults;
2. configuration file;
3. environment overrides;
4. explicit command-line overrides (if enabled by the host application).

The resolved configuration is immutable after bootstrap. YAML, TOML and JSON are input formats; they must resolve to the same typed representation. Parsing and schema validation must happen before runtime components are constructed.

## Environment variables

Use the prefix `UBIQ_FEATURE__`, uppercase feature names, and double underscores for nested paths. For example:

- `UBIQ_FEATURE__PROFILE=secure`
- `UBIQ_FEATURE__FEATURES__TRANSPORT_NATS=true`
- `UBIQ_FEATURE__FEATURES__DPOP=false`

Boolean values are limited to `true` and `false` (case-insensitive). Invalid values fail closed with a path-specific diagnostic.

## Status and scope

The JSON Schema in `schemas/runtime-features.schema.json` is the canonical initial input schema. It is not, by itself, a YAML/TOML parser or a production runtime loader. The Zig feature registry must remain the typed authority, and generated language SDKs should consume the same schema rather than reimplementing feature semantics.

const std = @import("std");
const capability = @import("capability.zig");
const event = @import("event.zig");

pub const AdapterError = error{
    InvalidDescriptor,
    IncompatibleAbi,
    NotHealthy,
    Backpressure,
    TransportFailure,
    SecurityFailure,
    SchemaFailure,
};

pub const abi_version: u16 = 1;

pub const Descriptor = struct {
    name: []const u8,
    technology: []const u8,
    transport: capability.Transport,
    capabilities: capability.Capabilities,
    max_payload_bytes: usize = 0,
    abi: u16 = abi_version,
    enabled: bool = true,
};

pub const Health = enum { unknown, healthy, degraded, unhealthy };

pub const FailureKind = enum { transport, security, schema, internal, backpressure, timeout };

pub const Failure = struct {
    kind: FailureKind,
    code: []const u8,
    detail: []const u8 = "",
    retryable: bool = false,
    preserve_execution: bool = true,
};

pub const Dispatch = struct {
    envelope: event.Envelope,
    adapter: Descriptor,
};

/// RFC-005/026/030 contract validation. Domain code only receives canonical
/// envelopes; adapter metadata is never promoted into business semantics.
pub fn validateDescriptor(descriptor: Descriptor) AdapterError!void {
    if (descriptor.name.len == 0 or descriptor.technology.len == 0) return error.InvalidDescriptor;
    if (descriptor.abi != abi_version) return error.IncompatibleAbi;
    if (!descriptor.enabled) return error.NotHealthy;
    if (descriptor.max_payload_bytes != 0 and descriptor.max_payload_bytes < 1) return error.InvalidDescriptor;
}

pub fn canSend(descriptor: Descriptor, envelope: event.Envelope) AdapterError!void {
    try validateDescriptor(descriptor);
    if (descriptor.max_payload_bytes != 0 and envelope.payload.len > descriptor.max_payload_bytes) {
        return error.SchemaFailure;
    }
}

pub fn normalizeTransportFailure(code: []const u8, detail: []const u8, retryable: bool) Failure {
    return .{ .kind = .transport, .code = code, .detail = detail, .retryable = retryable };
}

pub fn normalizeSecurityFailure(code: []const u8, detail: []const u8) Failure {
    return .{ .kind = .security, .code = code, .detail = detail, .retryable = false };
}

pub fn normalizeBackpressure(code: []const u8, detail: []const u8) Failure {
    return .{ .kind = .backpressure, .code = code, .detail = detail, .retryable = true };
}

test "adapter ABI validates MCP-style transport capabilities" {
    const descriptor = Descriptor{
        .name = "mcp-nats",
        .technology = "nats",
        .transport = .nats,
        .capabilities = capability.profile(.nats).capabilities,
        .max_payload_bytes = 1024,
    };
    try validateDescriptor(descriptor);
    const envelope = event.Envelope{
        .id = "evt",
        .event = try event.CanonicalEvent.parse("Agent.invoke.request"),
        .correlation_id = "corr",
        .causation_id = "cause",
        .idempotency_key = "idem",
        .schema_id = "Agent.invoke.request@1",
        .payload = "{}",
        .guarantee = .received,
        .created_at_ms = 0,
    };
    try canSend(descriptor, envelope);
    const failure = normalizeTransportFailure("CONNECTION_CLOSED", "NATS connection closed", true);
    try std.testing.expectEqual(FailureKind.transport, failure.kind);
}

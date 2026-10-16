const std = @import("std");
const event = @import("event.zig");
const mapping = @import("mapping.zig");
const wire = @import("wire.zig");
const http = @import("http_binding.zig");
const tcp = @import("tcp_binding.zig");

/// Shared, transport-neutral conformance fixtures. These checks exercise the
/// same envelope contract used by concrete bindings; live network drivers
/// remain covered by their dedicated integration jobs.
fn fixture() !event.Envelope {
    return .{
        .id = "conformance-event-01",
        .event = try event.CanonicalEvent.parse("Financial.createInvoice.request"),
        .correlation_id = "conformance-correlation",
        .causation_id = "conformance-causation",
        .idempotency_key = "conformance-idempotency",
        .schema_id = "Financial.createInvoice.request@1",
        .payload = "{}",
        .guarantee = .processed,
        .created_at_ms = 1_791_446_400_000,
    };
}

fn expectEnvelopeEqual(expected: event.Envelope, actual: event.Envelope) !void {
    try std.testing.expectEqualStrings(expected.id, actual.id);
    try std.testing.expectEqualStrings(expected.event.name, actual.event.name);
    try std.testing.expectEqual(expected.event.state, actual.event.state);
    try std.testing.expectEqualStrings(expected.correlation_id, actual.correlation_id);
    try std.testing.expectEqualStrings(expected.causation_id, actual.causation_id);
    try std.testing.expectEqualStrings(expected.idempotency_key, actual.idempotency_key);
    try std.testing.expectEqualStrings(expected.schema_id, actual.schema_id);
    try std.testing.expectEqualStrings(expected.payload, actual.payload);
    try std.testing.expectEqual(expected.guarantee, actual.guarantee);
    try std.testing.expectEqual(expected.created_at_ms, actual.created_at_ms);
}

test "cross-protocol conformance preserves every canonical envelope field" {
    const original = try fixture();

    // TCP and WebSocket share the same versioned binary envelope.
    var wire_buffer: [4096]u8 = undefined;
    const encoded = try wire.encode(original, &wire_buffer);
    const decoded = try wire.decode(encoded);
    try expectEnvelopeEqual(original, decoded);

    // REST uses a path binding but must reconstruct the identical event name.
    var route_buffer: [256]u8 = undefined;
    const route = try http.routeFor(original.event.name, &route_buffer);
    const http_event = try http.eventFromTarget(route);
    try std.testing.expectEqualStrings(original.event.name, http_event.name);
    try std.testing.expectEqual(original.event.state, http_event.state);

    // Physical transport addresses must never become semantic event identity.
    const transports = [_]@import("capability.zig").Transport{
        .memory,
        .tcp,
        .quic,
        .nats,
        .nats_jetstream,
        .kafka,
        .redpanda,
        .rabbitmq,
        .rest,
        .websocket,
        .sse,
    };
    for (transports) |transport| {
        const address = mapping.mapCanonical(transport, original.event.name);
        try std.testing.expectEqualStrings(original.event.name, mapping.mapTransportToCanonical(address));
    }
}

test "cross-protocol conformance rejects malformed REST identity" {
    try std.testing.expectError(
        error.InvalidRoute,
        http.eventFromTarget("/v1/events/Financial.createInvoice.request?alias=1"),
    );
    try std.testing.expectError(
        error.InvalidCanonicalName,
        http.eventFromTarget("/v1/events/Financial/createInvoice/request"),
    );
}

test "cross-protocol conformance rejects corrupted binary envelope" {
    var wire_buffer: [4096]u8 = undefined;
    const encoded = try wire.encode(try fixture(), &wire_buffer);
    try std.testing.expectError(error.InvalidFrame, wire.decode("not-a-wire-frame"));
    try std.testing.expectError(error.InvalidFrame, wire.decode(encoded[0 .. encoded.len - 1]));
}

test "TCP framing interoperates with the canonical envelope contract" {
    const original = try fixture();
    var output: [4096]u8 = undefined;
    var writer = std.Io.Writer.fixed(&output);
    var frame_buffer: [2048]u8 = undefined;
    try tcp.sendFramed(&writer, original, &frame_buffer);

    var input: [4096]u8 = undefined;
    @memcpy(input[0..writer.end], output[0..writer.end]);
    var reader = std.Io.Reader.fixed(input[0..writer.end]);
    var receive_buffer: [2048]u8 = undefined;
    const restored = try tcp.receiveFramed(&reader, &receive_buffer);
    try expectEnvelopeEqual(original, restored);
}

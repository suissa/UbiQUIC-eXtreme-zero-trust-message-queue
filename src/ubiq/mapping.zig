const std = @import("std");
const capability = @import("capability.zig");

pub const PhysicalAddress = struct {
    transport: capability.Transport,
    /// Physical channel/container. The canonical event is intentionally kept
    /// separately so transports never redefine semantic identity.
    channel: []const u8,
    canonical_event: []const u8,
};

pub fn mapCanonical(transport: capability.Transport, canonical_event: []const u8) PhysicalAddress {
    return .{
        .transport = transport,
        .channel = switch (transport) {
            .memory => "ubiq.memory",
            .tcp => "ubiq.tcp",
            .quic => "ubiq.quic",
            .nats => canonical_event,
            .nats_jetstream => "UBIQ_EVENTS",
            .kafka, .redpanda => "ubiq.events",
            .rabbitmq => "ubiq.events",
            .rest => "/v1/events",
            .websocket => "/v1/events/ws",
            .sse => "/v1/events/sse",
        },
        .canonical_event = canonical_event,
    };
}

pub fn mapTransportToCanonical(address: PhysicalAddress) []const u8 {
    return address.canonical_event;
}

pub fn isRoundTripReversible(transport: capability.Transport, canonical_event: []const u8) bool {
    const address = mapCanonical(transport, canonical_event);
    return std.mem.eql(u8, canonical_event, mapTransportToCanonical(address));
}

test "canonical identity survives every supported transport mapping" {
    const name = "Financial.createInvoice.ok";
    const transports = [_]capability.Transport{
        .memory, .tcp, .quic, .nats, .nats_jetstream, .kafka, .redpanda, .rabbitmq, .rest, .websocket, .sse,
    };
    for (transports) |transport| {
        try std.testing.expect(isRoundTripReversible(transport, name));
    }
}

test "Kafka uses a physical container without turning the topic into domain identity" {
    const address = mapCanonical(.kafka, "Financial.createInvoice.ok");
    try std.testing.expectEqualStrings("ubiq.events", address.channel);
    try std.testing.expectEqualStrings("Financial.createInvoice.ok", address.canonical_event);
}

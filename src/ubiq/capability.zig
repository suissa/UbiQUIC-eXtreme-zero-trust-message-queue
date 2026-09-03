const std = @import("std");
const event = @import("event.zig");

pub const Transport = enum {
    memory,
    tcp,
    quic,
    nats,
    nats_jetstream,
    kafka,
    redpanda,
    rabbitmq,
    rest,
    websocket,
    sse,
};

pub const Capabilities = struct {
    reliable: bool = false,
    ordered: bool = false,
    durable: bool = false,
    replay: bool = false,
    bidirectional: bool = false,
    native_ack: bool = false,
    consumer_groups: bool = false,
    streaming: bool = false,

    pub fn satisfies(self: Capabilities, required: Requirements) bool {
        return (!required.reliable or self.reliable) and
            (!required.ordered or self.ordered) and
            (!required.durable or self.durable) and
            (!required.replay or self.replay) and
            (!required.bidirectional or self.bidirectional) and
            (!required.native_ack or self.native_ack) and
            (!required.consumer_groups or self.consumer_groups) and
            (!required.streaming or self.streaming);
    }
};

pub const Requirements = struct {
    reliable: bool = false,
    ordered: bool = false,
    durable: bool = false,
    replay: bool = false,
    bidirectional: bool = false,
    native_ack: bool = false,
    consumer_groups: bool = false,
    streaming: bool = false,
};

pub const AdapterProfile = struct {
    transport: Transport,
    capabilities: Capabilities,
};

pub fn profile(transport: Transport) AdapterProfile {
    return .{
        .transport = transport,
        .capabilities = switch (transport) {
            .memory => .{ .reliable = true, .ordered = true, .bidirectional = true, .native_ack = true },
            .tcp => .{ .reliable = true, .ordered = true, .bidirectional = true, .streaming = true },
            .quic => .{ .reliable = true, .ordered = true, .bidirectional = true, .streaming = true },
            .nats => .{ .reliable = true, .bidirectional = true, .native_ack = false, .consumer_groups = true },
            .nats_jetstream => .{ .reliable = true, .durable = true, .replay = true, .bidirectional = true, .native_ack = true, .consumer_groups = true, .streaming = true },
            .kafka, .redpanda => .{ .reliable = true, .ordered = true, .durable = true, .replay = true, .native_ack = true, .consumer_groups = true, .streaming = true },
            .rabbitmq => .{ .reliable = true, .durable = true, .bidirectional = true, .native_ack = true, .consumer_groups = true },
            .rest => .{ .reliable = true, .bidirectional = true },
            .websocket => .{ .reliable = true, .ordered = true, .bidirectional = true, .streaming = true },
            .sse => .{ .reliable = true, .ordered = true, .streaming = true },
        },
    };
}

pub const RoutePlan = struct {
    ingress: Transport,
    backbone: Transport,
    egress: Transport,

    /// Durability/replay are guaranteed by the backbone. This allows REST,
    /// WebSocket, SSE or QUIC to be used at the edge without pretending those
    /// transports natively provide a durable log.
    pub fn satisfies(self: RoutePlan, required: Requirements) bool {
        const backbone = profile(self.backbone).capabilities;
        if ((required.durable and !backbone.durable) or (required.replay and !backbone.replay)) return false;

        const ingress = profile(self.ingress).capabilities;
        const egress = profile(self.egress).capabilities;
        if (required.bidirectional and (!ingress.bidirectional or !egress.bidirectional)) return false;
        if (required.reliable and (!ingress.reliable or !backbone.reliable or !egress.reliable)) return false;
        return true;
    }
};

pub fn requirementsFor(guarantee: event.DeliveryGuarantee) Requirements {
    return switch (guarantee) {
        .best_effort => .{},
        .received => .{ .reliable = true },
        .processed => .{ .reliable = true, .durable = true },
    };
}

test "durable backbone upgrades protocol edges without lying about edge capabilities" {
    const required = Requirements{ .reliable = true, .durable = true };
    const good = RoutePlan{ .ingress = .rest, .backbone = .nats_jetstream, .egress = .websocket };
    try std.testing.expect(good.satisfies(required));

    const bad = RoutePlan{ .ingress = .rest, .backbone = .memory, .egress = .websocket };
    try std.testing.expect(!bad.satisfies(required));
}

test "raw REST is not advertised as a replayable durable log" {
    const rest = profile(.rest).capabilities;
    try std.testing.expect(!rest.durable);
    try std.testing.expect(!rest.replay);
}

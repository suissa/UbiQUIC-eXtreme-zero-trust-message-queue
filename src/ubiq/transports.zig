const std = @import("std");
const features = @import("features.zig");

pub const Kind = enum { quic, http, grpc, websocket, nats };
pub const State = enum { disabled, stopped, starting, healthy, degraded, unhealthy };

pub const Entry = struct {
    kind: Kind,
    state: State = .stopped,
    published: u64 = 0,
    failed: u64 = 0,
    reconnects: u64 = 0,
};

pub const TransportError = error{ FeatureDisabled, InvalidTransition };

pub const Registry = struct {
    entries: [5]Entry = .{
        .{ .kind = .quic },
        .{ .kind = .http },
        .{ .kind = .grpc },
        .{ .kind = .websocket },
        .{ .kind = .nats },
    },

    pub fn initialize(self: *Registry, config: features.Config) void {
        for (&self.entries) |*entry| {
            entry.state = if (config.isEnabled(featureFor(entry.kind))) .stopped else .disabled;
        }
    }

    pub fn start(self: *Registry, kind: Kind) TransportError!void {
        const entry = &self.entries[@intFromEnum(kind)];
        if (entry.state == .disabled) return error.FeatureDisabled;
        if (entry.state != .stopped and entry.state != .unhealthy and entry.state != .degraded)
            return error.InvalidTransition;
        entry.state = .starting;
    }

    pub fn markHealthy(self: *Registry, kind: Kind) TransportError!void {
        const entry = &self.entries[@intFromEnum(kind)];
        if (entry.state != .starting and entry.state != .degraded)
            return error.InvalidTransition;
        entry.state = .healthy;
    }

    pub fn recordPublish(self: *Registry, kind: Kind, ok: bool) void {
        const entry = &self.entries[@intFromEnum(kind)];
        if (ok) {
            entry.published += 1;
        } else {
            entry.failed += 1;
            entry.state = .degraded;
        }
    }

    pub fn recordReconnect(self: *Registry, kind: Kind) void {
        const entry = &self.entries[@intFromEnum(kind)];
        entry.reconnects += 1;
        if (entry.state != .disabled) entry.state = .starting;
    }

    pub fn metrics(self: *const Registry) Metrics {
        var result = Metrics{};
        for (self.entries) |entry| {
            if (entry.state == .healthy) result.healthy += 1;
            result.published += entry.published;
            result.failed += entry.failed;
            result.reconnects += entry.reconnects;
        }
        return result;
    }
};

pub const Metrics = struct { healthy: u8 = 0, published: u64 = 0, failed: u64 = 0, reconnects: u64 = 0 };

pub fn featureFor(kind: Kind) features.Feature {
    return switch (kind) {
        .quic => .transport_quic,
        .http => .transport_http,
        .grpc => .transport_grpc,
        .websocket => .transport_websocket,
        .nats => .transport_nats,
    };
}

test "disabled transports do not start and enabled transports expose metrics" {
    var registry = Registry{};
    registry.initialize(.{ .profile = .minimal, .enabled = features.defaults(.minimal) });
    try std.testing.expectError(error.FeatureDisabled, registry.start(.nats));

    registry.initialize(.{ .profile = .full, .enabled = features.defaults(.full) });
    try registry.start(.nats);
    try registry.markHealthy(.nats);
    registry.recordPublish(.nats, true);
    registry.recordPublish(.nats, false);
    registry.recordReconnect(.nats);
    const metrics = registry.metrics();
    try std.testing.expectEqual(@as(u8, 0), metrics.healthy);
    try std.testing.expectEqual(@as(u64, 1), metrics.published);
    try std.testing.expectEqual(@as(u64, 1), metrics.failed);
    try std.testing.expectEqual(@as(u64, 1), metrics.reconnects);
}

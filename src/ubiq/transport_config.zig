const std = @import("std");
const features = @import("features.zig");

pub const Kind = enum {
    quic,
    http,
    grpc,
    websocket,
    nats,
};

pub const Setting = struct {
    kind: Kind,
    enabled: bool = true,
};

pub const ConfigError = error{ DuplicateTransport, FeatureNotEnabled };

/// Runtime transport selection. This is an allow-list for bootstrap, not a
/// claim that every selected wire adapter is compiled or production-ready.
pub const Config = struct {
    settings: []const Setting,

    pub fn isEnabled(self: Config, kind: Kind) bool {
        for (self.settings) |setting| {
            if (setting.kind == kind) return setting.enabled;
        }
        return false;
    }

    pub fn validate(self: Config, runtime: features.Config) ConfigError!void {
        for (self.settings, 0..) |setting, index| {
            for (self.settings[0..index]) |previous| {
                if (previous.kind == setting.kind) return error.DuplicateTransport;
            }
            if (!setting.enabled) continue;
            if (!featureEnabled(runtime, setting.kind)) return error.FeatureNotEnabled;
        }
    }
};

fn featureEnabled(runtime: features.Config, kind: Kind) bool {
    return switch (kind) {
        .quic => runtime.isEnabled(.transport_quic),
        .http => runtime.isEnabled(.transport_http),
        .grpc => runtime.isEnabled(.transport_grpc),
        .websocket => runtime.isEnabled(.transport_websocket),
        .nats => runtime.isEnabled(.transport_nats),
    };
}

pub const defaults = [_]Setting{
    .{ .kind = .quic, .enabled = true },
    .{ .kind = .http, .enabled = true },
    .{ .kind = .grpc, .enabled = true },
    .{ .kind = .websocket, .enabled = true },
    .{ .kind = .nats, .enabled = true },
};

test "full profile permits the declared transport set" {
    try (Config{ .settings = &defaults }).validate(.{
        .profile = .full,
        .enabled = features.defaults(.full),
    });
}

test "transports can be disabled independently" {
    const settings = [_]Setting{
        .{ .kind = .http, .enabled = true },
        .{ .kind = .nats, .enabled = false },
    };
    const runtime = features.Config{ .profile = .custom, .enabled = &.{
        .ubiq, .supervisor, .crypto, .transport_http,
    } };
    const config = Config{ .settings = &settings };
    try config.validate(runtime);
    try std.testing.expect(config.isEnabled(.http));
    try std.testing.expect(!config.isEnabled(.nats));
}

test "enabled transport must exist in runtime feature registry" {
    const settings = [_]Setting{.{ .kind = .nats, .enabled = true }};
    const runtime = features.Config{ .profile = .secure, .enabled = features.defaults(.secure) };
    try std.testing.expectError(error.FeatureNotEnabled, (Config{ .settings = &settings }).validate(runtime));
}

test "duplicate transport declarations are rejected" {
    const settings = [_]Setting{
        .{ .kind = .http },
        .{ .kind = .http, .enabled = false },
    };
    const runtime = features.Config{ .profile = .full, .enabled = features.defaults(.full) };
    try std.testing.expectError(error.DuplicateTransport, (Config{ .settings = &settings }).validate(runtime));
}

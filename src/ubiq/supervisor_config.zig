const std = @import("std");
const features = @import("features.zig");

pub const Kind = enum {
    root,
    security,
    transport,
    sharder,
    healing,
};

pub const RestartStrategy = enum {
    one_for_one,
    rest_for_one,
    one_for_all,
};

pub const Setting = struct {
    kind: Kind,
    enabled: bool = true,
    restart: RestartStrategy = .one_for_one,
};

pub const ConfigError = error{
    DuplicateSupervisor,
    MissingRootSupervisor,
    MissingRuntimeFeature,
    MissingSupervisorFeature,
};

/// Immutable supervisor-tree declaration. This module describes intended
/// supervision; it does not itself spawn tasks or claim runtime health.
pub const Config = struct {
    settings: []const Setting,

    pub fn isEnabled(self: Config, kind: Kind) bool {
        for (self.settings) |setting| {
            if (setting.kind == kind) return setting.enabled;
        }
        return false;
    }

    pub fn strategy(self: Config, kind: Kind) ?RestartStrategy {
        for (self.settings) |setting| {
            if (setting.kind == kind and setting.enabled) return setting.restart;
        }
        return null;
    }

    pub fn validate(self: Config, runtime: features.Config) ConfigError!void {
        for (self.settings, 0..) |setting, index| {
            for (self.settings[0..index]) |previous| {
                if (previous.kind == setting.kind) return error.DuplicateSupervisor;
            }
            if (!setting.enabled) continue;
            if (setting.kind != .root and !self.isEnabled(.root))
                return error.MissingRootSupervisor;
            if (!runtime.isEnabled(.supervisor))
                return error.MissingSupervisorFeature;

            switch (setting.kind) {
                .root => {},
                .security => if (!runtime.isEnabled(.crypto)) return error.MissingRuntimeFeature,
                .transport => {
                    if (!hasTransport(runtime)) return error.MissingRuntimeFeature;
                },
                .sharder => if (!runtime.isEnabled(.sharder)) return error.MissingRuntimeFeature,
                .healing => if (!runtime.isEnabled(.self_healing)) return error.MissingRuntimeFeature,
            }
        }
    }
};

fn hasTransport(runtime: features.Config) bool {
    return runtime.isEnabled(.transport_quic) or
        runtime.isEnabled(.transport_http) or
        runtime.isEnabled(.transport_grpc) or
        runtime.isEnabled(.transport_websocket) or
        runtime.isEnabled(.transport_nats);
}

pub const default_settings = [_]Setting{
    .{ .kind = .root, .enabled = true, .restart = .one_for_all },
    .{ .kind = .security, .enabled = true, .restart = .one_for_one },
    .{ .kind = .transport, .enabled = true, .restart = .rest_for_one },
    .{ .kind = .sharder, .enabled = false, .restart = .one_for_one },
    .{ .kind = .healing, .enabled = false, .restart = .one_for_one },
};

test "default supervisor tree validates against full feature profile" {
    const config = Config{ .settings = &default_settings };
    try config.validate(.{ .profile = .full, .enabled = features.defaults(.full) });
    try std.testing.expectEqual(RestartStrategy.one_for_all, config.strategy(.root).?);
}

test "enabled child supervisor requires enabled root" {
    const settings = [_]Setting{
        .{ .kind = .root, .enabled = false },
        .{ .kind = .security, .enabled = true },
    };
    const runtime = features.Config{ .profile = .secure, .enabled = features.defaults(.secure) };
    try std.testing.expectError(error.MissingRootSupervisor, (Config{ .settings = &settings }).validate(runtime));
}

test "transport supervisor requires at least one configured transport" {
    const settings = [_]Setting{
        .{ .kind = .root },
        .{ .kind = .transport },
    };
    const runtime = features.Config{ .profile = .secure, .enabled = features.defaults(.secure) };
    try std.testing.expectError(error.MissingRuntimeFeature, (Config{ .settings = &settings }).validate(runtime));
}

test "duplicate supervisor kinds are rejected" {
    const settings = [_]Setting{
        .{ .kind = .root },
        .{ .kind = .root },
    };
    const runtime = features.Config{ .profile = .minimal, .enabled = features.defaults(.minimal) };
    try std.testing.expectError(error.DuplicateSupervisor, (Config{ .settings = &settings }).validate(runtime));
}

const std = @import("std");

pub const Profile = enum { minimal, secure, full, custom };

pub const Feature = enum {
    ubiq,
    supervisor,
    mtls,
    dpop,
    replay_guard,
    sharder,
    event_sourcing,
    crypto,
    projection,
    self_healing,
    predictive_self_healing,
    shadow_simulation,
    human_in_the_healing_loop,
    transport_quic,
    transport_http,
    transport_grpc,
    transport_websocket,
    transport_nats,
};

pub const Config = struct {
    profile: Profile,
    enabled: []const Feature,

    pub fn isEnabled(self: Config, feature: Feature) bool {
        for (self.enabled) |enabled| {
            if (enabled == feature) return true;
        }
        return false;
    }

    /// Validates the resolved feature set before any listener or worker starts.
    /// This validates declarations only; it does not claim that a compiled
    /// transport or production crypto provider exists.
    pub fn validate(self: Config) ValidationError!void {
        for (self.enabled, 0..) |feature, index| {
            // Duplicate declarations are ambiguous configuration, not harmless
            // aliases: reject them before dependency resolution/bootstrap.
            for (self.enabled[0..index]) |previous| {
                if (previous == feature) return error.DuplicateFeature;
            }
            switch (feature) {
                .supervisor => if (!self.isEnabled(.ubiq)) return error.MissingDependency,
                .mtls, .replay_guard => {
                    if (!self.isEnabled(.crypto)) return error.MissingDependency;
                },
                .dpop => {
                    if (!self.isEnabled(.crypto) or !self.isEnabled(.transport_http))
                        return error.MissingDependency;
                },
                .sharder => {
                    if (!self.isEnabled(.ubiq) or !self.isEnabled(.supervisor))
                        return error.MissingDependency;
                },
                .projection => if (!self.isEnabled(.event_sourcing)) return error.MissingDependency,
                .predictive_self_healing => if (!self.isEnabled(.self_healing)) return error.MissingDependency,
                .transport_quic, .transport_http, .transport_grpc, .transport_websocket, .transport_nats => {
                    if (!self.isEnabled(.ubiq) or !self.isEnabled(.supervisor) or !self.isEnabled(.crypto))
                        return error.MissingDependency;
                },
                else => {},
            }
        }
    }
};

pub const ValidationError = error{ MissingDependency, DuplicateFeature };

const minimal = [_]Feature{ .ubiq, .supervisor };
const secure = [_]Feature{ .ubiq, .supervisor, .crypto, .mtls, .replay_guard };
const full = [_]Feature{
    .ubiq,
    .supervisor,
    .crypto,
    .mtls,
    .dpop,
    .replay_guard,
    .sharder,
    .event_sourcing,
    .projection,
    .self_healing,
    .predictive_self_healing,
    .shadow_simulation,
    .human_in_the_healing_loop,
    .transport_quic,
    .transport_http,
    .transport_grpc,
    .transport_websocket,
    .transport_nats,
};

/// Returns profile defaults. Callers should copy these into an owned resolved
/// configuration before applying file/environment overrides.
pub fn defaults(profile: Profile) []const Feature {
    return switch (profile) {
        .minimal => &minimal,
        .secure => &secure,
        .full => &full,
        .custom => &.{},
    };
}

test "minimal profile has only core features" {
    const config = Config{ .profile = .minimal, .enabled = defaults(.minimal) };
    try config.validate();
    try std.testing.expect(config.isEnabled(.ubiq));
    try std.testing.expect(!config.isEnabled(.crypto));
}

test "remote transport cannot start without security dependencies" {
    const config = Config{
        .profile = .custom,
        .enabled = &.{ .ubiq, .transport_nats },
    };
    try std.testing.expectError(error.MissingDependency, config.validate());
}

test "DPoP is restricted to HTTP transport" {
    const config = Config{
        .profile = .custom,
        .enabled = &.{ .ubiq, .supervisor, .crypto, .dpop },
    };
    try std.testing.expectError(error.MissingDependency, config.validate());
}

test "projection requires event sourcing" {
    const config = Config{
        .profile = .custom,
        .enabled = &.{ .ubiq, .projection },
    };
    try std.testing.expectError(error.MissingDependency, config.validate());
}

test "duplicate feature declarations are rejected" {
    const config = Config{
        .profile = .custom,
        .enabled = &.{ .ubiq, .supervisor, .ubiq },
    };
    try std.testing.expectError(error.DuplicateFeature, config.validate());
}

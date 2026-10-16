const std = @import("std");
const features = @import("features.zig");
const supervisor_config = @import("supervisor_config.zig");

pub const Strategy = enum { one_for_one, rest_for_one, one_for_all };
pub const Kind = enum { root, security, transport, sharder, healing };
pub const Health = enum { disabled, starting, healthy, degraded, unhealthy };

pub const Node = struct {
    kind: Kind,
    enabled: bool,
    strategy: Strategy = .one_for_one,
    health: Health = .disabled,
    restart_count: u64 = 0,
    failure_count: u64 = 0,
};

pub const SupervisorError = error{ MissingDependency, DuplicateSupervisor, InvalidTransition, InvalidConfiguration };

pub const Tree = struct {
    nodes: [5]Node = .{
        .{ .kind = .root, .enabled = true, .health = .starting },
        .{ .kind = .security, .enabled = true, .health = .starting },
        .{ .kind = .transport, .enabled = true, .health = .starting },
        .{ .kind = .sharder, .enabled = false },
        .{ .kind = .healing, .enabled = false },
    },

    pub fn configureFromConfig(self: *Tree, config: supervisor_config.Config, runtime: features.Config) SupervisorError!void {
        config.validate(runtime) catch return error.InvalidConfiguration;
        for (config.settings) |setting| {
            const kind: Kind = switch (setting.kind) {
                .root => .root,
                .security => .security,
                .transport => .transport,
                .sharder => .sharder,
                .healing => .healing,
            };
            const strategy: Strategy = switch (setting.restart) {
                .one_for_one => .one_for_one,
                .rest_for_one => .rest_for_one,
                .one_for_all => .one_for_all,
            };
            try self.configure(kind, setting.enabled, strategy, runtime);
        }
    }

    pub fn configure(self: *Tree, kind: Kind, enabled: bool, strategy: Strategy, config: features.Config) SupervisorError!void {
        if (kind != .root and enabled and !config.isEnabled(.supervisor))
            return error.MissingDependency;
        if (kind == .security and enabled and !config.isEnabled(.crypto))
            return error.MissingDependency;
        if (kind == .transport and enabled and
            (!config.isEnabled(.ubiq) or !config.isEnabled(.crypto)))
            return error.MissingDependency;
        if (kind == .sharder and enabled and !config.isEnabled(.sharder))
            return error.MissingDependency;
        if (kind == .healing and enabled and !config.isEnabled(.self_healing))
            return error.MissingDependency;

        const index = @intFromEnum(kind);
        self.nodes[index].enabled = enabled;
        self.nodes[index].strategy = strategy;
        self.nodes[index].health = if (enabled) .starting else .disabled;
    }

    pub fn markHealthy(self: *Tree, kind: Kind) SupervisorError!void {
        const node = &self.nodes[@intFromEnum(kind)];
        if (!node.enabled or node.health == .disabled) return error.InvalidTransition;
        node.health = .healthy;
    }

    pub fn recordFailure(self: *Tree, kind: Kind) void {
        const node = &self.nodes[@intFromEnum(kind)];
        if (!node.enabled) return;
        node.failure_count += 1;
        node.health = .unhealthy;
    }

    pub fn recordRestart(self: *Tree, kind: Kind) void {
        const node = &self.nodes[@intFromEnum(kind)];
        if (!node.enabled) return;
        node.restart_count += 1;
        node.health = .starting;
    }

    pub fn metrics(self: *const Tree) Metrics {
        var result = Metrics{};
        for (self.nodes) |node| {
            if (node.enabled) result.enabled += 1;
            if (node.health == .healthy) result.healthy += 1;
            result.restarts += node.restart_count;
            result.failures += node.failure_count;
        }
        return result;
    }
};

pub const Metrics = struct { enabled: u8 = 0, healthy: u8 = 0, restarts: u64 = 0, failures: u64 = 0 };

test "supervisor configuration checks runtime dependencies and records health metrics" {
    var tree = Tree{};
    const config = features.Config{ .profile = .full, .enabled = features.defaults(.full) };
    try tree.configure(.sharder, true, .rest_for_one, config);
    try tree.markHealthy(.sharder);
    tree.recordFailure(.sharder);
    tree.recordRestart(.sharder);
    const metrics = tree.metrics();
    try std.testing.expectEqual(@as(u8, 4), metrics.enabled);
    try std.testing.expectEqual(@as(u64, 1), metrics.failures);
    try std.testing.expectEqual(@as(u64, 1), metrics.restarts);
}

test "supervisor cannot enable security tree without crypto" {
    var tree = Tree{};
    const config = features.Config{ .profile = .minimal, .enabled = features.defaults(.minimal) };
    try std.testing.expectError(error.MissingDependency, tree.configure(.security, true, .one_for_one, config));
}


test "supervisor runtime applies declarative tree and strategies" {
    const runtime = features.Config{ .profile = .full, .enabled = features.defaults(.full) };
    const config = supervisor_config.Config{ .settings = &supervisor_config.default_settings };
    var tree = Tree{};
    try tree.configureFromConfig(config, runtime);
    try std.testing.expectEqual(Strategy.one_for_all, tree.nodes[@intFromEnum(Kind.root)].strategy);
    try std.testing.expectEqual(Health.disabled, tree.nodes[@intFromEnum(Kind.sharder)].health);
}

test "supervisor runtime rejects configuration with missing runtime dependencies" {
    const runtime = features.Config{ .profile = .minimal, .enabled = features.defaults(.minimal) };
    const config = supervisor_config.Config{ .settings = &supervisor_config.default_settings };
    var tree = Tree{};
    try std.testing.expectError(error.InvalidConfiguration, tree.configureFromConfig(config, runtime));
}

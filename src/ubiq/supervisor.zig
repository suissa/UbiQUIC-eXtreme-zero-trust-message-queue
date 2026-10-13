const std = @import("std");
const features = @import("features.zig");

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

pub const SupervisorError = error{ MissingDependency, DuplicateSupervisor, InvalidTransition };

pub const Tree = struct {
    nodes: [5]Node = .{
        .{ .kind = .root, .enabled = true, .health = .starting },
        .{ .kind = .security, .enabled = true, .health = .starting },
        .{ .kind = .transport, .enabled = true, .health = .starting },
        .{ .kind = .sharder, .enabled = false },
        .{ .kind = .healing, .enabled = false },
    },

    /// Resolve the complete tree from the validated runtime feature set before
    /// any supervisor is started. Disabled features remain disabled by default.
    pub fn initialize(self: *Tree, config: features.Config) void {
        for (&self.nodes) |*node| {
            const enabled = featureEnabled(node.kind, config);
            node.enabled = enabled;
            node.health = if (enabled) .starting else .disabled;
            node.restart_count = 0;
            node.failure_count = 0;
        }
    }

    pub fn configure(self: *Tree, kind: Kind, enabled: bool, strategy: Strategy, config: features.Config) SupervisorError!void {
        if (enabled and !featureEnabled(kind, config)) return error.MissingDependency;

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

fn featureEnabled(kind: Kind, config: features.Config) bool {
    if (!config.isEnabled(.supervisor)) return false;
    return switch (kind) {
        .root => true,
        .security => config.isEnabled(.crypto),
        .transport => config.isEnabled(.ubiq) and config.isEnabled(.crypto),
        .sharder => config.isEnabled(.sharder),
        .healing => config.isEnabled(.self_healing),
    };
}

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

test "tree initialization follows runtime feature profile without enabling unavailable supervisors" {
    var tree = Tree{};
    const minimal = features.Config{ .profile = .minimal, .enabled = features.defaults(.minimal) };
    tree.initialize(minimal);
    try std.testing.expect(tree.nodes[@intFromEnum(Kind.root)].enabled);
    try std.testing.expect(!tree.nodes[@intFromEnum(Kind.security)].enabled);
    try std.testing.expect(!tree.nodes[@intFromEnum(Kind.transport)].enabled);
    try std.testing.expect(!tree.nodes[@intFromEnum(Kind.sharder)].enabled);
    try std.testing.expect(!tree.nodes[@intFromEnum(Kind.healing)].enabled);
    try std.testing.expectEqual(@as(u8, 1), tree.metrics().enabled);
}

test "tree initialization disables all supervisors when supervisor feature is absent" {
    var tree = Tree{};
    const config = features.Config{ .profile = .custom, .enabled = &.{} };
    tree.initialize(config);
    try std.testing.expectEqual(@as(u8, 0), tree.metrics().enabled);
    try std.testing.expectError(error.MissingDependency, tree.configure(.root, true, .one_for_all, config));
}

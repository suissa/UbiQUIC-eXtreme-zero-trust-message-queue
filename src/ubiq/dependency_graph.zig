const std = @import("std");
const features = @import("features.zig");

pub const Relation = enum { required, optional, conflicts };
pub const Edge = struct { from: features.Feature, to: features.Feature, relation: Relation };
pub const GraphError = error{ MissingRequiredDependency, ConflictingFeatures, DependencyCycle };

/// Required edges define bootstrap order. Optional edges are diagnostic only;
/// conflicting edges prohibit a configuration containing both endpoints.
pub const default_edges = [_]Edge{
    .{ .from = .supervisor, .to = .ubiq, .relation = .required },
    .{ .from = .mtls, .to = .crypto, .relation = .required },
    .{ .from = .replay_guard, .to = .crypto, .relation = .required },
    .{ .from = .dpop, .to = .crypto, .relation = .required },
    .{ .from = .dpop, .to = .transport_http, .relation = .required },
    .{ .from = .sharder, .to = .supervisor, .relation = .required },
    .{ .from = .projection, .to = .event_sourcing, .relation = .required },
    .{ .from = .predictive_self_healing, .to = .self_healing, .relation = .required },
    .{ .from = .transport_quic, .to = .crypto, .relation = .required },
    .{ .from = .transport_http, .to = .crypto, .relation = .required },
    .{ .from = .transport_grpc, .to = .crypto, .relation = .required },
    .{ .from = .transport_websocket, .to = .crypto, .relation = .required },
    .{ .from = .transport_nats, .to = .crypto, .relation = .required },
};

pub fn validate(config: features.Config, edges: []const Edge) GraphError!void {
    for (edges) |edge| {
        const from_enabled = config.isEnabled(edge.from);
        const to_enabled = config.isEnabled(edge.to);
        switch (edge.relation) {
            .required => if (from_enabled and !to_enabled) return error.MissingRequiredDependency,
            .conflicts => if (from_enabled and to_enabled) return error.ConflictingFeatures,
            .optional => {},
        }
    }
    try detectCycles(edges);
}

pub fn detectCycles(edges: []const Edge) GraphError!void {
    const count = @typeInfo(features.Feature).@"enum".fields.len;
    var colors: [count]u2 = @splat(0);
    for (0..count) |index| {
        if (colors[index] == 0) try visit(@enumFromInt(index), edges, &colors);
    }
}

fn visit(node: features.Feature, edges: []const Edge, colors: []u2) GraphError!void {
    const index = @intFromEnum(node);
    if (colors[index] == 1) return error.DependencyCycle;
    if (colors[index] == 2) return;
    colors[index] = 1;
    for (edges) |edge| {
        if (edge.relation != .required or edge.from != node) continue;
        try visit(edge.to, edges, colors);
    }
    colors[index] = 2;
}

test "default graph validates full profile" {
    try validate(.{ .profile = .full, .enabled = features.defaults(.full) }, &default_edges);
}

test "graph rejects missing dependency and conflicts" {
    const enabled = [_]features.Feature{ .ubiq, .crypto, .transport_nats };
    try std.testing.expectError(error.MissingRequiredDependency, validate(
        .{ .profile = .custom, .enabled = &enabled },
        &.{.{ .from = .transport_nats, .to = .supervisor, .relation = .required }},
    ));
    try std.testing.expectError(error.ConflictingFeatures, validate(
        .{ .profile = .custom, .enabled = &enabled },
        &.{.{ .from = .crypto, .to = .transport_nats, .relation = .conflicts }},
    ));
}

test "graph detects required dependency cycles" {
    try std.testing.expectError(error.DependencyCycle, detectCycles(&.{
        .{ .from = .ubiq, .to = .crypto, .relation = .required },
        .{ .from = .crypto, .to = .ubiq, .relation = .required },
    }));
}

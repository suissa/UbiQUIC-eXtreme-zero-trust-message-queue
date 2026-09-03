const std = @import("std");

pub const ClusterError = error{
    ClusterFull,
    DuplicateNode,
    UnknownNode,
    NoHealthyNodes,
    InvalidReplicationFactor,
    InvalidShardCount,
};

pub const Node = struct {
    id: []const u8,
    healthy: bool = true,
};

pub fn Cluster(comptime max_nodes: usize) type {
    return struct {
        const Self = @This();

        pub const Placement = struct {
            node_indexes: [max_nodes]usize = undefined,
            len: usize = 0,

            pub fn contains(self: Placement, node_index: usize) bool {
                var i: usize = 0;
                while (i < self.len) : (i += 1) {
                    if (self.node_indexes[i] == node_index) return true;
                }
                return false;
            }
        };

        nodes: [max_nodes]Node = undefined,
        len: usize = 0,

        pub fn register(self: *Self, new_node: Node) ClusterError!usize {
            if (self.find(new_node.id) != null) return error.DuplicateNode;
            if (self.len >= max_nodes) return error.ClusterFull;
            const index = self.len;
            self.nodes[index] = new_node;
            self.len += 1;
            return index;
        }

        pub fn setHealthy(self: *Self, node_id: []const u8, healthy: bool) ClusterError!void {
            const index = self.find(node_id) orelse return error.UnknownNode;
            self.nodes[index].healthy = healthy;
        }

        /// Rendezvous-style deterministic placement. No central leader is
        /// required to compute the same replica set from the same membership.
        pub fn replicasFor(self: *const Self, key: []const u8, replication_factor: usize) ClusterError!Placement {
            if (replication_factor == 0 or replication_factor > max_nodes) return error.InvalidReplicationFactor;

            var result = Placement{};
            while (result.len < replication_factor) {
                var best_index: ?usize = null;
                var best_score: u64 = 0;

                var i: usize = 0;
                while (i < self.len) : (i += 1) {
                    if (!self.nodes[i].healthy or result.contains(i)) continue;
                    const candidate = placementScore(key, self.nodes[i].id);
                    if (best_index == null or candidate > best_score) {
                        best_index = i;
                        best_score = candidate;
                    }
                }

                const selected = best_index orelse {
                    if (result.len == 0) return error.NoHealthyNodes;
                    break;
                };
                result.node_indexes[result.len] = selected;
                result.len += 1;
            }
            return result;
        }

        pub fn shardFor(_: *const Self, key: []const u8, shard_count: u32) ClusterError!u32 {
            if (shard_count == 0) return error.InvalidShardCount;
            return @intCast(hash64(key) % shard_count);
        }

        pub fn nodeAt(self: *const Self, index: usize) *const Node {
            return &self.nodes[index];
        }

        fn find(self: *const Self, node_id: []const u8) ?usize {
            var i: usize = 0;
            while (i < self.len) : (i += 1) {
                if (std.mem.eql(u8, self.nodes[i].id, node_id)) return i;
            }
            return null;
        }
    };
}

/// Stable 64-bit FNV-1a variant used only for placement, never security.
fn hash64(bytes: []const u8) u64 {
    var hash: u64 = 14695981039346656037;
    for (bytes) |byte| {
        hash ^= byte;
        hash *%= 1099511628211;
    }
    return hash;
}

fn placementScore(key: []const u8, node_id: []const u8) u64 {
    var hash: u64 = 14695981039346656037;
    for (key) |byte| {
        hash ^= byte;
        hash *%= 1099511628211;
    }
    hash ^= 0xff;
    hash *%= 1099511628211;
    for (node_id) |byte| {
        hash ^= byte;
        hash *%= 1099511628211;
    }
    return hash;
}

test "replication is deterministic and excludes unhealthy nodes" {
    var cluster = Cluster(4){};
    _ = try cluster.register(.{ .id = "node-a" });
    _ = try cluster.register(.{ .id = "node-b" });
    _ = try cluster.register(.{ .id = "node-c" });

    const first = try cluster.replicasFor("customer:42", 2);
    const second = try cluster.replicasFor("customer:42", 2);
    try std.testing.expectEqual(first.node_indexes[0], second.node_indexes[0]);
    try std.testing.expectEqual(first.node_indexes[1], second.node_indexes[1]);

    const failed_id = cluster.nodeAt(first.node_indexes[0]).id;
    try cluster.setHealthy(failed_id, false);
    const failover = try cluster.replicasFor("customer:42", 2);
    try std.testing.expect(!std.mem.eql(u8, cluster.nodeAt(failover.node_indexes[0]).id, failed_id));
    try std.testing.expect(!std.mem.eql(u8, cluster.nodeAt(failover.node_indexes[1]).id, failed_id));
}

test "sharding is stable and bounded" {
    var cluster = Cluster(1){};
    _ = try cluster.register(.{ .id = "node-a" });
    const shard_a = try cluster.shardFor("invoice:100", 64);
    const shard_b = try cluster.shardFor("invoice:100", 64);
    try std.testing.expectEqual(shard_a, shard_b);
    try std.testing.expect(shard_a < 64);
}

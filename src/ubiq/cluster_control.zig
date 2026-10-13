const std = @import("std");

pub const Error = error{
    CapacityExceeded,
    DuplicateNode,
    UnknownNode,
    Unauthenticated,
    StaleEpoch,
    NodeUnhealthy,
    StaleFence,
    InvalidHeartbeatTimeout,
};

pub const Node = struct {
    id: []const u8,
    healthy: bool = true,
    last_heartbeat_ms: u64,
    joined_epoch: u64,
};

pub fn Membership(comptime capacity: usize) type {
    return struct {
        const Self = @This();
        nodes: [capacity]Node = undefined,
        len: usize = 0,
        epoch: u64 = 0,
        fence_generation: u64 = 0,

        pub fn join(self: *Self, id: []const u8, authenticated: bool, now_ms: u64) Error!u64 {
            if (!authenticated) return error.Unauthenticated;
            if (self.find(id) != null) return error.DuplicateNode;
            if (self.len == capacity) return error.CapacityExceeded;
            self.epoch += 1;
            self.nodes[self.len] = .{
                .id = id,
                .healthy = true,
                .last_heartbeat_ms = now_ms,
                .joined_epoch = self.epoch,
            };
            self.len += 1;
            return self.epoch;
        }

        pub fn leave(self: *Self, id: []const u8) Error!u64 {
            const index = self.find(id) orelse return error.UnknownNode;
            var i = index;
            while (i + 1 < self.len) : (i += 1) self.nodes[i] = self.nodes[i + 1];
            self.len -= 1;
            self.epoch += 1;
            self.fence_generation += 1;
            return self.epoch;
        }

        pub fn heartbeat(self: *Self, id: []const u8, claimed_epoch: u64, now_ms: u64) Error!void {
            if (claimed_epoch != self.epoch) return error.StaleEpoch;
            const index = self.find(id) orelse return error.UnknownNode;
            self.nodes[index].last_heartbeat_ms = now_ms;
            self.nodes[index].healthy = true;
        }

        pub fn expireUnhealthy(self: *Self, now_ms: u64, timeout_ms: u64) Error!usize {
            if (timeout_ms == 0) return error.InvalidHeartbeatTimeout;
            var expired: usize = 0;
            for (self.nodes[0..self.len]) |*node| {
                if (node.healthy and now_ms >= node.last_heartbeat_ms and
                    now_ms - node.last_heartbeat_ms > timeout_ms)
                {
                    node.healthy = false;
                    expired += 1;
                }
            }
            if (expired != 0) {
                self.epoch += 1;
                self.fence_generation += 1;
            }
            return expired;
        }

        pub fn advanceFence(self: *Self) u64 {
            self.fence_generation += 1;
            return self.fence_generation;
        }

        pub fn validateWrite(self: *const Self, node_id: []const u8, generation: u64) Error!void {
            const index = self.find(node_id) orelse return error.UnknownNode;
            if (!self.nodes[index].healthy) return error.NodeUnhealthy;
            if (generation != self.fence_generation) return error.StaleFence;
        }

        pub fn find(self: *const Self, id: []const u8) ?usize {
            for (self.nodes[0..self.len], 0..) |node, index| {
                if (std.mem.eql(u8, node.id, id)) return index;
            }
            return null;
        }
    };
}

test "membership requires authenticated joins and fences stale writers" {
    var membership = Membership(4){};
    try std.testing.expectError(error.Unauthenticated, membership.join("node-a", false, 0));
    const epoch = try membership.join("node-a", true, 10);
    try membership.heartbeat("node-a", epoch, 20);
    const generation = membership.advanceFence();
    try membership.validateWrite("node-a", generation);
    _ = membership.advanceFence();
    try std.testing.expectError(error.StaleFence, membership.validateWrite("node-a", generation));
}

test "missed heartbeats mark node unhealthy and advance epoch" {
    var membership = Membership(2){};
    _ = try membership.join("node-a", true, 10);
    const expired = try membership.expireUnhealthy(30, 10);
    try std.testing.expectEqual(@as(usize, 1), expired);
    try std.testing.expectError(error.NodeUnhealthy, membership.validateWrite("node-a", membership.fence_generation));
}

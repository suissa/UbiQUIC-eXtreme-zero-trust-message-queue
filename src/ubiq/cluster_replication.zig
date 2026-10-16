const std = @import("std");
const cluster_control = @import("cluster_control.zig");

pub const Error = error{
    QueueFull,
    InvalidRecord,
    StaleEpoch,
    StaleFence,
    DuplicateRecord,
    SequenceRegression,
    UnknownReplica,
};

pub const ReplicaRecord = struct {
    event_id: []const u8,
    source_node: []const u8,
    target_node: []const u8,
    membership_epoch: u64,
    fencing_generation: u64,
    sequence: u64,
    payload: []const u8,
};

pub const ReplicaState = enum { pending, delivered, applied };

/// Bounded in-memory replication outbox. The network adapter is responsible
/// for authenticated transmission; this queue never claims remote durability.
pub fn ReplicaQueue(comptime capacity: usize) type {
    return struct {
        const Self = @This();
        entries: [capacity]Entry = undefined,
        len: usize = 0,
        last_sequence: u64 = 0,

        const Entry = struct {
            record: ReplicaRecord,
            state: ReplicaState = .pending,
        };

        pub fn enqueue(self: *Self, record: ReplicaRecord, current_epoch: u64, current_fence: u64) Error!usize {
            if (record.event_id.len == 0 or record.source_node.len == 0 or
                record.target_node.len == 0 or record.source_node.len == record.target_node.len and
                std.mem.eql(u8, record.source_node, record.target_node))
                return error.InvalidRecord;
            if (record.membership_epoch != current_epoch) return error.StaleEpoch;
            if (record.fencing_generation != current_fence) return error.StaleFence;
            for (self.entries[0..self.len], 0..) |entry, index| {
                if (std.mem.eql(u8, entry.record.event_id, record.event_id) and
                    std.mem.eql(u8, entry.record.target_node, record.target_node))
                    return error.DuplicateRecord;
            }
            if (self.len == capacity) return error.QueueFull;
            if (record.sequence <= self.last_sequence) return error.SequenceRegression;
            self.entries[self.len] = .{ .record = record };
            self.len += 1;
            self.last_sequence = record.sequence;
            return self.len - 1;
        }

        pub fn markDelivered(self: *Self, index: usize, epoch: u64, fence: u64) Error!void {
            const entry = try self.entryAt(index);
            try self.validateAuthority(entry.record, epoch, fence);
            if (entry.state != .pending) return error.InvalidRecord;
            entry.state = .delivered;
        }

        /// Acknowledging receipt is not equivalent to applying the replica.
        pub fn markApplied(self: *Self, index: usize, epoch: u64, fence: u64) Error!void {
            const entry = try self.entryAt(index);
            try self.validateAuthority(entry.record, epoch, fence);
            if (entry.state != .delivered) return error.InvalidRecord;
            entry.state = .applied;
        }

        pub fn get(self: *const Self, index: usize) Error!ReplicaRecord {
            if (index >= self.len) return error.InvalidRecord;
            return self.entries[index].record;
        }

        pub fn state(self: *const Self, index: usize) Error!ReplicaState {
            if (index >= self.len) return error.InvalidRecord;
            return self.entries[index].state;
        }

        fn entryAt(self: *Self, index: usize) Error!*Entry {
            if (index >= self.len) return error.InvalidRecord;
            return &self.entries[index];
        }

        fn validateAuthority(_: *const Self, record: ReplicaRecord, epoch: u64, fence: u64) Error!void {
            if (record.membership_epoch != epoch) return error.StaleEpoch;
            if (record.fencing_generation != fence) return error.StaleFence;
        }
    };
}

test "replication queue is bounded and validates fencing authority" {
    var queue = ReplicaQueue(1){};
    const record = ReplicaRecord{
        .event_id = "evt-1",
        .source_node = "node-a",
        .target_node = "node-b",
        .membership_epoch = 4,
        .fencing_generation = 9,
        .sequence = 1,
        .payload = "payload",
    };
    const index = try queue.enqueue(record, 4, 9);
    try std.testing.expectError(error.QueueFull, queue.enqueue(.{
        .event_id = "evt-2",
        .source_node = "node-a",
        .target_node = "node-b",
        .membership_epoch = 4,
        .fencing_generation = 9,
        .sequence = 2,
        .payload = "payload",
    }, 4, 9));
    try std.testing.expectError(error.StaleFence, queue.markDelivered(index, 4, 10));
    try queue.markDelivered(index, 4, 9);
    try std.testing.expectEqual(ReplicaState.delivered, try queue.state(index));
    try queue.markApplied(index, 4, 9);
    try std.testing.expectEqual(ReplicaState.applied, try queue.state(index));
}

test "replication queue rejects duplicate targets and stale epochs" {
    var queue = ReplicaQueue(4){};
    const record = ReplicaRecord{
        .event_id = "evt-1",
        .source_node = "node-a",
        .target_node = "node-b",
        .membership_epoch = 2,
        .fencing_generation = 5,
        .sequence = 1,
        .payload = "payload",
    };
    _ = try queue.enqueue(record, 2, 5);
    try std.testing.expectError(error.DuplicateRecord, queue.enqueue(record, 2, 5));
    try std.testing.expectError(error.StaleEpoch, queue.enqueue(.{
        .event_id = "evt-2",
        .source_node = "node-a",
        .target_node = "node-c",
        .membership_epoch = 1,
        .fencing_generation = 5,
        .sequence = 2,
        .payload = "payload",
    }, 2, 5));
}

test "replica receipt cannot be marked applied before delivery" {
    var queue = ReplicaQueue(2){};
    const index = try queue.enqueue(.{
        .event_id = "evt-1",
        .source_node = "node-a",
        .target_node = "node-b",
        .membership_epoch = 1,
        .fencing_generation = 1,
        .sequence = 1,
        .payload = "payload",
    }, 1, 1);
    try std.testing.expectError(error.InvalidRecord, queue.markApplied(index, 1, 1));
}

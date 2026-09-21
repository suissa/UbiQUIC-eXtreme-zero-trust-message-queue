const std = @import("std");

pub const SecurityRuntimeError = error{
    ReplayDetected,
    ReplayTableFull,
    IdempotencyConflict,
    IdempotencyTableFull,
    UnknownIdempotencyKey,
};

pub const ReplayEntry = struct {
    nonce: []const u8,
    expires_at_ms: u64,
};

pub fn ReplayWindow(comptime capacity: usize) type {
    return struct {
        const Self = @This();
        entries: [capacity]ReplayEntry = undefined;
        len: usize = 0;

        pub fn accept(self: *Self, nonce: []const u8, now_ms: u64, ttl_ms: u64) SecurityRuntimeError!void {
            var i: usize = 0;
            while (i < self.len) : (i += 1) {
                if (now_ms >= self.entries[i].expires_at_ms) continue;
                if (std.mem.eql(u8, self.entries[i].nonce, nonce)) return error.ReplayDetected;
            }
            if (self.len < capacity) {
                self.entries[self.len] = .{ .nonce = nonce, .expires_at_ms = now_ms + ttl_ms };
                self.len += 1;
                return;
            }
            var oldest: usize = 0;
            for (self.entries[1..self.len], 1..) |entry, index| {
                if (entry.expires_at_ms < self.entries[oldest].expires_at_ms) oldest = index;
            }
            if (now_ms >= self.entries[oldest].expires_at_ms) {
                self.entries[oldest] = .{ .nonce = nonce, .expires_at_ms = now_ms + ttl_ms };
                return;
            }
            return error.ReplayTableFull;
        }

        pub fn reap(self: *Self, now_ms: u64) void {
            var write: usize = 0;
            for (self.entries[0..self.len]) |entry| {
                if (now_ms < entry.expires_at_ms) {
                    self.entries[write] = entry;
                    write += 1;
                }
            }
            self.len = write;
        }
    };
}

pub const Outcome = enum { dispatched, effect_unknown, settled_success, settled_failure };

pub const IdempotencyRecord = struct {
    key: []const u8,
    operation: []const u8,
    payload_digest: []const u8,
    outcome: Outcome,
};

pub fn IdempotencyTable(comptime capacity: usize) type {
    return struct {
        const Self = @This();
        records: [capacity]IdempotencyRecord = undefined;
        len: usize = 0;

        pub fn begin(self: *Self, key: []const u8, operation: []const u8, payload_digest: []const u8) SecurityRuntimeError!Outcome {
            if (self.find(key, operation)) |index| {
                const record = self.records[index];
                if (!std.mem.eql(u8, record.payload_digest, payload_digest)) return error.IdempotencyConflict;
                return record.outcome;
            }
            if (self.len >= capacity) return error.IdempotencyTableFull;
            self.records[self.len] = .{ .key = key, .operation = operation, .payload_digest = payload_digest, .outcome = .dispatched };
            self.len += 1;
            return .dispatched;
        }

        pub fn settle(self: *Self, key: []const u8, operation: []const u8, outcome: Outcome) SecurityRuntimeError!void {
            const index = self.find(key, operation) orelse return error.UnknownIdempotencyKey;
            self.records[index].outcome = outcome;
        }

        pub fn get(self: *const Self, key: []const u8, operation: []const u8) ?Outcome {
            const index = self.find(key, operation) orelse return null;
            return self.records[index].outcome;
        }

        fn find(self: *const Self, key: []const u8, operation: []const u8) ?usize {
            var i: usize = 0;
            while (i < self.len) : (i += 1) {
                if (std.mem.eql(u8, self.records[i].key, key) and
                    std.mem.eql(u8, self.records[i].operation, operation)) return i;
            }
            return null;
        }
    };
}

test "replay window distinguishes redelivery from replay" {
    var window = ReplayWindow(2){};
    try window.accept("nonce-1", 0, 10);
    try std.testing.expectError(error.ReplayDetected, window.accept("nonce-1", 1, 10));
    try window.accept("nonce-1", 11, 10);
}

test "idempotency rejects same key for different payload" {
    var table = IdempotencyTable(4){};
    _ = try table.begin("idem", "Financial.createInvoice", "digest-a");
    try std.testing.expectError(error.IdempotencyConflict, table.begin("idem", "Financial.createInvoice", "digest-b"));
    try table.settle("idem", "Financial.createInvoice", .settled_success);
    try std.testing.expectEqual(Outcome.settled_success, table.get("idem", "Financial.createInvoice").?);
}

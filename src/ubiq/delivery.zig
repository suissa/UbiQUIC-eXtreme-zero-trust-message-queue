const std = @import("std");
const event = @import("event.zig");

pub const DeliveryState = enum {
    published,
    leased,
    received,
    settled_ok,
    settled_error,
    expired,
};

pub const DeliveryError = error{
    QueueFull,
    NothingAvailable,
    InvalidRecord,
    NotLeaseOwner,
    InvalidTransition,
};

pub const Lease = struct {
    owner: []const u8,
    until_ms: u64,
    attempt: u32,

    pub fn active(self: Lease, now_ms: u64) bool {
        return now_ms < self.until_ms;
    }
};

pub const DeliveryRecord = struct {
    envelope: event.Envelope,
    state: DeliveryState = .published,
    lease: ?Lease = null,

    pub fn isSettled(self: DeliveryRecord) bool {
        return self.state == .settled_ok or self.state == .settled_error;
    }
};

/// Bounded, allocation-free reference broker for local Actor-to-Actor messaging.
/// It deliberately models RECEIVED separately from SETTLED_* so a transport ACK
/// can never be mistaken for proof that the worker executed the effect.
pub fn MemoryBroker(comptime capacity: usize) type {
    return struct {
        const Self = @This();

        records: [capacity]DeliveryRecord = undefined,
        len: usize = 0,

        pub fn publish(self: *Self, envelope: event.Envelope) DeliveryError!usize {
            if (self.findByIdempotencyKey(envelope.idempotency_key)) |existing| return existing;
            if (self.len >= capacity) return error.QueueFull;

            const index = self.len;
            self.records[index] = .{ .envelope = envelope };
            self.len += 1;
            return index;
        }

        pub fn claim(self: *Self, worker: []const u8, now_ms: u64, lease_ms: u64) DeliveryError!usize {
            var i: usize = 0;
            while (i < self.len) : (i += 1) {
                const record = &self.records[i];
                if (record.state == .published or record.state == .expired) {
                    const next_attempt: u32 = if (record.lease) |lease| lease.attempt + 1 else 1;
                    record.state = .leased;
                    record.lease = .{ .owner = worker, .until_ms = now_ms + lease_ms, .attempt = next_attempt };
                    return i;
                }
            }
            return error.NothingAvailable;
        }

        pub fn markReceived(self: *Self, index: usize, worker: []const u8) DeliveryError!void {
            const record = try self.record(index);
            try requireOwner(record, worker);
            if (record.state != .leased) return error.InvalidTransition;
            record.state = .received;
        }

        pub fn settle(self: *Self, index: usize, worker: []const u8, result: event.EventState) DeliveryError!void {
            const record = try self.record(index);
            try requireOwner(record, worker);
            if (record.state != .leased and record.state != .received) return error.InvalidTransition;

            record.state = switch (result) {
                .ok => .settled_ok,
                .error => .settled_error,
                .request => return error.InvalidTransition,
            };
            record.lease = null;
        }

        pub fn reapExpired(self: *Self, now_ms: u64) void {
            var i: usize = 0;
            while (i < self.len) : (i += 1) {
                const record = &self.records[i];
                if (record.state == .leased or record.state == .received) {
                    if (record.lease) |lease| {
                        if (!lease.active(now_ms)) record.state = .expired;
                    }
                }
            }
        }

        pub fn get(self: *const Self, index: usize) DeliveryError!*const DeliveryRecord {
            if (index >= self.len) return error.InvalidRecord;
            return &self.records[index];
        }

        fn record(self: *Self, index: usize) DeliveryError!*DeliveryRecord {
            if (index >= self.len) return error.InvalidRecord;
            return &self.records[index];
        }

        fn findByIdempotencyKey(self: *const Self, key: []const u8) ?usize {
            var i: usize = 0;
            while (i < self.len) : (i += 1) {
                if (std.mem.eql(u8, self.records[i].envelope.idempotency_key, key)) return i;
            }
            return null;
        }

        fn requireOwner(record: *const DeliveryRecord, worker: []const u8) DeliveryError!void {
            const lease = record.lease orelse return error.NotLeaseOwner;
            if (!std.mem.eql(u8, lease.owner, worker)) return error.NotLeaseOwner;
        }
    };
}

fn sampleEnvelope(idempotency_key: []const u8) !event.Envelope {
    return .{
        .id = "01K-evt",
        .event = try event.CanonicalEvent.parse("Financial.createInvoice.request"),
        .correlation_id = "01K-correlation",
        .causation_id = "01K-cause",
        .idempotency_key = idempotency_key,
        .schema_id = "Financial.createInvoice.request@1",
        .payload = "{}",
        .guarantee = .processed,
        .created_at_ms = 0,
    };
}

test "transport receipt is not execution settlement" {
    var broker = MemoryBroker(4){};
    const index = try broker.publish(try sampleEnvelope("idem-1"));
    _ = try broker.claim("worker-a", 0, 100);
    try broker.markReceived(index, "worker-a");

    const received = try broker.get(index);
    try std.testing.expectEqual(DeliveryState.received, received.state);
    try std.testing.expect(!received.isSettled());

    try broker.settle(index, "worker-a", .ok);
    const settled = try broker.get(index);
    try std.testing.expectEqual(DeliveryState.settled_ok, settled.state);
    try std.testing.expect(settled.isSettled());
}

test "expired lease is reassigned to another worker" {
    var broker = MemoryBroker(4){};
    _ = try broker.publish(try sampleEnvelope("idem-2"));
    const first = try broker.claim("worker-a", 0, 10);
    try broker.markReceived(first, "worker-a");

    broker.reapExpired(10);
    const second = try broker.claim("worker-b", 10, 10);
    try std.testing.expectEqual(first, second);
    try broker.settle(second, "worker-b", .ok);
}

test "idempotency key deduplicates publication" {
    var broker = MemoryBroker(4){};
    const first = try broker.publish(try sampleEnvelope("same-key"));
    const second = try broker.publish(try sampleEnvelope("same-key"));
    try std.testing.expectEqual(first, second);
    try std.testing.expectEqual(@as(usize, 1), broker.len);
}

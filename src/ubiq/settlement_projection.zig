const std = @import("std");
const journal = @import("settlement_journal.zig");

pub const ProjectionError = error{
    SequenceRegression,
    IdempotencyConflict,
    InvalidTransition,
};

/// In-memory recovery projection rebuilt from the durable journal at startup.
/// It owns every string copied from replay records because the JSON parser's
/// buffers are released after each replay callback.
pub const Projection = struct {
    allocator: std.mem.Allocator,
    entries: std.ArrayList(Entry) = .empty,
    last_sequence: u64 = 0,

    const Entry = struct {
        record: journal.Record,
    };

    pub fn init(allocator: std.mem.Allocator) Projection {
        return .{ .allocator = allocator };
    }

    pub fn deinit(self: *Projection) void {
        for (self.entries.items) |entry| deinitRecord(self.allocator, entry.record);
        self.entries.deinit(self.allocator);
        self.* = undefined;
    }

    /// Replays complete journal records in order; ordering errors fail closed.
    pub fn recover(self: *Projection, store: *const journal.Store) !usize {
        return store.replay(self, replayVisitor);
    }

    /// Validate before the caller performs an irreversible durable append.
    pub fn validate(self: *const Projection, record: journal.Record) ProjectionError!void {
        if (record.sequence <= self.last_sequence) return error.SequenceRegression;

        if (self.findByIdempotencyKey(record.idempotency_key)) |existing_index| {
            const existing = self.entries.items[existing_index].record;
            if (!std.mem.eql(u8, existing.event_id, record.event_id))
                return error.IdempotencyConflict;
        }

        if (self.findByEventId(record.event_id)) |index| {
            const previous = self.entries.items[index].record;
            if (!allowed(previous.delivery_state, record.delivery_state))
                return error.InvalidTransition;
        }
    }

    /// Apply a snapshot after it has been validated. Durable callers should
    /// append+sync first, then apply; after a post-append allocation failure,
    /// stop writes and rebuild from the journal.
    pub fn apply(self: *Projection, record: journal.Record) !void {
        try self.validate(record);

        if (self.findByEventId(record.event_id)) |index| {
            const previous = self.entries.items[index].record;
            const owned = try cloneRecord(self.allocator, record);
            deinitRecord(self.allocator, previous);
            self.entries.items[index].record = owned;
        } else {
            const owned = try cloneRecord(self.allocator, record);
            self.entries.append(self.allocator, .{ .record = owned }) catch |err| {
                deinitRecord(self.allocator, owned);
                return err;
            };
        }
        self.last_sequence = record.sequence;
    }

    pub fn findByIdempotencyKey(self: *const Projection, key: []const u8) ?usize {
        for (self.entries.items, 0..) |entry, index| {
            if (std.mem.eql(u8, entry.record.idempotency_key, key)) return index;
        }
        return null;
    }

    pub fn findByEventId(self: *const Projection, id: []const u8) ?usize {
        for (self.entries.items, 0..) |entry, index| {
            if (std.mem.eql(u8, entry.record.event_id, id)) return index;
        }
        return null;
    }

    /// RECEIVED remains unsettled. An expired lease becomes claimable at its
    /// deadline; settled records remain in the index for idempotency checks.
    pub fn isClaimable(self: *const Projection, event_id: []const u8, now_ms: u64) bool {
        const index = self.findByEventId(event_id) orelse return false;
        const record = self.entries.items[index].record;
        return switch (record.delivery_state) {
            .published, .expired => true,
            .leased, .received => record.lease_deadline_ms <= now_ms,
            .settled_ok, .settled_error => false,
        };
    }

    pub fn isSettled(self: *const Projection, idempotency_key: []const u8) bool {
        const index = self.findByIdempotencyKey(idempotency_key) orelse return false;
        const state = self.entries.items[index].record.delivery_state;
        return state == .settled_ok or state == .settled_error;
    }

    fn replayVisitor(context: *anyopaque, record: journal.Record) anyerror!void {
        const self: *Projection = @ptrCast(@alignCast(context));
        try self.apply(record);
    }
};

fn allowed(from: journal.State, to: journal.State) bool {
    return switch (from) {
        .published => to == .published or to == .leased or to == .expired,
        .leased => to == .leased or to == .received or to == .settled_ok or to == .settled_error or to == .expired,
        .received => to == .received or to == .leased or to == .settled_ok or to == .settled_error or to == .expired,
        .expired => to == .expired or to == .leased,
        .settled_ok, .settled_error => to == from,
    };
}

fn cloneOptional(allocator: std.mem.Allocator, value: ?[]const u8) !?[]const u8 {
    if (value) |slice| return try allocator.dupe(u8, slice);
    return null;
}

fn cloneRecord(allocator: std.mem.Allocator, source: journal.Record) !journal.Record {
    var result: journal.Record = .{
        .sequence = source.sequence,
        .event_id = "",
        .canonical_event = "",
        .idempotency_key = "",
        .payload_reference = "",
        .payload = "",
        .delivery_state = source.delivery_state,
        .lease_owner = null,
        .lease_deadline_ms = source.lease_deadline_ms,
        .attempt = source.attempt,
        .fencing_generation = source.fencing_generation,
        .created_at_ms = source.created_at_ms,
        .updated_at_ms = source.updated_at_ms,
        .correlation_id = "",
        .causation_id = "",
    };
    errdefer deinitRecord(allocator, result);
    result.event_id = try allocator.dupe(u8, source.event_id);
    result.canonical_event = try allocator.dupe(u8, source.canonical_event);
    result.idempotency_key = try allocator.dupe(u8, source.idempotency_key);
    result.payload_reference = try allocator.dupe(u8, source.payload_reference);
    result.payload = try allocator.dupe(u8, source.payload);
    result.lease_owner = try cloneOptional(allocator, source.lease_owner);
    result.correlation_id = try allocator.dupe(u8, source.correlation_id);
    result.causation_id = try allocator.dupe(u8, source.causation_id);
    return result;
}

fn deinitRecord(allocator: std.mem.Allocator, record: journal.Record) void {
    if (record.event_id.len > 0) allocator.free(record.event_id);
    if (record.canonical_event.len > 0) allocator.free(record.canonical_event);
    if (record.idempotency_key.len > 0) allocator.free(record.idempotency_key);
    if (record.payload_reference.len > 0) allocator.free(record.payload_reference);
    if (record.payload.len > 0) allocator.free(record.payload);
    if (record.lease_owner) |owner| allocator.free(owner);
    if (record.correlation_id.len > 0) allocator.free(record.correlation_id);
    if (record.causation_id.len > 0) allocator.free(record.causation_id);
}

test "recovery projection keeps received unsettled and expires leases by deadline" {
    var projection = Projection.init(std.testing.allocator);
    defer projection.deinit();

    try projection.apply(.{
        .sequence = 1,
        .event_id = "evt-1",
        .canonical_event = "Financial.createInvoice.request",
        .idempotency_key = "idem-1",
        .delivery_state = .published,
    });
    try projection.apply(.{
        .sequence = 2,
        .event_id = "evt-1",
        .canonical_event = "Financial.createInvoice.request",
        .idempotency_key = "idem-1",
        .delivery_state = .leased,
        .lease_owner = "worker-a",
        .lease_deadline_ms = 100,
    });
    try projection.apply(.{
        .sequence = 3,
        .event_id = "evt-1",
        .canonical_event = "Financial.createInvoice.request",
        .idempotency_key = "idem-1",
        .delivery_state = .received,
        .lease_owner = "worker-a",
        .lease_deadline_ms = 100,
    });

    try std.testing.expect(!projection.isSettled("idem-1"));
    try std.testing.expect(!projection.isClaimable("evt-1", 99));
    try std.testing.expect(projection.isClaimable("evt-1", 100));

    try projection.apply(.{
        .sequence = 4,
        .event_id = "evt-1",
        .canonical_event = "Financial.createInvoice.request",
        .idempotency_key = "idem-1",
        .delivery_state = .settled_ok,
    });
    try std.testing.expect(projection.isSettled("idem-1"));
    try std.testing.expect(!projection.isClaimable("evt-1", 1000));
}

test "recovery projection rejects duplicate idempotency key for another event" {
    var projection = Projection.init(std.testing.allocator);
    defer projection.deinit();
    try projection.apply(.{
        .sequence = 1,
        .event_id = "evt-1",
        .canonical_event = "Financial.createInvoice.request",
        .idempotency_key = "shared-key",
        .delivery_state = .published,
    });
    try std.testing.expectError(error.IdempotencyConflict, projection.apply(.{
        .sequence = 2,
        .event_id = "evt-2",
        .canonical_event = "Financial.createInvoice.request",
        .idempotency_key = "shared-key",
        .delivery_state = .published,
    }));
}

test "recovery projection rejects non-monotonic sequence" {
    var projection = Projection.init(std.testing.allocator);
    defer projection.deinit();
    try projection.apply(.{
        .sequence = 4,
        .event_id = "evt-1",
        .canonical_event = "Financial.createInvoice.request",
        .idempotency_key = "idem-1",
        .delivery_state = .published,
    });
    try std.testing.expectError(error.SequenceRegression, projection.apply(.{
        .sequence = 3,
        .event_id = "evt-1",
        .canonical_event = "Financial.createInvoice.request",
        .idempotency_key = "idem-1",
        .delivery_state = .leased,
        .lease_owner = "worker-a",
    }));
}

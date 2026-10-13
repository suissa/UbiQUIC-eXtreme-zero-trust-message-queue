const std = @import("std");
const journal = @import("settlement_journal.zig");
const projection_mod = @import("settlement_projection.zig");
const jetstream_mod = @import("jetstream.zig");

/// Coordinates durable business settlement with JetStream acknowledgement.
/// The caller executes the business effect before commitSettlementAndAck and
/// must check isSettled before executing a redelivered idempotency key.
pub const Coordinator = struct {
    allocator: std.mem.Allocator,
    store: *const journal.Store,
    projection: projection_mod.Projection,
    next_sequence: u64 = 1,
    recovered: bool = false,
    poisoned: bool = false,

    pub fn init(allocator: std.mem.Allocator, store: *const journal.Store) Coordinator {
        return .{
            .allocator = allocator,
            .store = store,
            .projection = projection_mod.Projection.init(allocator),
        };
    }

    pub fn deinit(self: *Coordinator) void {
        self.projection.deinit();
        self.* = undefined;
    }

    /// Startup recovery must complete before any delivery is acknowledged or
    /// new transition is appended. Invalid journals fail closed.
    pub fn recover(self: *Coordinator) !usize {
        if (self.recovered) return error.AlreadyRecovered;
        if (self.poisoned) return error.Poisoned;
        const count = self.projection.recover(self.store) catch |err| {
            self.poisoned = true;
            return err;
        };
        self.next_sequence = std.math.add(u64, self.projection.last_sequence, 1) catch
            return error.SequenceExhausted;
        self.recovered = true;
        return count;
    }

    pub fn isSettled(self: *const Coordinator, idempotency_key: []const u8) bool {
        return self.projection.isSettled(idempotency_key);
    }

    /// Persists and syncs a complete snapshot before updating the projection.
    /// The coordinator stops accepting writes if append outcome or projection
    /// update becomes ambiguous, requiring process restart and journal replay.
    pub fn persist(self: *Coordinator, snapshot: journal.Record) !u64 {
        try self.requireReady();
        var record = snapshot;
        record.sequence = self.next_sequence;
        try self.projection.validate(record);

        self.store.append(record) catch |err| {
            self.poisoned = true;
            return err;
        };
        self.projection.apply(record) catch |err| {
            self.poisoned = true;
            return err;
        };
        self.next_sequence = std.math.add(u64, self.next_sequence, 1) catch {
            self.poisoned = true;
            return error.SequenceExhausted;
        };
        return record.sequence;
    }

    /// Call only after the Action's effect has returned. SETTLED is fsynced
    /// before +ACK is sent, so an ACK failure can be safely retried on delivery.
    pub fn commitSettlementAndAck(
        self: *Coordinator,
        snapshot: journal.Record,
        js: *jetstream_mod.JetStream,
        ack_subject: []const u8,
        protocol_buffer: []u8,
    ) !void {
        if (snapshot.delivery_state != .settled_ok and snapshot.delivery_state != .settled_error)
            return error.InvalidSettlementState;

        _ = try self.persist(snapshot);
        try js.ack(ack_subject, protocol_buffer);
    }

    /// For a redelivery after a lost ACK: if durable settlement already exists,
    /// ACK the transport copy without re-running the business effect.
    pub fn ackIfAlreadySettled(
        self: *const Coordinator,
        idempotency_key: []const u8,
        js: *jetstream_mod.JetStream,
        ack_subject: []const u8,
        protocol_buffer: []u8,
    ) !bool {
        try self.requireReady();
        if (!self.isSettled(idempotency_key)) return false;
        try js.ack(ack_subject, protocol_buffer);
        return true;
    }

    fn requireReady(self: *const Coordinator) !void {
        if (self.poisoned) return error.Poisoned;
        if (!self.recovered) return error.RecoveryRequired;
    }
};

test "durable coordinator requires replay before writes and rebuilds settled index" {
    const io = std.testing.io;
    const allocator = std.testing.allocator;
    const path = ".zig-cache/ubiq-durable-coordinator-test.jsonl";
    const cwd = std.Io.Dir.cwd();
    defer cwd.deleteFile(io, path) catch {};

    const store = journal.Store{
        .io = io,
        .allocator = allocator,
        .path = path,
    };
    {
        var first = Coordinator.init(allocator, &store);
        defer first.deinit();
        try std.testing.expectError(error.RecoveryRequired, first.persist(.{
            .sequence = 0,
            .event_id = "evt-1",
            .canonical_event = "Financial.createInvoice.request",
            .idempotency_key = "idem-1",
            .delivery_state = .published,
        }));
        try std.testing.expectEqual(@as(usize, 0), try first.recover());
        _ = try first.persist(.{
            .sequence = 0,
            .event_id = "evt-1",
            .canonical_event = "Financial.createInvoice.request",
            .idempotency_key = "idem-1",
            .delivery_state = .published,
        });
        _ = try first.persist(.{
            .sequence = 0,
            .event_id = "evt-1",
            .canonical_event = "Financial.createInvoice.request",
            .idempotency_key = "idem-1",
            .delivery_state = .settled_ok,
        });
        try std.testing.expect(first.isSettled("idem-1"));
    }

    var restarted = Coordinator.init(allocator, &store);
    defer restarted.deinit();
    try std.testing.expectEqual(@as(usize, 2), try restarted.recover());
    try std.testing.expect(restarted.isSettled("idem-1"));
    try std.testing.expectError(error.InvalidTransition, restarted.persist(.{
        .sequence = 0,
        .event_id = "evt-1",
        .canonical_event = "Financial.createInvoice.request",
        .idempotency_key = "idem-1",
        .delivery_state = .received,
        .lease_owner = "worker-a",
    }));
}

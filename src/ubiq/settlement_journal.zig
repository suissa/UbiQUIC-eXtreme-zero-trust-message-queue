const std = @import("std");

/// Append-only execution-control snapshot. Audit correlation/causation is
/// stored alongside, but remains logically separate from the mutable projection.
pub const Record = struct {
    sequence: u64,
    event_id: []const u8,
    canonical_event: []const u8,
    idempotency_key: []const u8,
    payload_reference: []const u8 = "",
    payload: []const u8 = "",
    delivery_state: State,
    lease_owner: ?[]const u8 = null,
    lease_deadline_ms: u64 = 0,
    attempt: u32 = 0,
    fencing_generation: u64 = 0,
    created_at_ms: u64 = 0,
    updated_at_ms: u64 = 0,
    correlation_id: []const u8 = "",
    causation_id: []const u8 = "",
};

pub const State = enum {
    published,
    leased,
    received,
    settled_ok,
    settled_error,
    expired,
};

pub const Error = error{
    RecordTooLarge,
    JournalFull,
    InvalidRecord,
    ChecksumMismatch,
    InvalidChecksum,
    InvalidTransition,
};

pub const ReplayFn = *const fn (context: *anyopaque, record: Record) anyerror!void;

pub const Store = struct {
    io: std.Io,
    allocator: std.mem.Allocator,
    path: []const u8,
    max_bytes: u64 = 16 * 1024 * 1024,
    max_record_bytes: usize = 1024 * 1024,

    /// Writes a complete, checksummed JSON snapshot and syncs the file before
    /// returning. An incomplete trailing line is ignored during replay.
    pub fn append(self: *const Store, record: Record) !void {
        try validateRecord(record);
        const json = try std.json.Stringify.valueAlloc(self.allocator, record, .{});
        defer self.allocator.free(json);
        if (json.len > self.max_record_bytes) return error.RecordTooLarge;

        const checksum = checksum32(json);
        const line = try std.fmt.allocPrint(self.allocator, "{x}\t{s}\n", .{ checksum, json });
        defer self.allocator.free(line);

        const cwd = std.Io.Dir.cwd();
        var file = cwd.openFile(self.io, self.path, .{ .mode = .read_write }) catch |err| switch (err) {
            error.FileNotFound => try cwd.createFile(self.io, self.path, .{}),
            else => return err,
        };
        defer file.close(self.io);

        const current_size = try file.length(self.io);
        if (current_size > self.max_bytes or line.len > self.max_bytes - current_size)
            return error.JournalFull;

        try file.writePositionalAll(self.io, line, current_size);
        try file.sync(self.io);
    }

    /// Calls visitor once for each complete, valid record in sequence. The
    /// visitor must apply records in order and must not treat RECEIVED as
    /// settlement. Caller owns the context and any projection built from it.
    pub fn replay(self: *const Store, context: *anyopaque, visitor: ReplayFn) !usize {
        const bytes = std.Io.Dir.readFileAlloc(
            std.Io.Dir.cwd(),
            self.io,
            self.path,
            self.allocator,
            .limited(self.max_bytes),
        ) catch |err| switch (err) {
            error.FileNotFound => return 0,
            else => return err,
        };
        defer self.allocator.free(bytes);

        var count: usize = 0;
        var start: usize = 0;
        while (start < bytes.len) {
            const relative_end = std.mem.indexOfScalarPos(u8, bytes, start, '\n') orelse break;
            const line = bytes[start..relative_end];
            start = relative_end + 1;
            if (line.len == 0) return error.InvalidRecord;

            const tab = std.mem.indexOfScalar(u8, line, '\t') orelse return error.InvalidRecord;
            const expected = std.fmt.parseInt(u32, line[0..tab], 16) catch return error.InvalidChecksum;
            const json = line[tab + 1 ..];
            if (json.len == 0 or json.len > self.max_record_bytes) return error.RecordTooLarge;
            if (checksum32(json) != expected) return error.ChecksumMismatch;

            const parsed = try std.json.parseFromSlice(Record, self.allocator, json, .{});
            defer parsed.deinit();
            try validateRecord(parsed.value);
            try visitor(context, parsed.value);
            count += 1;
        }
        return count;
    }
};

fn validateRecord(record: Record) Error!void {
    if (record.event_id.len == 0 or record.canonical_event.len == 0 or record.idempotency_key.len == 0)
        return error.InvalidRecord;
    if (record.delivery_state == .settled_ok or record.delivery_state == .settled_error) {
        if (record.lease_owner != null) return error.InvalidTransition;
    }
    if ((record.delivery_state == .leased or record.delivery_state == .received) and record.lease_owner == null)
        return error.InvalidRecord;
}

fn checksum32(bytes: []const u8) u32 {
    var hash: u32 = 2166136261;
    for (bytes) |byte| {
        hash ^= byte;
        hash *%= 16777619;
    }
    return hash;
}

const ReplayProjection = struct {
    count: usize = 0,
    last_sequence: u64 = 0,
    last_state: State = .published,
    saw_received: bool = false,
    saw_settled: bool = false,
};

fn applyProjection(context: *anyopaque, record: Record) anyerror!void {
    const projection: *ReplayProjection = @ptrCast(@alignCast(context));
    projection.count += 1;
    projection.last_sequence = record.sequence;
    projection.last_state = record.delivery_state;
    if (record.delivery_state == .received) projection.saw_received = true;
    if (record.delivery_state == .settled_ok or record.delivery_state == .settled_error)
        projection.saw_settled = true;
}

test "journal replays received separately from settled after reopen" {
    const io = std.testing.io;
    const allocator = std.testing.allocator;
    const path = ".zig-cache/ubiq-settlement-journal-test.jsonl";
    const cwd = std.Io.Dir.cwd();
    defer cwd.deleteFile(io, path) catch {};

    const store = Store{ .io = io, .allocator = allocator, .path = path };
    try store.append(.{
        .sequence = 1,
        .event_id = "evt-1",
        .canonical_event = "Financial.createInvoice.request",
        .idempotency_key = "idem-1",
        .delivery_state = .received,
        .lease_owner = "worker-a",
        .lease_deadline_ms = 100,
        .attempt = 1,
        .fencing_generation = 7,
        .correlation_id = "corr-1",
        .causation_id = "cause-1",
    });
    try store.append(.{
        .sequence = 2,
        .event_id = "evt-1",
        .canonical_event = "Financial.createInvoice.request",
        .idempotency_key = "idem-1",
        .delivery_state = .settled_ok,
        .lease_owner = null,
        .attempt = 1,
        .fencing_generation = 7,
        .correlation_id = "corr-1",
        .causation_id = "cause-1",
    });

    var projection = ReplayProjection{};
    const count = try store.replay(&projection, applyProjection);
    try std.testing.expectEqual(@as(usize, 2), count);
    try std.testing.expectEqual(@as(u64, 2), projection.last_sequence);
    try std.testing.expectEqual(State.settled_ok, projection.last_state);
    try std.testing.expect(projection.saw_received);
    try std.testing.expect(projection.saw_settled);
}

test "journal rejects settled snapshots that still hold a lease" {
    try std.testing.expectError(error.InvalidTransition, validateRecord(.{
        .sequence = 3,
        .event_id = "evt-2",
        .canonical_event = "Financial.createInvoice.request",
        .idempotency_key = "idem-2",
        .delivery_state = .settled_ok,
        .lease_owner = "stale-worker",
    }));
}

test "journal ignores a truncated final line during recovery" {
    const io = std.testing.io;
    const allocator = std.testing.allocator;
    const path = ".zig-cache/ubiq-settlement-truncated-test.jsonl";
    const cwd = std.Io.Dir.cwd();
    defer cwd.deleteFile(io, path) catch {};

    const file = try cwd.createFile(io, path, .{});
    defer file.close(io);
    try file.writeStreamingAll(io, "deadbeef\t{\"sequence\":");
    try file.sync(io);

    const store = Store{ .io = io, .allocator = allocator, .path = path };
    var projection = ReplayProjection{};
    const count = try store.replay(&projection, applyProjection);
    try std.testing.expectEqual(@as(usize, 0), count);
}

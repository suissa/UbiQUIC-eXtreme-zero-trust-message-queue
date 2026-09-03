const std = @import("std");
const delivery = @import("delivery.zig");
const event = @import("event.zig");

pub const record_bytes: usize = 1536;
const header_bytes: usize = 64;
const checksum_bytes: usize = 8;
const body_capacity: usize = record_bytes - header_bytes - checksum_bytes;
const magic: u32 = 0x55424353; // UBCS: UbiQ Control Store
const version: u16 = 1;

pub const max_event_id_bytes: usize = 128;
pub const max_event_name_bytes: usize = 256;
pub const max_idempotency_key_bytes: usize = 256;
pub const max_worker_bytes: usize = 128;
pub const max_correlation_bytes: usize = 192;
pub const max_causation_bytes: usize = 192;
pub const max_schema_id_bytes: usize = 256;

pub const StoreError = error{
    BufferTooSmall,
    FieldTooLarge,
    InvalidMagic,
    UnsupportedVersion,
    InvalidRecord,
    ChecksumMismatch,
    IndexFull,
    InvalidTransition,
    MissingPublishedRecord,
    TailTruncated,
};

pub const TransitionKind = enum(u8) {
    published = 0,
    leased = 1,
    received = 2,
    settled_ok = 3,
    settled_error = 4,
    expired = 5,
};

pub const Transition = struct {
    kind: TransitionKind,
    timestamp_ms: u64,
    lease_until_ms: u64 = 0,
    attempt: u32 = 0,
    fencing_token: u64 = 0,
    payload_hash: u64 = 0,
    event_id: []const u8,
    event_name: []const u8,
    idempotency_key: []const u8,
    worker: []const u8 = "",
    correlation_id: []const u8 = "",
    causation_id: []const u8 = "",
    schema_id: []const u8 = "",

    pub fn fromEnvelope(envelope: event.Envelope, timestamp_ms: u64) Transition {
        return .{
            .kind = .published,
            .timestamp_ms = timestamp_ms,
            .payload_hash = fingerprint(envelope.payload),
            .event_id = envelope.id,
            .event_name = envelope.event.name,
            .idempotency_key = envelope.idempotency_key,
            .correlation_id = envelope.correlation_id,
            .causation_id = envelope.causation_id,
            .schema_id = envelope.schema_id,
        };
    }
};

pub const DecodedRecord = struct {
    transition: Transition,
};

pub fn encode(transition: Transition, out: []u8) StoreError![]const u8 {
    if (out.len < record_bytes) return error.BufferTooSmall;
    @memset(out[0..record_bytes], 0);

    const fields = [_][]const u8{
        transition.event_id,
        transition.event_name,
        transition.idempotency_key,
        transition.worker,
        transition.correlation_id,
        transition.causation_id,
        transition.schema_id,
    };
    const maximums = [_]usize{
        max_event_id_bytes,
        max_event_name_bytes,
        max_idempotency_key_bytes,
        max_worker_bytes,
        max_correlation_bytes,
        max_causation_bytes,
        max_schema_id_bytes,
    };

    var body_len: usize = 0;
    for (fields, maximums) |field, maximum| {
        if (field.len > maximum or field.len > std.math.maxInt(u16)) return error.FieldTooLarge;
        body_len += field.len;
    }
    if (body_len > body_capacity or body_len > std.math.maxInt(u16)) return error.FieldTooLarge;

    var cursor: usize = 0;
    putInt(u32, out, &cursor, magic);
    putInt(u16, out, &cursor, version);
    out[cursor] = @intFromEnum(transition.kind);
    cursor += 1;
    out[cursor] = @intFromEnum(stateForKind(transition.kind));
    cursor += 1;
    putInt(u64, out, &cursor, transition.timestamp_ms);
    putInt(u64, out, &cursor, transition.lease_until_ms);
    putInt(u32, out, &cursor, transition.attempt);
    putInt(u32, out, &cursor, 0);
    putInt(u64, out, &cursor, transition.fencing_token);
    putInt(u64, out, &cursor, transition.payload_hash);
    inline for (fields) |field| putInt(u16, out, &cursor, @intCast(field.len));
    putInt(u16, out, &cursor, @intCast(body_len));
    if (cursor != header_bytes) return error.InvalidRecord;

    for (fields) |field| {
        @memcpy(out[cursor .. cursor + field.len], field);
        cursor += field.len;
    }

    const checksum_offset = record_bytes - checksum_bytes;
    const checksum = fingerprint(out[0..checksum_offset]);
    var checksum_cursor = checksum_offset;
    putInt(u64, out, &checksum_cursor, checksum);
    return out[0..record_bytes];
}

pub fn decode(bytes: []const u8) StoreError!DecodedRecord {
    if (bytes.len != record_bytes) return error.InvalidRecord;
    const expected_checksum = std.mem.readInt(u64, bytes[record_bytes - checksum_bytes ..][0..8], .big);
    if (fingerprint(bytes[0 .. record_bytes - checksum_bytes]) != expected_checksum) return error.ChecksumMismatch;

    var cursor: usize = 0;
    if (takeInt(u32, bytes, &cursor) != magic) return error.InvalidMagic;
    if (takeInt(u16, bytes, &cursor) != version) return error.UnsupportedVersion;

    const kind: TransitionKind = switch (bytes[cursor]) {
        0 => .published,
        1 => .leased,
        2 => .received,
        3 => .settled_ok,
        4 => .settled_error,
        5 => .expired,
        else => return error.InvalidRecord,
    };
    cursor += 1;
    const encoded_state = bytes[cursor];
    cursor += 1;
    if (encoded_state != @intFromEnum(stateForKind(kind))) return error.InvalidRecord;

    const timestamp_ms = takeInt(u64, bytes, &cursor);
    const lease_until_ms = takeInt(u64, bytes, &cursor);
    const attempt = takeInt(u32, bytes, &cursor);
    _ = takeInt(u32, bytes, &cursor);
    const fencing_token = takeInt(u64, bytes, &cursor);
    const payload_hash = takeInt(u64, bytes, &cursor);

    var lengths: [7]usize = undefined;
    for (&lengths) |*length| length.* = takeInt(u16, bytes, &cursor);
    const body_len: usize = takeInt(u16, bytes, &cursor);
    if (cursor != header_bytes) return error.InvalidRecord;

    var computed_body_len: usize = 0;
    for (lengths) |length| computed_body_len += length;
    if (computed_body_len != body_len or body_len > body_capacity) return error.InvalidRecord;
    if (header_bytes + body_len > record_bytes - checksum_bytes) return error.InvalidRecord;

    const event_id = takeField(bytes, &cursor, lengths[0]);
    const event_name = takeField(bytes, &cursor, lengths[1]);
    const idempotency_key = takeField(bytes, &cursor, lengths[2]);
    const worker = takeField(bytes, &cursor, lengths[3]);
    const correlation_id = takeField(bytes, &cursor, lengths[4]);
    const causation_id = takeField(bytes, &cursor, lengths[5]);
    const schema_id = takeField(bytes, &cursor, lengths[6]);

    if (event_id.len == 0 or event_name.len == 0 or idempotency_key.len == 0) return error.InvalidRecord;
    return .{ .transition = .{
        .kind = kind,
        .timestamp_ms = timestamp_ms,
        .lease_until_ms = lease_until_ms,
        .attempt = attempt,
        .fencing_token = fencing_token,
        .payload_hash = payload_hash,
        .event_id = event_id,
        .event_name = event_name,
        .idempotency_key = idempotency_key,
        .worker = worker,
        .correlation_id = correlation_id,
        .causation_id = causation_id,
        .schema_id = schema_id,
    } };
}

pub fn Text(comptime capacity: usize) type {
    return struct {
        bytes: [capacity]u8 = undefined,
        len: usize = 0,

        pub fn set(self: *@This(), value: []const u8) StoreError!void {
            if (value.len > capacity) return error.FieldTooLarge;
            @memcpy(self.bytes[0..value.len], value);
            self.len = value.len;
        }

        pub fn slice(self: *const @This()) []const u8 {
            return self.bytes[0..self.len];
        }
    };
}

pub const Snapshot = struct {
    state: delivery.DeliveryState = .published,
    timestamp_ms: u64 = 0,
    lease_until_ms: u64 = 0,
    attempt: u32 = 0,
    fencing_token: u64 = 0,
    payload_hash: u64 = 0,
    event_id: Text(max_event_id_bytes) = .{},
    event_name: Text(max_event_name_bytes) = .{},
    idempotency_key: Text(max_idempotency_key_bytes) = .{},
    worker: Text(max_worker_bytes) = .{},
    correlation_id: Text(max_correlation_bytes) = .{},
    causation_id: Text(max_causation_bytes) = .{},
    schema_id: Text(max_schema_id_bytes) = .{},

    pub fn settled(self: Snapshot) bool {
        return self.state == .settled_ok or self.state == .settled_error;
    }

    pub fn redeliverable(self: Snapshot) bool {
        return self.state == .published or self.state == .expired;
    }
};

pub fn Index(comptime capacity: usize) type {
    return struct {
        const Self = @This();

        entries: [capacity]Snapshot = undefined,
        len: usize = 0,
        recovered_expired: usize = 0,

        pub fn apply(self: *Self, decoded: DecodedRecord) StoreError!usize {
            const transition = decoded.transition;
            const existing = self.find(transition.idempotency_key);
            const index = existing orelse blk: {
                if (transition.kind != .published) return error.MissingPublishedRecord;
                if (self.len >= capacity) return error.IndexFull;
                const next = self.len;
                self.entries[next] = .{};
                self.len += 1;
                break :blk next;
            };

            const snapshot = &self.entries[index];
            if (existing != null and !validTransition(snapshot.state, transition.kind)) return error.InvalidTransition;

            snapshot.state = stateForKind(transition.kind);
            snapshot.timestamp_ms = transition.timestamp_ms;
            snapshot.lease_until_ms = transition.lease_until_ms;
            snapshot.attempt = transition.attempt;
            snapshot.fencing_token = transition.fencing_token;
            if (transition.payload_hash != 0) snapshot.payload_hash = transition.payload_hash;
            try snapshot.event_id.set(transition.event_id);
            try snapshot.event_name.set(transition.event_name);
            try snapshot.idempotency_key.set(transition.idempotency_key);
            try snapshot.worker.set(transition.worker);
            try snapshot.correlation_id.set(transition.correlation_id);
            try snapshot.causation_id.set(transition.causation_id);
            try snapshot.schema_id.set(transition.schema_id);
            return index;
        }

        pub fn recoverExpired(self: *Self, now_ms: u64) usize {
            var count: usize = 0;
            for (self.entries[0..self.len]) |*snapshot| {
                if ((snapshot.state == .leased or snapshot.state == .received) and
                    snapshot.lease_until_ms != 0 and snapshot.lease_until_ms <= now_ms)
                {
                    snapshot.state = .expired;
                    count += 1;
                }
            }
            self.recovered_expired += count;
            return count;
        }

        pub fn get(self: *const Self, key: []const u8) ?*const Snapshot {
            const index = self.find(key) orelse return null;
            return &self.entries[index];
        }

        pub fn isSettled(self: *const Self, key: []const u8) bool {
            const snapshot = self.get(key) orelse return false;
            return snapshot.settled();
        }

        pub fn findRedeliverable(self: *const Self) ?usize {
            for (self.entries[0..self.len], 0..) |snapshot, index| {
                if (snapshot.redeliverable()) return index;
            }
            return null;
        }

        fn find(self: *const Self, key: []const u8) ?usize {
            for (self.entries[0..self.len], 0..) |snapshot, index| {
                if (std.mem.eql(u8, snapshot.idempotency_key.slice(), key)) return index;
            }
            return null;
        }
    };
}

/// File-backed append-only journal. Every append is fsync'd before returning so
/// a `processed` transition is durable before a broker ACK is released.
pub const FileStore = struct {
    io: std.Io,
    file: std.Io.File,

    pub fn open(io: std.Io, path: []const u8) !FileStore {
        const cwd = std.Io.Dir.cwd();
        const file = cwd.openFile(io, path, .{ .mode = .read_write }) catch |err| switch (err) {
            error.FileNotFound => try cwd.createFile(io, path, .{ .read = true }),
            else => return err,
        };
        return .{ .io = io, .file = file };
    }

    pub fn close(self: *FileStore) void {
        self.file.close(self.io);
    }

    pub fn append(self: *FileStore, transition: Transition) !u64 {
        var encoded: [record_bytes]u8 = undefined;
        const bytes = try encode(transition, &encoded);
        const offset = try self.file.length(self.io);
        try self.file.writePositionalAll(self.io, bytes, offset);
        try self.file.sync(self.io);
        return offset / record_bytes;
    }

    pub fn replay(self: *FileStore, comptime capacity: usize, index: *Index(capacity), now_ms: u64) !usize {
        const length = try self.file.length(self.io);
        const complete_bytes = length - (length % record_bytes);
        var offset: u64 = 0;
        var records: usize = 0;
        var buffer: [record_bytes]u8 = undefined;
        while (offset < complete_bytes) : (offset += record_bytes) {
            const read = try self.file.readPositionalAll(self.io, &buffer, offset);
            if (read != record_bytes) return error.InvalidRecord;
            _ = try index.apply(try decode(&buffer));
            records += 1;
        }

        if (complete_bytes != length) {
            try self.file.setLength(self.io, complete_bytes);
            try self.file.sync(self.io);
        }
        _ = index.recoverExpired(now_ms);
        return records;
    }
};

pub fn fingerprint(bytes: []const u8) u64 {
    var hash: u64 = 14695981039346656037;
    for (bytes) |byte| {
        hash ^= byte;
        hash *%= 1099511628211;
    }
    return hash;
}

fn stateForKind(kind: TransitionKind) delivery.DeliveryState {
    return switch (kind) {
        .published => .published,
        .leased => .leased,
        .received => .received,
        .settled_ok => .settled_ok,
        .settled_error => .settled_error,
        .expired => .expired,
    };
}

fn validTransition(current: delivery.DeliveryState, next: TransitionKind) bool {
    return switch (next) {
        .published => false,
        .leased => current == .published or current == .expired,
        .received => current == .leased,
        .settled_ok, .settled_error => current == .leased or current == .received,
        .expired => current == .leased or current == .received,
    };
}

fn putInt(comptime T: type, out: []u8, cursor: *usize, value: T) void {
    std.mem.writeInt(T, out[cursor.*..][0..@sizeOf(T)], value, .big);
    cursor.* += @sizeOf(T);
}

fn takeInt(comptime T: type, bytes: []const u8, cursor: *usize) T {
    const value = std.mem.readInt(T, bytes[cursor.*..][0..@sizeOf(T)], .big);
    cursor.* += @sizeOf(T);
    return value;
}

fn takeField(bytes: []const u8, cursor: *usize, length: usize) []const u8 {
    const result = bytes[cursor.* .. cursor.* + length];
    cursor.* += length;
    return result;
}

fn sampleTransition(kind: TransitionKind) Transition {
    return .{
        .kind = kind,
        .timestamp_ms = 10,
        .lease_until_ms = if (kind == .leased or kind == .received) 100 else 0,
        .attempt = if (kind == .published) 0 else 1,
        .fencing_token = if (kind == .published) 0 else 7,
        .payload_hash = 123,
        .event_id = "evt-1",
        .event_name = "Financial.createInvoice.request",
        .idempotency_key = "idem-1",
        .worker = if (kind == .published) "" else "worker-a",
        .correlation_id = "corr-1",
        .causation_id = "cause-1",
        .schema_id = "Financial.createInvoice.request@1",
    };
}

test "control journal record round trips with checksum" {
    var buffer: [record_bytes]u8 = undefined;
    const bytes = try encode(sampleTransition(.received), &buffer);
    const decoded = try decode(bytes);
    try std.testing.expectEqual(TransitionKind.received, decoded.transition.kind);
    try std.testing.expectEqualStrings("idem-1", decoded.transition.idempotency_key);
    try std.testing.expectEqual(@as(u64, 7), decoded.transition.fencing_token);

    buffer[100] ^= 0x01;
    try std.testing.expectError(error.ChecksumMismatch, decode(&buffer));
}

test "replay index keeps RECEIVED separate from SETTLED and recovers expired leases" {
    var index = Index(4){};
    var buffer: [record_bytes]u8 = undefined;

    _ = try index.apply(try decode(try encode(sampleTransition(.published), &buffer)));
    _ = try index.apply(try decode(try encode(sampleTransition(.leased), &buffer)));
    _ = try index.apply(try decode(try encode(sampleTransition(.received), &buffer)));

    const received = index.get("idem-1").?;
    try std.testing.expectEqual(delivery.DeliveryState.received, received.state);
    try std.testing.expect(!received.settled());
    try std.testing.expectEqual(@as(usize, 1), index.recoverExpired(100));
    try std.testing.expectEqual(delivery.DeliveryState.expired, index.get("idem-1").?.state);
    try std.testing.expect(index.findRedeliverable() != null);
}

test "settled idempotency survives replay state reconstruction" {
    var index = Index(4){};
    var buffer: [record_bytes]u8 = undefined;
    _ = try index.apply(try decode(try encode(sampleTransition(.published), &buffer)));
    _ = try index.apply(try decode(try encode(sampleTransition(.leased), &buffer)));
    _ = try index.apply(try decode(try encode(sampleTransition(.received), &buffer)));
    _ = try index.apply(try decode(try encode(sampleTransition(.settled_ok), &buffer)));
    try std.testing.expect(index.isSettled("idem-1"));
    try std.testing.expect(index.findRedeliverable() == null);
}

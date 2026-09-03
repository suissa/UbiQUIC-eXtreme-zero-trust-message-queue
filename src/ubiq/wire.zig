const std = @import("std");
const event = @import("event.zig");

pub const magic: u32 = 0x55424951; // UBIQ
pub const version: u16 = 1;
const fixed_header_len: usize = 44;

pub const WireError = error{
    BufferTooSmall,
    InvalidMagic,
    UnsupportedVersion,
    InvalidFrame,
    InvalidState,
    FieldTooLarge,
} || event.EventNameError;

/// Binary transport representation shared by TCP, QUIC and WebSocket binary
/// frames. Decoding is zero-copy: returned slices point into the input frame.
pub fn encode(envelope: event.Envelope, out: []u8) WireError![]const u8 {
    const lengths = [_]usize{
        envelope.id.len,
        envelope.event.name.len,
        envelope.correlation_id.len,
        envelope.causation_id.len,
        envelope.idempotency_key.len,
        envelope.schema_id.len,
    };
    for (lengths) |len| if (len > std.math.maxInt(u16)) return error.FieldTooLarge;
    if (envelope.payload.len > std.math.maxInt(u32)) return error.FieldTooLarge;

    const required = fixed_header_len + envelope.id.len + envelope.event.name.len +
        envelope.correlation_id.len + envelope.causation_id.len +
        envelope.idempotency_key.len + envelope.schema_id.len + envelope.payload.len;
    if (out.len < required) return error.BufferTooSmall;

    var cursor: usize = 0;
    putInt(u32, out, &cursor, magic);
    putInt(u16, out, &cursor, version);
    out[cursor] = @intFromEnum(envelope.event.state);
    cursor += 1;
    out[cursor] = @intFromEnum(envelope.guarantee);
    cursor += 1;
    putInt(u64, out, &cursor, envelope.created_at_ms);
    inline for (lengths) |len| putInt(u16, out, &cursor, @intCast(len));
    putInt(u32, out, &cursor, @intCast(envelope.payload.len));
    putInt(u64, out, &cursor, 0); // reserved for future flags/trace extension

    copyField(out, &cursor, envelope.id);
    copyField(out, &cursor, envelope.event.name);
    copyField(out, &cursor, envelope.correlation_id);
    copyField(out, &cursor, envelope.causation_id);
    copyField(out, &cursor, envelope.idempotency_key);
    copyField(out, &cursor, envelope.schema_id);
    copyField(out, &cursor, envelope.payload);
    return out[0..cursor];
}

pub fn decode(frame: []const u8) WireError!event.Envelope {
    if (frame.len < fixed_header_len) return error.InvalidFrame;
    var cursor: usize = 0;
    if (takeInt(u32, frame, &cursor) != magic) return error.InvalidMagic;
    if (takeInt(u16, frame, &cursor) != version) return error.UnsupportedVersion;

    const encoded_state = std.meta.intToEnum(event.EventState, frame[cursor]) catch return error.InvalidState;
    cursor += 1;
    const guarantee = std.meta.intToEnum(event.DeliveryGuarantee, frame[cursor]) catch return error.InvalidFrame;
    cursor += 1;
    const created_at_ms = takeInt(u64, frame, &cursor);

    var lengths: [6]usize = undefined;
    for (&lengths) |*len| len.* = takeInt(u16, frame, &cursor);
    const payload_len: usize = takeInt(u32, frame, &cursor);
    _ = takeInt(u64, frame, &cursor); // reserved

    const total_dynamic = lengths[0] + lengths[1] + lengths[2] + lengths[3] + lengths[4] + lengths[5] + payload_len;
    if (cursor + total_dynamic != frame.len) return error.InvalidFrame;

    const id = try takeField(frame, &cursor, lengths[0]);
    const event_name = try takeField(frame, &cursor, lengths[1]);
    const correlation_id = try takeField(frame, &cursor, lengths[2]);
    const causation_id = try takeField(frame, &cursor, lengths[3]);
    const idempotency_key = try takeField(frame, &cursor, lengths[4]);
    const schema_id = try takeField(frame, &cursor, lengths[5]);
    const payload = try takeField(frame, &cursor, payload_len);

    const canonical = try event.CanonicalEvent.parse(event_name);
    if (canonical.state != encoded_state) return error.InvalidState;

    return .{
        .id = id,
        .event = canonical,
        .correlation_id = correlation_id,
        .causation_id = causation_id,
        .idempotency_key = idempotency_key,
        .schema_id = schema_id,
        .payload = payload,
        .guarantee = guarantee,
        .created_at_ms = created_at_ms,
    };
}

fn putInt(comptime T: type, out: []u8, cursor: *usize, value: T) void {
    std.mem.writeInt(T, out[cursor.*..][0..@sizeOf(T)], value, .big);
    cursor.* += @sizeOf(T);
}

fn takeInt(comptime T: type, frame: []const u8, cursor: *usize) T {
    const value = std.mem.readInt(T, frame[cursor.*..][0..@sizeOf(T)], .big);
    cursor.* += @sizeOf(T);
    return value;
}

fn copyField(out: []u8, cursor: *usize, value: []const u8) void {
    @memcpy(out[cursor.* .. cursor.* + value.len], value);
    cursor.* += value.len;
}

fn takeField(frame: []const u8, cursor: *usize, len: usize) WireError![]const u8 {
    if (cursor.* + len > frame.len) return error.InvalidFrame;
    const value = frame[cursor.* .. cursor.* + len];
    cursor.* += len;
    return value;
}

fn sample() !event.Envelope {
    return .{
        .id = "evt-42",
        .event = try event.CanonicalEvent.parse("Financial.createInvoice.ok"),
        .correlation_id = "corr-42",
        .causation_id = "cause-41",
        .idempotency_key = "idem-42",
        .schema_id = "Financial.createInvoice.ok@1",
        .payload = "{\"invoice_id\":42}",
        .guarantee = .processed,
        .created_at_ms = 123456,
    };
}

test "binary wire envelope round trips canonical identity and payload" {
    var buffer: [1024]u8 = undefined;
    const original = try sample();
    const bytes = try encode(original, &buffer);
    const restored = try decode(bytes);

    try std.testing.expectEqualStrings(original.event.name, restored.event.name);
    try std.testing.expectEqual(original.event.state, restored.event.state);
    try std.testing.expectEqual(original.guarantee, restored.guarantee);
    try std.testing.expectEqualStrings(original.idempotency_key, restored.idempotency_key);
    try std.testing.expectEqualStrings(original.payload, restored.payload);
}

test "wire decoder rejects state/name disagreement" {
    var buffer: [1024]u8 = undefined;
    const bytes = try encode(try sample(), &buffer);
    buffer[6] = @intFromEnum(event.EventState.@"error");
    try std.testing.expectError(error.InvalidState, decode(bytes));
}

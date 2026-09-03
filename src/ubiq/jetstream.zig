const std = @import("std");
const event = @import("event.zig");
const wire = @import("wire.zig");
const nats = @import("nats_client.zig");

pub const JetStreamError = error{
    InvalidStreamName,
    InvalidSubjectFilter,
    ApiError,
    InvalidPubAck,
    WrongStream,
} || nats.ClientError;

pub const PubAck = struct {
    stream: []const u8,
    sequence: u64,
    duplicate: bool,
};

/// JetStream is a durable backbone adapter. A successful PubAck means the
/// server persisted the publication; it never means the business Action ran.
pub const JetStream = struct {
    client: *nats.Client,
    stream_name: []const u8,

    pub fn init(client: *nats.Client, stream_name: []const u8) JetStreamError!JetStream {
        try validateStreamName(stream_name);
        return .{ .client = client, .stream_name = stream_name };
    }

    /// Creates/updates the reference stream through the official JetStream API
    /// subject. Intended for bootstrap/tests; production may provision streams
    /// independently while using the same binding for data traffic.
    pub fn createStream(
        self: *JetStream,
        subject_filter: []const u8,
        inbox: []const u8,
        sid: []const u8,
        response_buffer: []u8,
        subject_buffer: []u8,
        reply_buffer: []u8,
        protocol_buffer: []u8,
    ) JetStreamError!void {
        try validateSubjectFilter(subject_filter);
        var api_subject_buffer: [256]u8 = undefined;
        const api_subject = std.fmt.bufPrint(&api_subject_buffer, "$JS.API.STREAM.CREATE.{s}", .{self.stream_name}) catch return error.InvalidStreamName;
        var json_buffer: [512]u8 = undefined;
        const body = std.fmt.bufPrint(
            &json_buffer,
            "{{\"name\":\"{s}\",\"subjects\":[\"{s}\"],\"storage\":\"file\",\"num_replicas\":1}}",
            .{ self.stream_name, subject_filter },
        ) catch return error.InvalidSubjectFilter;

        try self.client.subscribe(inbox, sid, protocol_buffer);
        try self.client.unsubscribeAfter(sid, 1, protocol_buffer);
        try self.client.publishRaw(api_subject, inbox, body, protocol_buffer);
        const response = try self.client.nextRaw(response_buffer, subject_buffer, reply_buffer, protocol_buffer);
        if (std.mem.indexOf(u8, response.payload, "\"error\"") != null) return error.ApiError;
        if (std.mem.indexOf(u8, response.payload, "\"stream_create_response\"") == null) return error.ApiError;
    }

    pub fn publish(
        self: *JetStream,
        envelope: event.Envelope,
        inbox: []const u8,
        sid: []const u8,
        wire_buffer: []u8,
        response_buffer: []u8,
        subject_buffer: []u8,
        reply_buffer: []u8,
        protocol_buffer: []u8,
    ) JetStreamError!PubAck {
        const encoded = try wire.encode(envelope, wire_buffer);
        var header_buffer: [1024]u8 = undefined;
        const headers = std.fmt.bufPrint(
            &header_buffer,
            "Nats-Msg-Id: {s}\r\nNats-Expected-Stream: {s}",
            .{ envelope.idempotency_key, self.stream_name },
        ) catch return error.InvalidPubAck;

        try self.client.subscribe(inbox, sid, protocol_buffer);
        try self.client.unsubscribeAfter(sid, 1, protocol_buffer);
        try self.client.publishHeaders(envelope.event.name, inbox, headers, encoded, protocol_buffer);

        const response = try self.client.nextRaw(response_buffer, subject_buffer, reply_buffer, protocol_buffer);
        return parsePubAck(response.payload, self.stream_name);
    }
};

pub fn parsePubAck(payload: []const u8, expected_stream: []const u8) JetStreamError!PubAck {
    if (std.mem.indexOf(u8, payload, "\"error\"") != null) return error.ApiError;
    const stream = extractJsonString(payload, "\"stream\":\"") orelse return error.InvalidPubAck;
    if (!std.mem.eql(u8, stream, expected_stream)) return error.WrongStream;
    const sequence = extractJsonU64(payload, "\"seq\":") orelse return error.InvalidPubAck;
    const duplicate = extractJsonBool(payload, "\"duplicate\":") orelse false;
    return .{ .stream = stream, .sequence = sequence, .duplicate = duplicate };
}

fn validateStreamName(name: []const u8) JetStreamError!void {
    if (name.len == 0 or name.len > 128) return error.InvalidStreamName;
    for (name) |byte| {
        if (!(std.ascii.isAlphanumeric(byte) or byte == '_' or byte == '-')) return error.InvalidStreamName;
    }
}

fn validateSubjectFilter(subject: []const u8) JetStreamError!void {
    if (subject.len == 0 or std.mem.indexOfAny(u8, subject, "\"\\\r\n") != null) return error.InvalidSubjectFilter;
}

fn extractJsonString(payload: []const u8, prefix: []const u8) ?[]const u8 {
    const start_index = std.mem.indexOf(u8, payload, prefix) orelse return null;
    const start = start_index + prefix.len;
    const tail = payload[start..];
    const end = std.mem.indexOfScalar(u8, tail, '"') orelse return null;
    return tail[0..end];
}

fn extractJsonU64(payload: []const u8, prefix: []const u8) ?u64 {
    const start_index = std.mem.indexOf(u8, payload, prefix) orelse return null;
    var cursor = start_index + prefix.len;
    while (cursor < payload.len and payload[cursor] == ' ') cursor += 1;
    const start = cursor;
    while (cursor < payload.len and std.ascii.isDigit(payload[cursor])) cursor += 1;
    if (cursor == start) return null;
    return std.fmt.parseInt(u64, payload[start..cursor], 10) catch null;
}

fn extractJsonBool(payload: []const u8, prefix: []const u8) ?bool {
    const start_index = std.mem.indexOf(u8, payload, prefix) orelse return null;
    var cursor = start_index + prefix.len;
    while (cursor < payload.len and payload[cursor] == ' ') cursor += 1;
    if (std.mem.startsWith(u8, payload[cursor..], "true")) return true;
    if (std.mem.startsWith(u8, payload[cursor..], "false")) return false;
    return null;
}

test "JetStream PubAck is parsed without promoting it to execution settlement" {
    const ack = try parsePubAck("{\"stream\":\"UBIQ_EVENTS\",\"seq\":42,\"duplicate\":false}", "UBIQ_EVENTS");
    try std.testing.expectEqualStrings("UBIQ_EVENTS", ack.stream);
    try std.testing.expectEqual(@as(u64, 42), ack.sequence);
    try std.testing.expect(!ack.duplicate);
}

test "JetStream duplicate PubAck is visible to idempotency layer" {
    const ack = try parsePubAck("{\"stream\":\"UBIQ_EVENTS\",\"seq\":42,\"duplicate\":true}", "UBIQ_EVENTS");
    try std.testing.expect(ack.duplicate);
}

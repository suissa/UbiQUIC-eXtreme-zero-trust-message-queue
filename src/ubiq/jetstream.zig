const std = @import("std");
const event = @import("event.zig");
const wire = @import("wire.zig");
const nats = @import("nats_client.zig");

const stream_create_response_type = "io.nats.jetstream.api.v1.stream_create_response";
const consumer_create_response_type = "io.nats.jetstream.api.v1.consumer_create_response";
const ack_prefix = "$JS.ACK.";

pub const JetStreamError = error{
    InvalidStreamName,
    InvalidConsumerName,
    InvalidSubjectFilter,
    InvalidAckSubject,
    ApiError,
    InvalidApiResponse,
    InvalidPubAck,
    InvalidPullDelivery,
    WrongStream,
} || nats.ClientError;

pub const PubAck = struct {
    stream: []const u8,
    sequence: u64,
    duplicate: bool,
};

pub const PullDelivery = struct {
    envelope: event.Envelope,
    ack_subject: []const u8,
};

/// JetStream is a durable backbone adapter. A successful PubAck means the
/// server persisted the publication; it never means the business Action ran.
pub const JetStream = struct {
    client: *nats.Client,
    stream_name: []const u8,

    pub fn init(client: *nats.Client, stream_name: []const u8) JetStreamError!JetStream {
        try validateAssetName(stream_name, error.InvalidStreamName);
        return .{ .client = client, .stream_name = stream_name };
    }

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

        try self.requestApi(api_subject, inbox, sid, body, response_buffer, subject_buffer, reply_buffer, protocol_buffer, stream_create_response_type);
    }

    pub fn createDurablePullConsumer(
        self: *JetStream,
        consumer_name: []const u8,
        subject_filter: []const u8,
        ack_wait_ns: u64,
        max_deliver: u32,
        inbox: []const u8,
        sid: []const u8,
        response_buffer: []u8,
        subject_buffer: []u8,
        reply_buffer: []u8,
        protocol_buffer: []u8,
    ) JetStreamError!void {
        try validateAssetName(consumer_name, error.InvalidConsumerName);
        try validateSubjectFilter(subject_filter);

        var api_subject_buffer: [384]u8 = undefined;
        const api_subject = std.fmt.bufPrint(
            &api_subject_buffer,
            "$JS.API.CONSUMER.DURABLE.CREATE.{s}.{s}",
            .{ self.stream_name, consumer_name },
        ) catch return error.InvalidConsumerName;

        var json_buffer: [1024]u8 = undefined;
        const body = std.fmt.bufPrint(
            &json_buffer,
            "{{\"stream_name\":\"{s}\",\"config\":{{\"durable_name\":\"{s}\",\"name\":\"{s}\",\"deliver_policy\":\"all\",\"ack_policy\":\"explicit\",\"ack_wait\":{d},\"max_deliver\":{d},\"filter_subject\":\"{s}\",\"replay_policy\":\"instant\",\"max_ack_pending\":1}}}}",
            .{ self.stream_name, consumer_name, consumer_name, ack_wait_ns, max_deliver, subject_filter },
        ) catch return error.InvalidConsumerName;

        try self.requestApi(api_subject, inbox, sid, body, response_buffer, subject_buffer, reply_buffer, protocol_buffer, consumer_create_response_type);
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

    pub fn pullOne(
        self: *JetStream,
        consumer_name: []const u8,
        expires_ns: u64,
        inbox: []const u8,
        sid: []const u8,
        payload_buffer: []u8,
        subject_buffer: []u8,
        ack_subject_buffer: []u8,
        protocol_buffer: []u8,
    ) JetStreamError!PullDelivery {
        try validateAssetName(consumer_name, error.InvalidConsumerName);
        var api_subject_buffer: [384]u8 = undefined;
        const api_subject = std.fmt.bufPrint(
            &api_subject_buffer,
            "$JS.API.CONSUMER.MSG.NEXT.{s}.{s}",
            .{ self.stream_name, consumer_name },
        ) catch return error.InvalidConsumerName;
        var request_buffer: [128]u8 = undefined;
        const body = std.fmt.bufPrint(&request_buffer, "{{\"batch\":1,\"expires\":{d}}}", .{expires_ns}) catch return error.InvalidPullDelivery;

        try self.client.subscribe(inbox, sid, protocol_buffer);
        try self.client.unsubscribeAfter(sid, 1, protocol_buffer);
        try self.client.publishRaw(api_subject, inbox, body, protocol_buffer);
        const raw = try self.client.nextRaw(payload_buffer, subject_buffer, ack_subject_buffer, protocol_buffer);
        if (!std.mem.startsWith(u8, raw.reply_to, ack_prefix)) return error.InvalidAckSubject;
        return .{ .envelope = try wire.decode(raw.payload), .ack_subject = raw.reply_to };
    }

    /// JetStream ACK only releases the backbone delivery. It is deliberately
    /// separate from the UbiQ worker's `SETTLED_OK | SETTLED_ERROR` frame.
    pub fn ack(self: *JetStream, ack_subject: []const u8, protocol_buffer: []u8) JetStreamError!void {
        if (!std.mem.startsWith(u8, ack_subject, ack_prefix)) return error.InvalidAckSubject;
        try self.client.publishRaw(ack_subject, null, "+ACK", protocol_buffer);
    }

    /// Work-in-progress extends JetStream's ack window without claiming that
    /// the UbiQ Action has completed.
    pub fn working(self: *JetStream, ack_subject: []const u8, protocol_buffer: []u8) JetStreamError!void {
        if (!std.mem.startsWith(u8, ack_subject, ack_prefix)) return error.InvalidAckSubject;
        try self.client.publishRaw(ack_subject, null, "+WPI", protocol_buffer);
    }

    fn requestApi(
        self: *JetStream,
        api_subject: []const u8,
        inbox: []const u8,
        sid: []const u8,
        body: []const u8,
        response_buffer: []u8,
        subject_buffer: []u8,
        reply_buffer: []u8,
        protocol_buffer: []u8,
        expected_response_type: []const u8,
    ) JetStreamError!void {
        try self.client.subscribe(inbox, sid, protocol_buffer);
        try self.client.unsubscribeAfter(sid, 1, protocol_buffer);
        try self.client.publishRaw(api_subject, inbox, body, protocol_buffer);
        const response = try self.client.nextRaw(response_buffer, subject_buffer, reply_buffer, protocol_buffer);
        try validateApiResponseType(response.payload, expected_response_type);
    }
};

pub fn parsePubAck(payload: []const u8, expected_stream: []const u8) JetStreamError!PubAck {
    if (hasApiError(payload)) return error.ApiError;
    const stream = extractJsonString(payload, "\"stream\":\"") orelse return error.InvalidPubAck;
    if (!std.mem.eql(u8, stream, expected_stream)) return error.WrongStream;
    const sequence = extractJsonU64(payload, "\"seq\":") orelse return error.InvalidPubAck;
    const duplicate = extractJsonBool(payload, "\"duplicate\":") orelse false;
    return .{ .stream = stream, .sequence = sequence, .duplicate = duplicate };
}

fn validateApiResponseType(payload: []const u8, expected_type: []const u8) JetStreamError!void {
    if (hasApiError(payload)) return error.ApiError;
    const response_type = extractJsonString(payload, "\"type\":\"") orelse return error.InvalidApiResponse;
    if (!std.mem.eql(u8, response_type, expected_type)) return error.InvalidApiResponse;
}

fn hasApiError(payload: []const u8) bool {
    return std.mem.indexOf(u8, payload, "\"error\":") != null;
}

fn validateAssetName(name: []const u8, comptime invalid_error: JetStreamError) JetStreamError!void {
    if (name.len == 0 or name.len > 128) return invalid_error;
    for (name) |byte| {
        if (!(std.ascii.isAlphanumeric(byte) or byte == '_' or byte == '-')) return invalid_error;
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

test "JetStream API response type is validated exactly" {
    try validateApiResponseType(
        "{\"type\":\"io.nats.jetstream.api.v1.stream_create_response\",\"did_create\":true}",
        stream_create_response_type,
    );
    try std.testing.expectError(
        error.InvalidApiResponse,
        validateApiResponseType("{\"type\":\"io.nats.jetstream.api.v1.stream_info_response\"}", stream_create_response_type),
    );
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

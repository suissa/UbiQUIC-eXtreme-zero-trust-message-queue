const std = @import("std");
const event = @import("event.zig");
const wire = @import("wire.zig");
const protocol = @import("nats_protocol.zig");

pub const ClientError = error{
    ExpectedInfo,
    ExpectedPong,
    ServerError,
    PayloadTooLarge,
    ReplySubjectTooLarge,
    SubjectTooLarge,
    InvalidTrailer,
} || protocol.ProtocolError || wire.WireError || std.Io.Reader.Error || std.Io.Writer.Error;

pub const RawMessage = struct {
    subject: []const u8,
    reply_to: []const u8,
    payload: []const u8,
};

/// Allocation-free NATS Core protocol client. Network ownership stays outside
/// this type: callers provide stable Zig 0.16 `std.Io.Reader/Writer` pointers.
pub const Client = struct {
    reader: *std.Io.Reader,
    writer: *std.Io.Writer,

    pub fn init(reader: *std.Io.Reader, writer: *std.Io.Writer) Client {
        return .{ .reader = reader, .writer = writer };
    }

    pub fn handshake(self: *Client, client_name: []const u8, scratch: []u8) ClientError!void {
        const first = try self.reader.takeDelimiterInclusive('\n');
        const info = try protocol.parseControlLine(first);
        if (info.kind != .info) return error.ExpectedInfo;

        const connect_frame = try protocol.encodeConnect(scratch, client_name);
        try self.writer.writeAll(connect_frame);
        const ping = try protocol.encodePing(scratch);
        try self.writer.writeAll(ping);
        try self.writer.flush();

        while (true) {
            const line = try self.reader.takeDelimiterInclusive('\n');
            const operation = try protocol.parseControlLine(line);
            switch (operation.kind) {
                .pong => return,
                .ping => try self.sendPong(scratch),
                .err => return error.ServerError,
                .info, .ok => {},
                else => return error.ExpectedPong,
            }
        }
    }

    pub fn subscribe(self: *Client, subject: []const u8, sid: []const u8, scratch: []u8) ClientError!void {
        const frame = try protocol.encodeSub(scratch, subject, sid);
        try self.writer.writeAll(frame);
        try self.writer.flush();
    }

    pub fn unsubscribeAfter(self: *Client, sid: []const u8, max_messages: u32, scratch: []u8) ClientError!void {
        const frame = try protocol.encodeUnsub(scratch, sid, max_messages);
        try self.writer.writeAll(frame);
        try self.writer.flush();
    }

    pub fn publishRaw(
        self: *Client,
        subject: []const u8,
        reply_to: ?[]const u8,
        payload: []const u8,
        scratch: []u8,
    ) ClientError!void {
        const frame = try protocol.encodePub(scratch, subject, reply_to, payload);
        try self.writer.writeAll(frame);
        try self.writer.flush();
    }

    pub fn publishHeaders(
        self: *Client,
        subject: []const u8,
        reply_to: ?[]const u8,
        header_lines: []const u8,
        payload: []const u8,
        scratch: []u8,
    ) ClientError!void {
        const frame = try protocol.encodeHpub(scratch, subject, reply_to, header_lines, payload);
        try self.writer.writeAll(frame);
        try self.writer.flush();
    }

    pub fn publishEnvelope(
        self: *Client,
        envelope: event.Envelope,
        wire_buffer: []u8,
        protocol_buffer: []u8,
    ) ClientError!void {
        const payload = try wire.encode(envelope, wire_buffer);
        try self.publishRaw(envelope.event.name, null, payload, protocol_buffer);
    }

    pub fn nextEnvelope(
        self: *Client,
        payload_buffer: []u8,
        subject_buffer: []u8,
        reply_buffer: []u8,
        scratch: []u8,
    ) ClientError!event.Envelope {
        const raw = try self.nextRaw(payload_buffer, subject_buffer, reply_buffer, scratch);
        const envelope = try wire.decode(raw.payload);
        if (!std.mem.eql(u8, raw.subject, envelope.event.name)) return error.InvalidFrame;
        return envelope;
    }

    pub fn nextRaw(
        self: *Client,
        payload_buffer: []u8,
        subject_buffer: []u8,
        reply_buffer: []u8,
        scratch: []u8,
    ) ClientError!RawMessage {
        while (true) {
            const line = try self.reader.takeDelimiterInclusive('\n');
            const operation = try protocol.parseControlLine(line);
            switch (operation.kind) {
                .ping => {
                    try self.sendPong(scratch);
                    continue;
                },
                .info, .ok, .pong => continue,
                .err => return error.ServerError,
                .msg => return self.readMsg(operation, payload_buffer, subject_buffer, reply_buffer),
                .hmsg => return self.readHmsg(operation, payload_buffer, subject_buffer, reply_buffer),
            }
        }
    }

    fn readMsg(
        self: *Client,
        operation: protocol.ServerMessage,
        payload_buffer: []u8,
        subject_buffer: []u8,
        reply_buffer: []u8,
    ) ClientError!RawMessage {
        const payload_len = try protocol.payloadLength(operation);
        if (payload_len > payload_buffer.len) return error.PayloadTooLarge;
        const subject = try copySubject(subject_buffer, operation.subject);
        const reply = try copyReply(reply_buffer, operation.reply_to);
        try self.reader.readSliceAll(payload_buffer[0..payload_len]);
        try consumeCrlf(self.reader);
        return .{ .subject = subject, .reply_to = reply, .payload = payload_buffer[0..payload_len] };
    }

    fn readHmsg(
        self: *Client,
        operation: protocol.ServerMessage,
        payload_buffer: []u8,
        subject_buffer: []u8,
        reply_buffer: []u8,
    ) ClientError!RawMessage {
        const lengths = try protocol.hmsgLengths(operation);
        if (lengths.total < lengths.header) return error.InvalidMessage;
        const payload_len = lengths.total - lengths.header;
        if (lengths.total > payload_buffer.len) return error.PayloadTooLarge;
        const subject = try copySubject(subject_buffer, operation.subject);
        const reply = try copyReply(reply_buffer, operation.reply_to);
        try self.reader.readSliceAll(payload_buffer[0..lengths.total]);
        try consumeCrlf(self.reader);
        return .{
            .subject = subject,
            .reply_to = reply,
            .payload = payload_buffer[lengths.header .. lengths.header + payload_len],
        };
    }

    fn sendPong(self: *Client, scratch: []u8) ClientError!void {
        const pong = try protocol.encodePong(scratch);
        try self.writer.writeAll(pong);
        try self.writer.flush();
    }
};

fn consumeCrlf(reader: *std.Io.Reader) ClientError!void {
    var trailer: [2]u8 = undefined;
    try reader.readSliceAll(&trailer);
    if (!std.mem.eql(u8, &trailer, "\r\n")) return error.InvalidTrailer;
}

fn copySubject(out: []u8, value: []const u8) ClientError![]const u8 {
    if (value.len > out.len) return error.SubjectTooLarge;
    @memcpy(out[0..value.len], value);
    return out[0..value.len];
}

fn copyReply(out: []u8, value: []const u8) ClientError![]const u8 {
    if (value.len > out.len) return error.ReplySubjectTooLarge;
    @memcpy(out[0..value.len], value);
    return out[0..value.len];
}

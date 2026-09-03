const std = @import("std");

pub const ProtocolError = error{
    BufferTooSmall,
    InvalidControlLine,
    InvalidMessage,
    InvalidLength,
    InvalidSubject,
};

pub const ServerKind = enum {
    info,
    ok,
    err,
    ping,
    pong,
    msg,
    hmsg,
};

pub const ServerMessage = struct {
    kind: ServerKind,
    subject: []const u8 = "",
    sid: []const u8 = "",
    reply_to: []const u8 = "",
    headers: []const u8 = "",
    payload: []const u8 = "",
    info_or_error: []const u8 = "",
};

pub fn validateSubject(subject: []const u8) ProtocolError!void {
    if (subject.len == 0) return error.InvalidSubject;
    for (subject) |byte| {
        if (byte <= ' ' or byte == 0x7f) return error.InvalidSubject;
    }
}

pub fn encodeConnect(out: []u8, name: []const u8) ProtocolError![]const u8 {
    return std.fmt.bufPrint(
        out,
        "CONNECT {{\"verbose\":false,\"pedantic\":true,\"tls_required\":false,\"name\":\"{s}\",\"lang\":\"zig\",\"version\":\"0.1.0\",\"protocol\":1,\"headers\":true}}\r\n",
        .{name},
    ) catch error.BufferTooSmall;
}

pub fn encodePing(out: []u8) ProtocolError![]const u8 {
    if (out.len < 6) return error.BufferTooSmall;
    @memcpy(out[0..6], "PING\r\n");
    return out[0..6];
}

pub fn encodePong(out: []u8) ProtocolError![]const u8 {
    if (out.len < 6) return error.BufferTooSmall;
    @memcpy(out[0..6], "PONG\r\n");
    return out[0..6];
}

pub fn encodeSub(out: []u8, subject: []const u8, sid: []const u8) ProtocolError![]const u8 {
    try validateSubject(subject);
    return std.fmt.bufPrint(out, "SUB {s} {s}\r\n", .{ subject, sid }) catch error.BufferTooSmall;
}

pub fn encodeUnsub(out: []u8, sid: []const u8, max_messages: ?u32) ProtocolError![]const u8 {
    if (max_messages) |max| {
        return std.fmt.bufPrint(out, "UNSUB {s} {d}\r\n", .{ sid, max }) catch error.BufferTooSmall;
    }
    return std.fmt.bufPrint(out, "UNSUB {s}\r\n", .{sid}) catch error.BufferTooSmall;
}

pub fn encodePub(
    out: []u8,
    subject: []const u8,
    reply_to: ?[]const u8,
    payload: []const u8,
) ProtocolError![]const u8 {
    try validateSubject(subject);
    var cursor: usize = 0;
    const control = if (reply_to) |reply|
        std.fmt.bufPrint(out, "PUB {s} {s} {d}\r\n", .{ subject, reply, payload.len }) catch return error.BufferTooSmall
    else
        std.fmt.bufPrint(out, "PUB {s} {d}\r\n", .{ subject, payload.len }) catch return error.BufferTooSmall;
    cursor = control.len;
    if (out.len < cursor + payload.len + 2) return error.BufferTooSmall;
    @memcpy(out[cursor .. cursor + payload.len], payload);
    cursor += payload.len;
    @memcpy(out[cursor .. cursor + 2], "\r\n");
    return out[0 .. cursor + 2];
}

/// HPUB encoder used by JetStream. The caller supplies complete NATS header
/// lines without the `NATS/1.0` prelude or final blank line.
pub fn encodeHpub(
    out: []u8,
    subject: []const u8,
    reply_to: ?[]const u8,
    header_lines: []const u8,
    payload: []const u8,
) ProtocolError![]const u8 {
    try validateSubject(subject);

    var header_block: [2048]u8 = undefined;
    const headers = std.fmt.bufPrint(&header_block, "NATS/1.0\r\n{s}\r\n", .{header_lines}) catch return error.BufferTooSmall;
    const total_len = headers.len + payload.len;

    const control = if (reply_to) |reply|
        std.fmt.bufPrint(out, "HPUB {s} {s} {d} {d}\r\n", .{ subject, reply, headers.len, total_len }) catch return error.BufferTooSmall
    else
        std.fmt.bufPrint(out, "HPUB {s} {d} {d}\r\n", .{ subject, headers.len, total_len }) catch return error.BufferTooSmall;

    var cursor = control.len;
    if (out.len < cursor + total_len + 2) return error.BufferTooSmall;
    @memcpy(out[cursor .. cursor + headers.len], headers);
    cursor += headers.len;
    @memcpy(out[cursor .. cursor + payload.len], payload);
    cursor += payload.len;
    @memcpy(out[cursor .. cursor + 2], "\r\n");
    return out[0 .. cursor + 2];
}

pub fn parseControlLine(line_with_optional_crlf: []const u8) ProtocolError!ServerMessage {
    const line = std.mem.trimEnd(u8, line_with_optional_crlf, "\r\n");
    if (std.mem.eql(u8, line, "+OK")) return .{ .kind = .ok };
    if (std.mem.eql(u8, line, "PING")) return .{ .kind = .ping };
    if (std.mem.eql(u8, line, "PONG")) return .{ .kind = .pong };
    if (std.mem.startsWith(u8, line, "INFO ")) return .{ .kind = .info, .info_or_error = line[5..] };
    if (std.mem.startsWith(u8, line, "-ERR ")) return .{ .kind = .err, .info_or_error = line[5..] };

    if (std.mem.startsWith(u8, line, "MSG ")) return parseMsgLine(line[4..]);
    if (std.mem.startsWith(u8, line, "HMSG ")) return parseHmsgLine(line[5..]);
    return error.InvalidControlLine;
}

fn parseMsgLine(fields: []const u8) ProtocolError!ServerMessage {
    var tokens: [4][]const u8 = undefined;
    const count = splitFields(fields, &tokens);
    if (count != 3 and count != 4) return error.InvalidMessage;
    _ = std.fmt.parseInt(usize, tokens[count - 1], 10) catch return error.InvalidLength;
    return .{
        .kind = .msg,
        .subject = tokens[0],
        .sid = tokens[1],
        .reply_to = if (count == 4) tokens[2] else "",
        .info_or_error = tokens[count - 1], // temporarily carries payload length
    };
}

fn parseHmsgLine(fields: []const u8) ProtocolError!ServerMessage {
    var tokens: [5][]const u8 = undefined;
    const count = splitFields(fields, &tokens);
    if (count != 4 and count != 5) return error.InvalidMessage;
    _ = std.fmt.parseInt(usize, tokens[count - 2], 10) catch return error.InvalidLength;
    _ = std.fmt.parseInt(usize, tokens[count - 1], 10) catch return error.InvalidLength;
    return .{
        .kind = .hmsg,
        .subject = tokens[0],
        .sid = tokens[1],
        .reply_to = if (count == 5) tokens[2] else "",
        .headers = tokens[count - 2], // temporarily carries header length
        .info_or_error = tokens[count - 1], // temporarily carries total length
    };
}

pub fn payloadLength(message: ServerMessage) ProtocolError!usize {
    if (message.kind != .msg) return error.InvalidMessage;
    return std.fmt.parseInt(usize, message.info_or_error, 10) catch error.InvalidLength;
}

pub const HmsgLengths = struct { header: usize, total: usize };

pub fn hmsgLengths(message: ServerMessage) ProtocolError!HmsgLengths {
    if (message.kind != .hmsg) return error.InvalidMessage;
    return .{
        .header = std.fmt.parseInt(usize, message.headers, 10) catch return error.InvalidLength,
        .total = std.fmt.parseInt(usize, message.info_or_error, 10) catch return error.InvalidLength,
    };
}

fn splitFields(input: []const u8, out: [][]const u8) usize {
    var count: usize = 0;
    var iterator = std.mem.tokenizeScalar(u8, input, ' ');
    while (iterator.next()) |token| {
        if (count >= out.len) return out.len + 1;
        out[count] = token;
        count += 1;
    }
    return count;
}

test "NATS PUB and SUB preserve canonical dotted subject" {
    var buffer: [512]u8 = undefined;
    const sub = try encodeSub(&buffer, "Financial.createInvoice.ok", "42");
    try std.testing.expectEqualStrings("SUB Financial.createInvoice.ok 42\r\n", sub);

    const pub_frame = try encodePub(&buffer, "Financial.createInvoice.ok", null, "abc");
    try std.testing.expectEqualStrings("PUB Financial.createInvoice.ok 3\r\nabc\r\n", pub_frame);
}

test "JetStream HPUB carries idempotency and expected stream headers" {
    var buffer: [1024]u8 = undefined;
    const headers = "Nats-Msg-Id: idem-42\r\nNats-Expected-Stream: UBIQ_EVENTS";
    const frame = try encodeHpub(&buffer, "Financial.createInvoice.ok", "_INBOX.42", headers, "payload");
    try std.testing.expect(std.mem.startsWith(u8, frame, "HPUB Financial.createInvoice.ok _INBOX.42 "));
    try std.testing.expect(std.mem.indexOf(u8, frame, "Nats-Msg-Id: idem-42") != null);
}

test "server MSG parser extracts reply subject and payload length" {
    const parsed = try parseControlLine("MSG Financial.createInvoice.ok 7 _INBOX.A 123\r\n");
    try std.testing.expectEqual(ServerKind.msg, parsed.kind);
    try std.testing.expectEqualStrings("Financial.createInvoice.ok", parsed.subject);
    try std.testing.expectEqualStrings("_INBOX.A", parsed.reply_to);
    try std.testing.expectEqual(@as(usize, 123), try payloadLength(parsed));
}

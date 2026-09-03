const std = @import("std");
const event = @import("event.zig");
const wire = @import("wire.zig");

pub const WebSocketBindingError = error{
    NotWebSocketUpgrade,
    MissingWebSocketKey,
    UnexpectedFrameType,
} || wire.WireError || std.http.Server.Request.ExpectContinueError || std.http.Server.WebSocket.ReadSmallTextMessageError || std.Io.Writer.Error;

pub fn upgrade(request: *std.http.Server.Request) WebSocketBindingError!std.http.Server.WebSocket {
    return switch (request.upgradeRequested()) {
        .websocket => |maybe_key| request.respondWebSocket(.{ .key = maybe_key orelse return error.MissingWebSocketKey }),
        else => error.NotWebSocketUpgrade,
    };
}

/// Sends the same canonical UbiQ binary envelope used by TCP/QUIC. WebSocket
/// is therefore only framing; it does not create a second event dialect.
pub fn sendEnvelope(ws: *std.http.Server.WebSocket, envelope: event.Envelope, frame_buffer: []u8) WebSocketBindingError!void {
    const frame = try wire.encode(envelope, frame_buffer);
    try ws.writeMessage(frame, .binary);
}

pub fn receiveEnvelope(ws: *std.http.Server.WebSocket) WebSocketBindingError!event.Envelope {
    const message = try ws.readSmallMessage();
    if (message.opcode != .binary) return error.UnexpectedFrameType;
    return wire.decode(message.data);
}

pub fn sendReceived(ws: *std.http.Server.WebSocket, event_id: []const u8) !void {
    var buffer: [256]u8 = undefined;
    const body = try std.fmt.bufPrint(&buffer, "{{\"control\":\"RECEIVED\",\"event_id\":\"{s}\"}}", .{event_id});
    try ws.writeMessage(body, .text);
}

const std = @import("std");
const event = @import("event.zig");
const wire = @import("wire.zig");

pub const max_frame_bytes: usize = 4 * 1024 * 1024;

pub const TcpBindingError = error{
    FrameTooLarge,
} || wire.WireError || std.Io.Reader.Error || std.Io.Writer.Error;

/// TCP framing is `[u32 big-endian length][UbiQ wire envelope]`.
pub fn sendFramed(writer: *std.Io.Writer, envelope: event.Envelope, frame_buffer: []u8) TcpBindingError!void {
    const frame = try wire.encode(envelope, frame_buffer);
    if (frame.len > max_frame_bytes) return error.FrameTooLarge;
    try writer.writeInt(u32, @intCast(frame.len), .big);
    try writer.writeAll(frame);
    try writer.flush();
}

pub fn receiveFramed(reader: *std.Io.Reader, frame_buffer: []u8) TcpBindingError!event.Envelope {
    const frame_len: usize = try reader.takeInt(u32, .big);
    if (frame_len > frame_buffer.len or frame_len > max_frame_bytes) return error.FrameTooLarge;
    try reader.readSliceAll(frame_buffer[0..frame_len]);
    return wire.decode(frame_buffer[0..frame_len]);
}

/// Concrete std.net TCP sender. The UbiQ semantic layer never sees socket
/// addresses; only this binding owns the TCP stream.
pub fn send(stream: std.net.Stream, envelope: event.Envelope, frame_buffer: []u8) TcpBindingError!void {
    var socket_buffer: [8192]u8 = undefined;
    var socket_writer = stream.writer(&socket_buffer);
    try sendFramed(&socket_writer.interface, envelope, frame_buffer);
}

pub fn receive(stream: std.net.Stream, frame_buffer: []u8) TcpBindingError!event.Envelope {
    var socket_buffer: [8192]u8 = undefined;
    var socket_reader = stream.reader(&socket_buffer);
    return receiveFramed(socket_reader.interface(), frame_buffer);
}

fn sampleEnvelope() !event.Envelope {
    return .{
        .id = "evt-tcp",
        .event = try event.CanonicalEvent.parse("Inventory.reserve.request"),
        .correlation_id = "corr-tcp",
        .causation_id = "cause-tcp",
        .idempotency_key = "idem-tcp",
        .schema_id = "Inventory.reserve.request@1",
        .payload = "{\"sku\":\"ABC\"}",
        .guarantee = .received,
        .created_at_ms = 42,
    };
}

test "TCP length framing carries the canonical binary wire envelope" {
    var output: [2048]u8 = undefined;
    var writer = std.Io.Writer.fixed(&output);
    var frame_buffer: [1024]u8 = undefined;
    try sendFramed(&writer, try sampleEnvelope(), &frame_buffer);

    var input_buffer: [2048]u8 = undefined;
    @memcpy(input_buffer[0..writer.end], output[0..writer.end]);
    var reader = std.Io.Reader.fixed(input_buffer[0..writer.end]);
    var receive_buffer: [1024]u8 = undefined;
    const restored = try receiveFramed(&reader, &receive_buffer);
    try std.testing.expectEqualStrings("Inventory.reserve.request", restored.event.name);
    try std.testing.expectEqualStrings("{\"sku\":\"ABC\"}", restored.payload);
}

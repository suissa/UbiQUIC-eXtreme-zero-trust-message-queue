const std = @import("std");
const event = @import("event.zig");

pub const SseError = error{
    BufferTooSmall,
    InvalidPayload,
};

/// SSE is an egress projection only. Canonical event identity is carried in
/// the `event:` field and the raw payload is emitted as one `data:` line.
pub fn encode(envelope: event.Envelope, out: []u8) SseError![]const u8 {
    if (std.mem.indexOfScalar(u8, envelope.payload, '\n') != null or std.mem.indexOfScalar(u8, envelope.payload, '\r') != null) {
        return error.InvalidPayload;
    }
    return std.fmt.bufPrint(out,
        "id: {s}\nevent: {s}\ndata: {s}\n\n",
        .{ envelope.id, envelope.event.name, envelope.payload },
    ) catch error.BufferTooSmall;
}

pub fn respond(request: *std.http.Server.Request, envelope: event.Envelope, buffer: []u8) !void {
    const body = try encode(envelope, buffer);
    try request.respond(body, .{
        .keep_alive = false,
        .extra_headers = &.{
            .{ .name = "content-type", .value = "text/event-stream" },
            .{ .name = "cache-control", .value = "no-cache" },
        },
    });
}

test "SSE projection preserves canonical event name" {
    var buffer: [256]u8 = undefined;
    const envelope = event.Envelope{
        .id = "evt-1",
        .event = try event.CanonicalEvent.parse("Inventory.reserve.ok"),
        .correlation_id = "corr",
        .causation_id = "cause",
        .idempotency_key = "idem",
        .schema_id = "Inventory.reserve.ok@1",
        .payload = "{\"reserved\":true}",
        .guarantee = .received,
        .created_at_ms = 0,
    };
    const frame = try encode(envelope, &buffer);
    try std.testing.expect(std.mem.indexOf(u8, frame, "event: Inventory.reserve.ok") != null);
}

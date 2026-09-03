const std = @import("std");
const event = @import("event.zig");

pub const route_prefix = "/v1/events/";
pub const max_payload_bytes: usize = 1024 * 1024;

pub const HttpBindingError = error{
    InvalidMethod,
    InvalidRoute,
    MissingContentLength,
    PayloadTooLarge,
    MissingEventId,
    MissingCorrelationId,
    MissingCausationId,
    MissingIdempotencyKey,
    MissingSchemaId,
    InvalidGuarantee,
} || event.EventNameError || std.Io.Reader.Error || std.http.Server.Request.ExpectContinueError;

pub const Metadata = struct {
    id: []const u8,
    correlation_id: []const u8,
    causation_id: []const u8,
    idempotency_key: []const u8,
    schema_id: []const u8,
    guarantee: event.DeliveryGuarantee,
};

/// REST projection of the canonical event identity. Dots remain legal URI path
/// characters and the canonical name is recovered without transport aliases.
pub fn routeFor(canonical_event: []const u8, out: []u8) error{BufferTooSmall}![]const u8 {
    if (out.len < route_prefix.len + canonical_event.len) return error.BufferTooSmall;
    @memcpy(out[0..route_prefix.len], route_prefix);
    @memcpy(out[route_prefix.len .. route_prefix.len + canonical_event.len], canonical_event);
    return out[0 .. route_prefix.len + canonical_event.len];
}

pub fn eventFromTarget(target: []const u8) HttpBindingError!event.CanonicalEvent {
    if (!std.mem.startsWith(u8, target, route_prefix)) return error.InvalidRoute;
    const name = target[route_prefix.len..];
    if (name.len == 0 or std.mem.indexOfScalar(u8, name, '?') != null) return error.InvalidRoute;
    return event.CanonicalEvent.parse(name);
}

pub fn metadataFromHeaders(request: *const std.http.Server.Request) HttpBindingError!Metadata {
    var id: ?[]const u8 = null;
    var correlation_id: ?[]const u8 = null;
    var causation_id: ?[]const u8 = null;
    var idempotency_key: ?[]const u8 = null;
    var schema_id: ?[]const u8 = null;
    var guarantee: event.DeliveryGuarantee = .processed;

    var headers = request.iterateHeaders();
    while (headers.next()) |header| {
        if (std.ascii.eqlIgnoreCase(header.name, "x-ubiq-id")) id = header.value;
        if (std.ascii.eqlIgnoreCase(header.name, "x-ubiq-correlation-id")) correlation_id = header.value;
        if (std.ascii.eqlIgnoreCase(header.name, "x-ubiq-causation-id")) causation_id = header.value;
        if (std.ascii.eqlIgnoreCase(header.name, "x-ubiq-idempotency-key")) idempotency_key = header.value;
        if (std.ascii.eqlIgnoreCase(header.name, "x-ubiq-schema-id")) schema_id = header.value;
        if (std.ascii.eqlIgnoreCase(header.name, "x-ubiq-guarantee")) {
            guarantee = parseGuarantee(header.value) orelse return error.InvalidGuarantee;
        }
    }

    return .{
        .id = id orelse return error.MissingEventId,
        .correlation_id = correlation_id orelse return error.MissingCorrelationId,
        .causation_id = causation_id orelse return error.MissingCausationId,
        .idempotency_key = idempotency_key orelse return error.MissingIdempotencyKey,
        .schema_id = schema_id orelse return error.MissingSchemaId,
        .guarantee = guarantee,
    };
}

/// Reads an actual HTTP request into the canonical UbiQ envelope. `payload_out`
/// is caller-owned so the binding remains bounded and does not allocate.
pub fn readEnvelope(
    request: *std.http.Server.Request,
    body_reader_buffer: []u8,
    payload_out: []u8,
    created_at_ms: u64,
) HttpBindingError!event.Envelope {
    if (request.head.method != .POST) return error.InvalidMethod;
    const canonical = try eventFromTarget(request.head.target);
    const metadata = try metadataFromHeaders(request);
    const content_length = request.head.content_length orelse return error.MissingContentLength;
    if (content_length > payload_out.len or content_length > max_payload_bytes) return error.PayloadTooLarge;

    const reader = try request.readerExpectContinue(body_reader_buffer);
    const payload = payload_out[0..@intCast(content_length)];
    try reader.readSliceAll(payload);

    return .{
        .id = metadata.id,
        .event = canonical,
        .correlation_id = metadata.correlation_id,
        .causation_id = metadata.causation_id,
        .idempotency_key = metadata.idempotency_key,
        .schema_id = metadata.schema_id,
        .payload = payload,
        .guarantee = metadata.guarantee,
        .created_at_ms = created_at_ms,
    };
}

pub fn respondReceived(request: *std.http.Server.Request, event_id: []const u8) !void {
    var body: [256]u8 = undefined;
    const json = try std.fmt.bufPrint(&body, "{{\"event_id\":\"{s}\",\"delivery\":\"received\",\"settled\":false}}", .{event_id});
    try request.respond(json, .{
        .status = .accepted,
        .keep_alive = false,
        .extra_headers = &.{.{ .name = "content-type", .value = "application/json" }},
    });
}

fn parseGuarantee(value: []const u8) ?event.DeliveryGuarantee {
    if (std.mem.eql(u8, value, "best_effort")) return .best_effort;
    if (std.mem.eql(u8, value, "received")) return .received;
    if (std.mem.eql(u8, value, "processed")) return .processed;
    return null;
}

test "REST route preserves dotted canonical event identity" {
    var buffer: [128]u8 = undefined;
    const route = try routeFor("Financial.createInvoice.ok", &buffer);
    try std.testing.expectEqualStrings("/v1/events/Financial.createInvoice.ok", route);
    const restored = try eventFromTarget(route);
    try std.testing.expectEqualStrings("Financial.createInvoice.ok", restored.name);
}

test "REST route rejects query aliases so transport cannot mutate identity" {
    try std.testing.expectError(error.InvalidRoute, eventFromTarget("/v1/events/Financial.createInvoice.ok?v=1"));
}

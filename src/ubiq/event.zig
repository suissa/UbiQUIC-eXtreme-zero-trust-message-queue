const std = @import("std");

pub const EventState = enum {
    request,
    ok,
    error,
};

pub const Pipeline = enum {
    execution,
    consume,
    self_healing,
};

pub const EventNameError = error{
    EmptyEventName,
    InvalidCanonicalName,
    InvalidState,
};

/// Transport-neutral semantic event identity.
///
/// Canonical names use `<Entity>.<operation>.<state>` as the minimum shape.
/// Additional namespace/context segments are allowed; the final segment is the
/// state and therefore never depends on the physical transport address.
pub const CanonicalEvent = struct {
    name: []const u8,
    state: EventState,

    pub fn parse(name: []const u8) EventNameError!CanonicalEvent {
        if (name.len == 0) return error.EmptyEventName;

        var segments: usize = 1;
        for (name) |byte| {
            if (byte == '.') segments += 1;
            if (!(std.ascii.isAlphanumeric(byte) or byte == '.' or byte == '_' or byte == '-')) {
                return error.InvalidCanonicalName;
            }
        }
        if (segments < 3) return error.InvalidCanonicalName;

        const last_dot = std.mem.lastIndexOfScalar(u8, name, '.') orelse return error.InvalidCanonicalName;
        if (last_dot == 0 or last_dot + 1 >= name.len) return error.InvalidCanonicalName;
        const state_text = name[last_dot + 1 ..];
        const state: EventState = if (std.mem.eql(u8, state_text, "request"))
            .request
        else if (std.mem.eql(u8, state_text, "ok"))
            .ok
        else if (std.mem.eql(u8, state_text, "error"))
            .error
        else
            return error.InvalidState;

        return .{ .name = name, .state = state };
    }

    pub fn pipeline(self: CanonicalEvent) Pipeline {
        return switch (self.state) {
            .request => .execution,
            .ok => .consume,
            .error => .self_healing,
        };
    }
};

pub const DeliveryGuarantee = enum {
    best_effort,
    received,
    processed,
};

/// UbiQ envelope. Slices are caller-owned so the core can remain allocation-free.
pub const Envelope = struct {
    id: []const u8,
    event: CanonicalEvent,
    correlation_id: []const u8,
    causation_id: []const u8,
    idempotency_key: []const u8,
    schema_id: []const u8,
    payload: []const u8,
    guarantee: DeliveryGuarantee,
    created_at_ms: u64,
};

test "canonical event selects pipeline from state before payload inspection" {
    const ok = try CanonicalEvent.parse("Financial.createInvoice.ok");
    try std.testing.expectEqual(Pipeline.consume, ok.pipeline());

    const failure = try CanonicalEvent.parse("Financial.createInvoice.error");
    try std.testing.expectEqual(Pipeline.self_healing, failure.pipeline());

    const request = try CanonicalEvent.parse("Financial.createInvoice.request");
    try std.testing.expectEqual(Pipeline.execution, request.pipeline());
}

test "canonical event rejects transport-shaped or untyped names" {
    try std.testing.expectError(error.InvalidCanonicalName, CanonicalEvent.parse("invoice"));
    try std.testing.expectError(error.InvalidCanonicalName, CanonicalEvent.parse("Financial/createInvoice/ok"));
    try std.testing.expectError(error.InvalidState, CanonicalEvent.parse("Financial.createInvoice.done"));
}

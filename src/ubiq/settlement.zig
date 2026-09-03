const std = @import("std");
const delivery = @import("delivery.zig");

pub const ControlKind = enum {
    received,
    settled_ok,
    settled_error,
    lease_expired,
};

/// Transport-neutral control-plane frame emitted by workers/brokers.
/// This is intentionally separate from the domain event namespace.
pub const ControlFrame = struct {
    event_id: []const u8,
    delivery_id: []const u8,
    worker_id: []const u8,
    attempt: u32,
    kind: ControlKind,
    emitted_at_ms: u64,
};

pub const TransitionError = error{
    InvalidTransition,
};

pub fn transition(current: delivery.DeliveryState, kind: ControlKind) TransitionError!delivery.DeliveryState {
    return switch (kind) {
        .received => if (current == .leased) .received else error.InvalidTransition,
        .settled_ok => if (current == .leased or current == .received) .settled_ok else error.InvalidTransition,
        .settled_error => if (current == .leased or current == .received) .settled_error else error.InvalidTransition,
        .lease_expired => if (current == .leased or current == .received) .expired else error.InvalidTransition,
    };
}

pub fn needsRedelivery(state: delivery.DeliveryState) bool {
    return state == .published or state == .expired;
}

test "received frame does not imply settlement" {
    const state = try transition(.leased, .received);
    try std.testing.expectEqual(delivery.DeliveryState.received, state);
    try std.testing.expect(!needsRedelivery(state));
}

test "lease expiration becomes redeliverable" {
    const state = try transition(.received, .lease_expired);
    try std.testing.expectEqual(delivery.DeliveryState.expired, state);
    try std.testing.expect(needsRedelivery(state));
}

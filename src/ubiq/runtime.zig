const event = @import("event.zig");
const capability = @import("capability.zig");
const security = @import("security.zig");
const mapping = @import("mapping.zig");

pub const RuntimeError = error{
    CapabilityMismatch,
    SecurityMismatch,
};

pub const DispatchPlan = struct {
    pipeline: event.Pipeline,
    route: capability.RoutePlan,
    ingress_address: mapping.PhysicalAddress,
    egress_address: mapping.PhysicalAddress,
};

pub fn plan(envelope: event.Envelope, route: capability.RoutePlan, security_profile: security.SecurityProfile) RuntimeError!DispatchPlan {
    const requirements = capability.requirementsFor(envelope.guarantee);
    if (!route.satisfies(requirements)) return error.CapabilityMismatch;

    if (!security_profile.validateForTransport(route.ingress)) return error.SecurityMismatch;
    if (!security_profile.validateForTransport(route.egress)) return error.SecurityMismatch;

    return .{
        .pipeline = envelope.event.pipeline(),
        .route = route,
        .ingress_address = mapping.mapCanonical(route.ingress, envelope.event.name),
        .egress_address = mapping.mapCanonical(route.egress, envelope.event.name),
    };
}

test "processed event can bridge REST to WebSocket through durable JetStream" {
    const envelope = event.Envelope{
        .id = "evt-1",
        .event = try event.CanonicalEvent.parse("Financial.createInvoice.ok"),
        .correlation_id = "corr-1",
        .causation_id = "cause-1",
        .idempotency_key = "idem-1",
        .schema_id = "Financial.createInvoice.ok@1",
        .payload = "{}",
        .guarantee = .processed,
        .created_at_ms = 0,
    };
    const route = capability.RoutePlan{ .ingress = .rest, .backbone = .nats_jetstream, .egress = .websocket };
    const dispatch = try plan(envelope, route, .{});
    try @import("std").testing.expectEqual(event.Pipeline.consume, dispatch.pipeline);
}

test "processed guarantee refuses volatile-only backbone" {
    const envelope = event.Envelope{
        .id = "evt-2",
        .event = try event.CanonicalEvent.parse("Financial.createInvoice.error"),
        .correlation_id = "corr-2",
        .causation_id = "cause-2",
        .idempotency_key = "idem-2",
        .schema_id = "Financial.createInvoice.error@1",
        .payload = "{}",
        .guarantee = .processed,
        .created_at_ms = 0,
    };
    const route = capability.RoutePlan{ .ingress = .rest, .backbone = .memory, .egress = .websocket };
    try @import("std").testing.expectError(error.CapabilityMismatch, plan(envelope, route, .{}));
}

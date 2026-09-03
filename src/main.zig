const std = @import("std");
const ubiq = @import("ubiq/root.zig");

pub fn main() !void {
    const request = try ubiq.event.CanonicalEvent.parse("Financial.createInvoice.request");
    const envelope = ubiq.event.Envelope{
        .id = "01K-ubiq-demo",
        .event = request,
        .correlation_id = "01K-correlation",
        .causation_id = "01K-causation",
        .idempotency_key = "demo-create-invoice-1",
        .schema_id = "Financial.createInvoice.request@1",
        .payload = "{\"sale_id\":\"sale-100\"}",
        .guarantee = .processed,
        .created_at_ms = 0,
    };

    const route = ubiq.capability.RoutePlan{
        .ingress = .rest,
        .backbone = .nats_jetstream,
        .egress = .websocket,
    };

    const dispatch = try ubiq.runtime.plan(envelope, route, .{});

    var broker = ubiq.delivery.MemoryBroker(16){};
    const record_index = try broker.publish(envelope);
    _ = try broker.claim("financial-worker-01", 0, 5_000);
    try broker.markReceived(record_index, "financial-worker-01");
    try broker.settle(record_index, "financial-worker-01", .ok);

    const record = try broker.get(record_index);
    std.debug.print(
        "UbiQ: event={s} pipeline={s} ingress={s} egress={s} settlement={s}\n",
        .{
            envelope.event.name,
            @tagName(dispatch.pipeline),
            @tagName(dispatch.route.ingress),
            @tagName(dispatch.route.egress),
            @tagName(record.state),
        },
    );
}

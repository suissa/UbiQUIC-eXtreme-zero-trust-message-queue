const std = @import("std");
const ubiq = @import("ubiq");

pub fn main(init: std.process.Init) !void {
    const io = init.io;
    const address = try std.Io.net.IpAddress.parse("127.0.0.1", 4222);
    const stream = try address.connect(io, .{ .mode = .stream, .protocol = .tcp });
    defer stream.close(io);

    var read_buffer: [16 * 1024]u8 = undefined;
    var write_buffer: [16 * 1024]u8 = undefined;
    var stream_reader = stream.reader(io, &read_buffer);
    var stream_writer = stream.writer(io, &write_buffer);
    var client = ubiq.nats_client.Client.init(&stream_reader.interface, &stream_writer.interface);

    var protocol_buffer: [32 * 1024]u8 = undefined;
    try client.handshake("ubiq-zig-integration", &protocol_buffer);

    // NATS Core preserves the canonical identity end to end.
    const core_event = ubiq.event.Envelope{
        .id = "evt-core-1",
        .event = try ubiq.event.CanonicalEvent.parse("Financial.corePing.ok"),
        .correlation_id = "corr-core-1",
        .causation_id = "cause-core-1",
        .idempotency_key = "idem-core-1",
        .schema_id = "Financial.corePing.ok@1",
        .payload = "{\"core\":true}",
        .guarantee = .received,
        .created_at_ms = 1,
    };
    try client.subscribe(core_event.event.name, "1", &protocol_buffer);
    var wire_buffer: [4096]u8 = undefined;
    try client.publishEnvelope(core_event, &wire_buffer, &protocol_buffer);

    var message_buffer: [8192]u8 = undefined;
    var subject_buffer: [512]u8 = undefined;
    var reply_buffer: [512]u8 = undefined;
    const core_received = try client.nextEnvelope(&message_buffer, &subject_buffer, &reply_buffer, &protocol_buffer);
    if (!std.mem.eql(u8, core_received.event.name, core_event.event.name)) return error.CanonicalIdentityChanged;
    if (!std.mem.eql(u8, core_received.payload, core_event.payload)) return error.PayloadChanged;

    var js = try ubiq.jetstream.JetStream.init(&client, "UBIQ_EVENTS");
    try js.createStream(
        "Financial.>",
        "_INBOX.UBIQ.CREATE",
        "2",
        &message_buffer,
        &subject_buffer,
        &reply_buffer,
        &protocol_buffer,
    );

    const durable_event = ubiq.event.Envelope{
        .id = "evt-js-1",
        .event = try ubiq.event.CanonicalEvent.parse("Financial.createInvoice.ok"),
        .correlation_id = "corr-js-1",
        .causation_id = "cause-js-1",
        .idempotency_key = "idem-js-stable-1",
        .schema_id = "Financial.createInvoice.ok@1",
        .payload = "{\"invoice_id\":\"INV-1\"}",
        .guarantee = .processed,
        .created_at_ms = 2,
    };

    const first_ack = try js.publish(
        durable_event,
        "_INBOX.UBIQ.PUB1",
        "3",
        &wire_buffer,
        &message_buffer,
        &subject_buffer,
        &reply_buffer,
        &protocol_buffer,
    );
    if (first_ack.duplicate) return error.UnexpectedDuplicate;

    const duplicate_ack = try js.publish(
        durable_event,
        "_INBOX.UBIQ.PUB2",
        "4",
        &wire_buffer,
        &message_buffer,
        &subject_buffer,
        &reply_buffer,
        &protocol_buffer,
    );
    if (!duplicate_ack.duplicate) return error.DeduplicationFailed;
    if (first_ack.sequence != duplicate_ack.sequence) return error.DuplicateSequenceChanged;

    // Durable pull consumer with explicit broker ACK. This ACK is not the UbiQ
    // execution settlement; the two state machines are exercised separately.
    try js.createDurablePullConsumer(
        "UBIQ_WORKER",
        "Financial.createInvoice.ok",
        5_000_000_000,
        5,
        "_INBOX.UBIQ.CONSUMER",
        "5",
        &message_buffer,
        &subject_buffer,
        &reply_buffer,
        &protocol_buffer,
    );

    const pulled = try js.pullOne(
        "UBIQ_WORKER",
        2_000_000_000,
        "_INBOX.UBIQ.NEXT",
        "6",
        &message_buffer,
        &subject_buffer,
        &reply_buffer,
        &protocol_buffer,
    );
    if (!std.mem.eql(u8, pulled.envelope.event.name, durable_event.event.name)) return error.CanonicalIdentityChanged;
    if (!std.mem.eql(u8, pulled.envelope.payload, durable_event.payload)) return error.PayloadChanged;

    var execution_state: ubiq.delivery.DeliveryState = .leased;
    execution_state = try ubiq.settlement.transition(execution_state, .received);
    if (execution_state != .received) return error.ReceiptWasPromotedToSettlement;

    // Broker work-in-progress only extends JetStream's ack window.
    try js.working(pulled.ack_subject, &protocol_buffer);
    if (execution_state != .received) return error.BrokerAckMutatedExecutionState;

    // The worker/Action explicitly settles after successful execution.
    execution_state = try ubiq.settlement.transition(execution_state, .settled_ok);
    if (execution_state != .settled_ok) return error.ExecutionDidNotSettle;

    // Only after durable UbiQ settlement do we release the JetStream message.
    try js.ack(pulled.ack_subject, &protocol_buffer);
    if (execution_state != .settled_ok) return error.BrokerAckMutatedExecutionState;

    std.debug.print(
        "UbiQ NATS integration: core={s} stream={s} seq={d} duplicate={any} pulled={s} settlement={s}\n",
        .{
            core_received.event.name,
            first_ack.stream,
            first_ack.sequence,
            duplicate_ack.duplicate,
            pulled.envelope.event.name,
            @tagName(execution_state),
        },
    );
}

const std = @import("std");
const event = @import("event.zig");

pub const ContractError = error{
    InvalidCorrelation,
    InvalidStreamTransition,
    InvalidSubscription,
    QueueFull,
    PoisonMessage,
};

pub const Request = struct {
    envelope: event.Envelope,
    expected_schema: []const u8 = "",
    deadline_ms: u64 = 0,
};

pub const Response = struct {
    envelope: event.Envelope,
    request_correlation_id: []const u8,
};

pub fn validateResponse(request: Request, response: Response) ContractError!void {
    if (!std.mem.eql(u8, request.envelope.correlation_id, response.request_correlation_id) or
        !std.mem.eql(u8, request.envelope.correlation_id, response.envelope.correlation_id))
        return error.InvalidCorrelation;
    if (request.expected_schema.len != 0 and !std.mem.eql(u8, request.expected_schema, response.envelope.schema_id))
        return error.InvalidCorrelation;
}

pub const StreamState = enum { open, paused, ended, cancelled, failed };

pub const Stream = struct {
    id: []const u8,
    next_sequence: u64 = 0,
    accepted_sequence: u64 = 0,
    state: StreamState = .open,

    pub fn accept(self: *Stream, sequence: u64) ContractError!void {
        if (self.state != .open and self.state != .paused) return error.InvalidStreamTransition;
        if (sequence != self.accepted_sequence + 1) return error.InvalidStreamTransition;
        self.accepted_sequence = sequence;
    }

    pub fn resumeFrom(self: *Stream, last_accepted: u64) ContractError!void {
        if (self.state == .ended or self.state == .cancelled) return error.InvalidStreamTransition;
        if (last_accepted > self.accepted_sequence) return error.InvalidStreamTransition;
        self.accepted_sequence = last_accepted;
        self.state = .open;
    }

    pub fn pause(self: *Stream) ContractError!void {
        if (self.state != .open) return error.InvalidStreamTransition;
        self.state = .paused;
    }

    pub fn end(self: *Stream) ContractError!void {
        if (self.state == .cancelled or self.state == .failed) return error.InvalidStreamTransition;
        self.state = .ended;
    }
};

pub const SubscriptionMode = enum { broadcast, competing_consumer, durable_replay, ephemeral, flow_bound };

pub const Subscription = struct {
    id: []const u8,
    semantic_address: []const u8,
    mode: SubscriptionMode,
    execution_id: []const u8 = "",
    active: bool = true,

    pub fn validate(self: Subscription) ContractError!void {
        if (self.id.len == 0 or self.semantic_address.len == 0) return error.InvalidSubscription;
        if (self.mode == .flow_bound and self.execution_id.len == 0) return error.InvalidSubscription;
    }

    pub fn terminate(self: *Subscription) void {
        self.active = false;
    }
};

pub const QueuePolicy = struct {
    max_inflight: usize,
    lease_ms: u64,
    max_attempts: u32,
    priority: bool = false,
};

pub const QueueItem = struct {
    envelope: event.Envelope,
    attempts: u32 = 0,
    leased_until_ms: u64 = 0,
    quarantined: bool = false,
};

pub fn SemanticQueue(comptime capacity: usize) type {
    return struct {
        const Self = @This();
        items: [capacity]QueueItem = undefined,
        len: usize = 0,
        policy: QueuePolicy,

        pub fn init(policy: QueuePolicy) Self {
            return .{ .policy = policy };
        }

        pub fn enqueue(self: *Self, envelope: event.Envelope) ContractError!void {
            if (self.len >= capacity) return error.QueueFull;
            self.items[self.len] = .{ .envelope = envelope };
            self.len += 1;
        }

        pub fn next(self: *Self, now_ms: u64) ?*QueueItem {
            var selected: ?usize = null;
            var i: usize = 0;
            while (i < self.len) : (i += 1) {
                const item = &self.items[i];
                if (item.quarantined or now_ms < item.leased_until_ms) continue;
                if (selected == null or (self.policy.priority and item.attempts < self.items[selected.?].attempts)) selected = i;
            }
            if (selected) |index| {
                self.items[index].attempts += 1;
                self.items[index].leased_until_ms = now_ms + self.policy.lease_ms;
                if (self.items[index].attempts > self.policy.max_attempts) self.items[index].quarantined = true;
                return &self.items[index];
            }
            return null;
        }

        pub fn settle(_: *Self, item: *QueueItem) void {
            item.leased_until_ms = 0;
        }
    };
}

test "request response preserves correlation and response schema" {
    const request = Request{ .envelope = .{
        .id = "req",
        .event = try event.CanonicalEvent.parse("Agent.ask.request"),
        .correlation_id = "corr",
        .causation_id = "cause",
        .idempotency_key = "idem",
        .schema_id = "Agent.ask.request@1",
        .payload = "{}",
        .guarantee = .received,
        .created_at_ms = 0,
    }, .expected_schema = "Agent.ask.ok@1" };
    const response = Response{
        .request_correlation_id = "corr",
        .envelope = .{
            .id = "res",
            .event = try event.CanonicalEvent.parse("Agent.ask.ok"),
            .correlation_id = "corr",
            .causation_id = "req",
            .idempotency_key = "idem-res",
            .schema_id = "Agent.ask.ok@1",
            .payload = "{}",
            .guarantee = .received,
            .created_at_ms = 1,
        },
    };
    try validateResponse(request, response);
}

test "stream resume prevents duplicate accepted items" {
    var stream = Stream{ .id = "stream-1" };
    try stream.accept(1);
    try stream.accept(2);
    try stream.resumeFrom(1);
    try std.testing.expectError(error.InvalidStreamTransition, stream.accept(3));
}

test "flow-bound subscription requires execution identity" {
    const invalid = Subscription{ .id = "sub", .semantic_address = "Agent.ask.ok", .mode = .flow_bound };
    try std.testing.expectError(error.InvalidSubscription, invalid.validate());
}

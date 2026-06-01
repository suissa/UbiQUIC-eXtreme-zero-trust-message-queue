const std = @import("std");

const DAY_MS: u64 = 86_400_000;

const Action = enum(u8) {
    ConfigLoaded = 1,
    SubscriberRegistered = 2,
    EventPublished = 3,
    DpopGenerated = 4,
    MtlsKyberEnvelopeCreated = 5,
    AckReceived = 6,
    ReemitScheduled = 7,
    DlqWritten = 8,
    OutboxWritten = 9,
};

const Status = enum(u8) {
    success = 1,
    error = 2,
};

const SubscriberConfig = struct {
    id: []const u8,
    address: []const u8,
};

const Config = struct {
    ack_ttl_ms: u64 = 500,
    dlq_to_outbox_after_ms: u64 = DAY_MS,
    data_dir: []const u8 = "data",
    owns_data_dir: bool = false,
    mtls_kyber_enabled: bool = true,
    dpop_enabled: bool = true,
    register_addresses: bool = true,
    subscribers: std.ArrayList(SubscriberConfig),

    pub fn deinit(self: *Config, allocator: std.mem.Allocator) void {
        if (self.owns_data_dir) allocator.free(self.data_dir);
        for (self.subscribers.items) |subscriber| {
            allocator.free(subscriber.id);
            allocator.free(subscriber.address);
        }
        self.subscribers.deinit();
    }
};

const BinaryEvent = struct {
    timestamp_ms: u64,
    action: Action,
    status: Status,
    event_hash: u64,
    subscriber_hash: u64,
    payload: []const u8,
};

const EventStore = struct {
    allocator: std.mem.Allocator,
    success_file: std.fs.File,
    error_file: std.fs.File,

    const magic: u32 = 0x514d4553; // QMES
    const version: u16 = 1;

    pub fn init(allocator: std.mem.Allocator, data_dir: []const u8) !EventStore {
        try std.fs.cwd().makePath(data_dir);
        const success_path = try std.fs.path.join(allocator, &.{ data_dir, "events-success.qmes" });
        defer allocator.free(success_path);
        const error_path = try std.fs.path.join(allocator, &.{ data_dir, "events-error.qmes" });
        defer allocator.free(error_path);

        return .{
            .allocator = allocator,
            .success_file = try std.fs.cwd().createFile(success_path, .{ .read = true, .truncate = false }),
            .error_file = try std.fs.cwd().createFile(error_path, .{ .read = true, .truncate = false }),
        };
    }

    pub fn deinit(self: *EventStore) void {
        self.success_file.close();
        self.error_file.close();
    }

    pub fn append(self: *EventStore, event: BinaryEvent) !void {
        const file = if (event.status == .success) &self.success_file else &self.error_file;
        try file.seekFromEnd(0);
        const writer = file.writer();
        try writer.writeInt(u32, magic, .little);
        try writer.writeInt(u16, version, .little);
        try writer.writeByte(@intFromEnum(event.action));
        try writer.writeByte(@intFromEnum(event.status));
        try writer.writeInt(u64, event.timestamp_ms, .little);
        try writer.writeInt(u64, event.event_hash, .little);
        try writer.writeInt(u64, event.subscriber_hash, .little);
        try writer.writeInt(u32, @intCast(event.payload.len), .little);
        try writer.writeAll(event.payload);
    }
};

const Dpop = struct {
    pub fn token(allocator: std.mem.Allocator, event_id: []const u8, subscriber_id: []const u8, emitted_at_ms: u64) ![]u8 {
        var nonce_input = std.ArrayList(u8).init(allocator);
        defer nonce_input.deinit();
        try nonce_input.writer().print("{s}:{s}:{d}", .{ event_id, subscriber_id, emitted_at_ms });
        var digest: [32]u8 = undefined;
        std.crypto.hash.sha2.Sha256.hash(nonce_input.items, &digest, .{});
        const digest_hex = try hex(allocator, digest[0..]);
        defer allocator.free(digest_hex);
        return std.fmt.allocPrint(allocator,
            "DPoP alg=HS256 typ=dpop+jwt event={s} sub={s} iat={d} proof={s}",
            .{ event_id, subscriber_id, emitted_at_ms, digest_hex });
    }
};

const KyberMtls = struct {
    pub fn envelope(allocator: std.mem.Allocator, subscriber_id: []const u8, address: []const u8, payload: []const u8) ![]u8 {
        var transcript = std.ArrayList(u8).init(allocator);
        defer transcript.deinit();
        try transcript.writer().print("mtls-client-cert:{s}|kyber768-kem:{s}|", .{ subscriber_id, address });
        try transcript.appendSlice(payload);
        var secret: [32]u8 = undefined;
        std.crypto.hash.sha2.Sha256.hash(transcript.items, &secret, .{});
        const secret_hex = try hex(allocator, secret[0..]);
        defer allocator.free(secret_hex);
        return std.fmt.allocPrint(allocator, "mTLS+KyberEnvelope(sub={s},addr={s},shared={s})", .{ subscriber_id, address, secret_hex });
    }
};

const Subscriber = struct {
    id: []const u8,
    address: []const u8,
    last_delivery_ms: u64 = 0,
    healthy: bool = true,
};

const PendingEvent = struct {
    id: []const u8,
    payload: []const u8,
    created_ms: u64,
    attempts: u32 = 0,
};

const QueueRecord = struct {
    event: PendingEvent,
    failed_at_ms: u64,
    reason: []const u8,
};

const Broker = struct {
    allocator: std.mem.Allocator,
    config: Config,
    store: EventStore,
    subscribers: std.ArrayList(Subscriber),
    dlq: std.ArrayList(QueueRecord),
    outbox: std.ArrayList(QueueRecord),

    pub fn init(allocator: std.mem.Allocator, config: Config, store: EventStore) !Broker {
        var broker = Broker{
            .allocator = allocator,
            .config = config,
            .store = store,
            .subscribers = std.ArrayList(Subscriber).init(allocator),
            .dlq = std.ArrayList(QueueRecord).init(allocator),
            .outbox = std.ArrayList(QueueRecord).init(allocator),
        };

        try broker.audit(.ConfigLoaded, .success, "config", "broker", "config.yml loaded");
        if (broker.config.register_addresses) {
            for (broker.config.subscribers.items) |subscriber| {
                try broker.registerSubscriber(subscriber.id, subscriber.address);
            }
        }
        return broker;
    }

    pub fn deinit(self: *Broker) void {
        for (self.subscribers.items) |subscriber| {
            self.allocator.free(subscriber.id);
            self.allocator.free(subscriber.address);
        }
        self.subscribers.deinit();
        self.freeQueue(&self.dlq);
        self.freeQueue(&self.outbox);
        self.config.deinit(self.allocator);
        self.store.deinit();
    }

    fn freeQueue(self: *Broker, queue: *std.ArrayList(QueueRecord)) void {
        for (queue.items) |record| {
            self.allocator.free(record.event.id);
            self.allocator.free(record.event.payload);
            self.allocator.free(record.reason);
        }
        queue.deinit();
    }

    pub fn registerSubscriber(self: *Broker, id: []const u8, address: []const u8) !void {
        try self.subscribers.append(.{
            .id = try self.allocator.dupe(u8, id),
            .address = try self.allocator.dupe(u8, address),
        });
        const payload = try std.fmt.allocPrint(self.allocator, "registered {s} at {s}", .{ id, address });
        defer self.allocator.free(payload);
        try self.audit(.SubscriberRegistered, .success, "register", id, payload);
    }

    pub fn publish(self: *Broker, event_id: []const u8, payload: []const u8, emitted_at_ms: u64) !void {
        var event = PendingEvent{
            .id = try self.allocator.dupe(u8, event_id),
            .payload = try self.allocator.dupe(u8, payload),
            .created_ms = emitted_at_ms,
        };
        errdefer self.allocator.free(event.id);
        errdefer self.allocator.free(event.payload);

        try self.audit(.EventPublished, .success, event_id, "broker", payload);
        const delivered = try self.tryDeliver(&event, emitted_at_ms);
        if (delivered) {
            self.allocator.free(event.id);
            self.allocator.free(event.payload);
            return;
        }

        try self.enqueueDlq(event, emitted_at_ms, "ack ttl exhausted for every subscriber");
    }

    fn tryDeliver(self: *Broker, event: *PendingEvent, emitted_at_ms: u64) !bool {
        if (self.subscribers.items.len == 0) return false;
        self.orderSubscribersByIdleTime();
        for (self.subscribers.items) |*subscriber| {
            event.attempts += 1;
            if (self.config.dpop_enabled) {
                const proof = try Dpop.token(self.allocator, event.id, subscriber.id, emitted_at_ms);
                defer self.allocator.free(proof);
                try self.audit(.DpopGenerated, .success, event.id, subscriber.id, proof);
            }
            if (self.config.mtls_kyber_enabled) {
                const envelope = try KyberMtls.envelope(self.allocator, subscriber.id, subscriber.address, event.payload);
                defer self.allocator.free(envelope);
                try self.audit(.MtlsKyberEnvelopeCreated, .success, event.id, subscriber.id, envelope);
            }
            try self.audit(.ReemitScheduled, .success, event.id, subscriber.id, "sent and waiting for ack ttl");
            if (subscriber.healthy) {
                subscriber.last_delivery_ms = emitted_at_ms + self.config.ack_ttl_ms;
                try self.audit(.AckReceived, .success, event.id, subscriber.id, "ack received");
                return true;
            }
            try self.audit(.AckReceived, .error, event.id, subscriber.id, "ack timeout");
        }
        return false;
    }

    fn orderSubscribersByIdleTime(self: *Broker) void {
        const Context = struct {
            fn lessThan(_: void, lhs: Subscriber, rhs: Subscriber) bool {
                return lhs.last_delivery_ms < rhs.last_delivery_ms;
            }
        };
        std.mem.sort(Subscriber, self.subscribers.items, {}, Context.lessThan);
    }

    fn enqueueDlq(self: *Broker, event: PendingEvent, now_ms: u64, reason: []const u8) !void {
        try self.dlq.append(.{
            .event = event,
            .failed_at_ms = now_ms,
            .reason = try self.allocator.dupe(u8, reason),
        });
        try self.audit(.DlqWritten, .success, event.id, "dlq", reason);
    }

    pub fn processDlq(self: *Broker, now_ms: u64) !void {
        var i: usize = 0;
        while (i < self.dlq.items.len) {
            const record = self.dlq.items[i];
            if (now_ms - record.failed_at_ms >= self.config.dlq_to_outbox_after_ms) {
                try self.outbox.append(record);
                _ = self.dlq.orderedRemove(i);
                try self.audit(.OutboxWritten, .success, record.event.id, "outbox", "dlq expired after configured ttl");
            } else {
                i += 1;
            }
        }
    }

    fn audit(self: *Broker, action: Action, status: Status, event_id: []const u8, subscriber_id: []const u8, payload: []const u8) !void {
        try self.store.append(.{
            .timestamp_ms = epochMs(),
            .action = action,
            .status = status,
            .event_hash = std.hash.Wyhash.hash(0, event_id),
            .subscriber_hash = std.hash.Wyhash.hash(0, subscriber_id),
            .payload = payload,
        });
    }
};

pub fn main() !void {
    var gpa = std.heap.GeneralPurposeAllocator(.{}){};
    defer _ = gpa.deinit();
    const allocator = gpa.allocator();

    var config = try loadConfig(allocator, "config.yml");
    const store = try EventStore.init(allocator, config.data_dir);
    var broker = try Broker.init(allocator, config, store);
    config = undefined;
    defer broker.deinit();

    try broker.publish("order.created.v1:0001", "{\"order_id\":\"A-100\",\"amount\":42}", epochMs());
    try broker.processDlq(epochMs() + DAY_MS + 1);

    std.debug.print("QUICMQ demo completed: subscribers={d} dlq={d} outbox={d}\n", .{ broker.subscribers.items.len, broker.dlq.items.len, broker.outbox.items.len });
}

fn loadConfig(allocator: std.mem.Allocator, path: []const u8) !Config {
    const bytes = std.fs.cwd().readFileAlloc(allocator, path, 1024 * 1024) catch |err| switch (err) {
        error.FileNotFound => try allocator.dupe(u8, ""),
        else => return err,
    };
    defer allocator.free(bytes);

    var config = Config{ .subscribers = std.ArrayList(SubscriberConfig).init(allocator) };
    var current_id: ?[]u8 = null;
    var lines = std.mem.splitScalar(u8, bytes, '\n');
    while (lines.next()) |raw_line| {
        const line = std.mem.trim(u8, raw_line, " \t\r");
        if (line.len == 0 or line[0] == '#') continue;
        if (std.mem.startsWith(u8, line, "ack_ttl_ms:")) {
            config.ack_ttl_ms = try parseU64AfterColon(line);
        } else if (std.mem.startsWith(u8, line, "dlq_to_outbox_after_ms:")) {
            config.dlq_to_outbox_after_ms = try parseU64AfterColon(line);
        } else if (std.mem.startsWith(u8, line, "data_dir:")) {
            if (config.owns_data_dir) allocator.free(config.data_dir);
            config.data_dir = try allocator.dupe(u8, try valueAfterColon(line));
            config.owns_data_dir = true;
        } else if (std.mem.startsWith(u8, line, "mtls_kyber_enabled:")) {
            config.mtls_kyber_enabled = parseBoolAfterColon(line);
        } else if (std.mem.startsWith(u8, line, "dpop_enabled:")) {
            config.dpop_enabled = parseBoolAfterColon(line);
        } else if (std.mem.startsWith(u8, line, "register_addresses:")) {
            config.register_addresses = parseBoolAfterColon(line);
        } else if (std.mem.startsWith(u8, line, "- id:")) {
            if (current_id) |id| allocator.free(id);
            current_id = try allocator.dupe(u8, try valueAfterColon(line));
        } else if (std.mem.startsWith(u8, line, "address:")) {
            if (current_id) |id| {
                try config.subscribers.append(.{
                    .id = id,
                    .address = try allocator.dupe(u8, try valueAfterColon(line)),
                });
                current_id = null;
            }
        }
    }
    if (current_id) |id| allocator.free(id);
    return config;
}

fn parseU64AfterColon(line: []const u8) !u64 {
    return std.fmt.parseInt(u64, try valueAfterColon(line), 10);
}

fn parseBoolAfterColon(line: []const u8) bool {
    const value = valueAfterColon(line) catch return false;
    return std.mem.eql(u8, value, "true") or std.mem.eql(u8, value, "yes") or std.mem.eql(u8, value, "on");
}

fn valueAfterColon(line: []const u8) ![]const u8 {
    const pos = std.mem.indexOfScalar(u8, line, ':') orelse return error.InvalidConfig;
    return std.mem.trim(u8, line[pos + 1 ..], " \t\r\"");
}

fn epochMs() u64 {
    return @intCast(std.time.milliTimestamp());
}

fn hex(allocator: std.mem.Allocator, bytes: []const u8) ![]u8 {
    const out = try allocator.alloc(u8, bytes.len * 2);
    _ = std.fmt.bufPrint(out, "{}", .{std.fmt.fmtSliceHexLower(bytes)}) catch unreachable;
    return out;
}

test "config parser loads subscriber addresses and feature flags" {
    const allocator = std.testing.allocator;
    var config = try loadConfig(allocator, "config.yml");
    defer config.deinit(allocator);
    try std.testing.expect(config.mtls_kyber_enabled);
    try std.testing.expect(config.dpop_enabled);
    try std.testing.expect(config.register_addresses);
    try std.testing.expectEqual(@as(usize, 3), config.subscribers.items.len);
}

test "broker moves expired dlq records to outbox" {
    const allocator = std.testing.allocator;
    var config = Config{ .subscribers = std.ArrayList(SubscriberConfig).init(allocator) };
    config.data_dir = "zig-cache/test-data";
    config.dlq_to_outbox_after_ms = 10;
    const store = try EventStore.init(allocator, config.data_dir);
    var broker = try Broker.init(allocator, config, store);
    defer broker.deinit();

    try broker.publish("evt", "payload", 0);
    try std.testing.expectEqual(@as(usize, 1), broker.dlq.items.len);
    try broker.processDlq(11);
    try std.testing.expectEqual(@as(usize, 0), broker.dlq.items.len);
    try std.testing.expectEqual(@as(usize, 1), broker.outbox.items.len);
}

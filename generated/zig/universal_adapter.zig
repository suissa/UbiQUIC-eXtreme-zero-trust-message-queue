// GENERATED from schemas/universal-adapter.schema.yml. Do not add transport semantics here.
const std = @import("std");

pub const AdapterError = error{
    InvalidConfig,
    NotOpen,
    UnsupportedCapability,
    SecurityRequirementNotMet,
    MappingNotReversible,
    PublishFailed,
    SubscribeFailed,
    AckFailed,
};

pub const Envelope = struct {
    canonical_event: []const u8,
    message_id: []const u8,
    interaction_id: []const u8,
    correlation_id: []const u8,
    payload: []const u8,
};

pub const Delivery = struct {
    delivery_id: []const u8,
    canonical_event: []const u8,
    transport_name: []const u8,
};

pub const Capabilities = struct {
    reliable: bool,
    ordered: bool,
    durable: bool,
    replay: bool,
    request_reply: bool,
    pub_sub: bool,
};

pub const Health = enum { healthy, degraded, unhealthy };

pub const Adapter = struct {
    ctx: *anyopaque,
    vtable: *const VTable,

    pub const VTable = struct {
        open: *const fn (*anyopaque, []const u8) AdapterError!void,
        close: *const fn (*anyopaque) void,
        publish: *const fn (*anyopaque, Envelope) AdapterError!void,
        subscribe: *const fn (*anyopaque, []const u8) AdapterError!void,
        ack: *const fn (*anyopaque, Delivery) AdapterError!void,
        nack: *const fn (*anyopaque, Delivery, []const u8) AdapterError!void,
        health: *const fn (*anyopaque) Health,
        capabilities: *const fn (*anyopaque) Capabilities,
        mapCanonicalToTransport: *const fn (*anyopaque, []const u8, []u8) AdapterError![]const u8,
        mapTransportToCanonical: *const fn (*anyopaque, []const u8, []u8) AdapterError![]const u8,
    };

    pub fn open(self: Adapter, config_yaml: []const u8) AdapterError!void {
        return self.vtable.open(self.ctx, config_yaml);
    }
    pub fn close(self: Adapter) void { self.vtable.close(self.ctx); }
    pub fn publish(self: Adapter, envelope: Envelope) AdapterError!void { return self.vtable.publish(self.ctx, envelope); }
    pub fn subscribe(self: Adapter, canonical_event: []const u8) AdapterError!void { return self.vtable.subscribe(self.ctx, canonical_event); }
    pub fn ack(self: Adapter, delivery: Delivery) AdapterError!void { return self.vtable.ack(self.ctx, delivery); }
    pub fn nack(self: Adapter, delivery: Delivery, reason: []const u8) AdapterError!void { return self.vtable.nack(self.ctx, delivery, reason); }
    pub fn health(self: Adapter) Health { return self.vtable.health(self.ctx); }
    pub fn capabilities(self: Adapter) Capabilities { return self.vtable.capabilities(self.ctx); }
    pub fn mapCanonicalToTransport(self: Adapter, canonical_event: []const u8, out: []u8) AdapterError![]const u8 {
        return self.vtable.mapCanonicalToTransport(self.ctx, canonical_event, out);
    }
    pub fn mapTransportToCanonical(self: Adapter, transport_name: []const u8, out: []u8) AdapterError![]const u8 {
        return self.vtable.mapTransportToCanonical(self.ctx, transport_name, out);
    }
};

pub fn assertCanonicalRoundTrip(adapter: Adapter, canonical_event: []const u8, a: []u8, b: []u8) AdapterError!void {
    const transport_name = try adapter.mapCanonicalToTransport(canonical_event, a);
    const restored = try adapter.mapTransportToCanonical(transport_name, b);
    if (!std.mem.eql(u8, canonical_event, restored)) return AdapterError.MappingNotReversible;
}

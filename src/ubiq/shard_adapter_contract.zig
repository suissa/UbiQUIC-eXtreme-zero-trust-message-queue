const std = @import("std");

pub const StoreKind = enum { postgres, mongodb, sqlite, generic_sql, document_store };

pub const Error = error{
    EmptyNamespace,
    EmptyValue,
    InvalidShardCount,
    InvalidOwner,
    StaleFence,
    UnsupportedGuarantee,
    InvalidAdapter,
};

/// Canonical, namespace-qualified shard key. The same key bytes must be used by
/// every adapter; database-specific collation must not alter placement.
pub const ShardKey = struct {
    namespace: []const u8,
    value: []const u8,

    pub fn validate(self: ShardKey) Error!void {
        if (self.namespace.len == 0) return error.EmptyNamespace;
        if (self.value.len == 0) return error.EmptyValue;
    }

    pub fn hash(self: ShardKey) u64 {
        var hash_value: u64 = 14695981039346656037;
        for (self.namespace) |byte| {
            hash_value ^= byte;
            hash_value *%= 1099511628211;
        }
        hash_value ^= 0xff;
        hash_value *%= 1099511628211;
        for (self.value) |byte| {
            hash_value ^= byte;
            hash_value *%= 1099511628211;
        }
        return hash_value;
    }

    pub fn shard(self: ShardKey, shard_count: u32) Error!u32 {
        try self.validate();
        if (shard_count == 0) return error.InvalidShardCount;
        return @intCast(self.hash() % shard_count);
    }
};

pub const Ownership = struct {
    shard_id: u32,
    owner_id: []const u8,
    epoch: u64,
    fencing_generation: u64,

    pub fn validate(self: Ownership, expected_owner: []const u8, expected_epoch: u64, current_fence: u64) Error!void {
        if (self.owner_id.len == 0 or expected_owner.len == 0) return error.InvalidOwner;
        if (!std.mem.eql(u8, self.owner_id, expected_owner) or self.epoch != expected_epoch or
            self.fencing_generation != current_fence) return error.StaleFence;
    }
};

pub const Capabilities = struct {
    transactions: bool,
    conditional_writes: bool,
    online_copy: bool,
    resumable_checkpoint: bool,
    dual_read: bool,
    dual_write: bool,
};

pub const AdapterContract = struct {
    name: []const u8,
    kind: StoreKind,
    capabilities: Capabilities,

    pub fn validate(self: AdapterContract, require_online_migration: bool) Error!void {
        if (self.name.len == 0) return error.InvalidAdapter;
        if (!self.capabilities.conditional_writes) return error.UnsupportedGuarantee;
        if (require_online_migration and (!self.capabilities.online_copy or
            !self.capabilities.resumable_checkpoint or !self.capabilities.dual_read or
            !self.capabilities.dual_write)) return error.UnsupportedGuarantee;
    }
};

test "canonical shard key is namespace qualified and deterministic" {
    const key = ShardKey{ .namespace = "customer", .value = "42" };
    try key.validate();
    try std.testing.expectEqual(key.hash(), (ShardKey{ .namespace = "customer", .value = "42" }).hash());
    try std.testing.expect(key.hash() != (ShardKey{ .namespace = "invoice", .value = "42" }).hash());
    try std.testing.expect((try key.shard(64)) < 64);
    try std.testing.expectError(error.InvalidShardCount, key.shard(0));
}

test "shard ownership rejects stale owner epoch and fencing generation" {
    const owner = Ownership{ .shard_id = 4, .owner_id = "node-a", .epoch = 7, .fencing_generation = 11 };
    try owner.validate("node-a", 7, 11);
    try std.testing.expectError(error.StaleFence, owner.validate("node-a", 7, 12));
    try std.testing.expectError(error.StaleFence, owner.validate("node-b", 7, 11));
    try std.testing.expectError(error.StaleFence, owner.validate("node-a", 8, 11));
}

test "adapter must declare conditional writes and migration guarantees honestly" {
    const safe = AdapterContract{
        .name = "sql-adapter",
        .kind = .generic_sql,
        .capabilities = .{
            .transactions = true,
            .conditional_writes = true,
            .online_copy = true,
            .resumable_checkpoint = true,
            .dual_read = true,
            .dual_write = true,
        },
    };
    try safe.validate(true);
    try std.testing.expectError(error.UnsupportedGuarantee, (AdapterContract{
        .name = "limited-document-store",
        .kind = .document_store,
        .capabilities = .{
            .transactions = false,
            .conditional_writes = false,
            .online_copy = false,
            .resumable_checkpoint = false,
            .dual_read = false,
            .dual_write = false,
        },
    }).validate(true));
}

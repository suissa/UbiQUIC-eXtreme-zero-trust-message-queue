const std = @import("std");

pub const StoreKind = enum { postgres, mongodb, sqlite, generic_sql, document_store };
pub const MigrationState = enum {
    planned,
    copying,
    verifying,
    ready_to_cutover,
    cutover,
    rolling_back,
    rolled_back,
    failed,
};

pub const Capabilities = struct {
    transactions: bool,
    conditional_writes: bool,
    online_copy: bool,
    resumable_checkpoint: bool,
    dual_write: bool,
};

pub const Adapter = struct {
    name: []const u8,
    kind: StoreKind,
    capabilities: Capabilities,

    pub fn validate(self: Adapter) AdapterError!void {
        if (self.name.len == 0) return error.InvalidAdapter;
        if (!self.capabilities.conditional_writes) return error.FencingUnsupported;
        if (!self.capabilities.resumable_checkpoint) return error.CheckpointUnsupported;
    }
};

pub const AdapterError = error{ InvalidAdapter, FencingUnsupported, CheckpointUnsupported };
pub const MigrationError = error{
    InvalidTransition,
    StaleFence,
    VerificationRequired,
    MissingCheckpoint,
};

pub const Migration = struct {
    id: []const u8,
    from_shard: u32,
    to_shard: u32,
    state: MigrationState = .planned,
    fence_generation: u64,
    checkpoint: ?[]const u8 = null,
    verified: bool = false,

    pub fn begin(self: *Migration, current_fence: u64) MigrationError!void {
        if (self.state != .planned) return error.InvalidTransition;
        if (current_fence != self.fence_generation) return error.StaleFence;
        self.state = .copying;
    }

    pub fn saveCheckpoint(self: *Migration, token: []const u8, current_fence: u64) MigrationError!void {
        if (self.state != .copying) return error.InvalidTransition;
        if (current_fence != self.fence_generation) return error.StaleFence;
        if (token.len == 0) return error.MissingCheckpoint;
        self.checkpoint = token;
    }

    pub fn finishCopy(self: *Migration, current_fence: u64) MigrationError!void {
        if (self.state != .copying or self.checkpoint == null) return error.MissingCheckpoint;
        if (current_fence != self.fence_generation) return error.StaleFence;
        self.state = .verifying;
    }

    pub fn verify(self: *Migration, valid: bool, current_fence: u64) MigrationError!void {
        if (self.state != .verifying) return error.InvalidTransition;
        if (current_fence != self.fence_generation) return error.StaleFence;
        if (!valid) {
            self.state = .failed;
            return;
        }
        self.verified = true;
        self.state = .ready_to_cutover;
    }

    pub fn cutover(self: *Migration, current_fence: u64) MigrationError!void {
        if (self.state != .ready_to_cutover) return error.VerificationRequired;
        if (!self.verified) return error.VerificationRequired;
        if (current_fence != self.fence_generation) return error.StaleFence;
        self.state = .cutover;
    }

    pub fn rollback(self: *Migration, current_fence: u64) MigrationError!void {
        if (self.state == .cutover or self.state == .rolled_back) return error.InvalidTransition;
        if (current_fence != self.fence_generation) return error.StaleFence;
        self.state = .rolling_back;
        self.state = .rolled_back;
    }
};

test "database adapters must support fencing and resumable checkpoints" {
    const adapter = Adapter{
        .name = "postgres-contract",
        .kind = .postgres,
        .capabilities = .{
            .transactions = true,
            .conditional_writes = true,
            .online_copy = true,
            .resumable_checkpoint = true,
            .dual_write = true,
        },
    };
    try adapter.validate();
    try std.testing.expectError(error.FencingUnsupported, (Adapter{
        .name = "unsafe",
        .kind = .sqlite,
        .capabilities = .{
            .transactions = true,
            .conditional_writes = false,
            .online_copy = false,
            .resumable_checkpoint = true,
            .dual_write = false,
        },
    }).validate());
}

test "migration requires checkpoint verification and current fencing generation" {
    var migration = Migration{ .id = "move-1", .from_shard = 1, .to_shard = 2, .fence_generation = 7 };
    try migration.begin(7);
    try std.testing.expectError(error.MissingCheckpoint, migration.finishCopy(7));
    try migration.saveCheckpoint("offset-42", 7);
    try migration.finishCopy(7);
    try migration.verify(true, 7);
    try migration.cutover(7);
    try std.testing.expectEqual(MigrationState.cutover, migration.state);
    try std.testing.expectError(error.InvalidTransition, migration.rollback(7));
}

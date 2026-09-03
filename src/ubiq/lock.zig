const std = @import("std");

pub const LockError = error{
    TableFull,
    AlreadyLocked,
    NotOwner,
    UnknownLock,
    Expired,
};

pub const Lock = struct {
    key: []const u8,
    owner: []const u8,
    expires_at_ms: u64,
    generation: u64,
    active: bool = true,
};

/// Bounded lease lock table. Locks expire automatically and use a monotonically
/// increasing generation as a fencing token so stale workers can be rejected by
/// storage/effect adapters after another worker acquires the same resource.
pub fn LeaseLockTable(comptime capacity: usize) type {
    return struct {
        const Self = @This();

        locks: [capacity]Lock = undefined,
        len: usize = 0,
        next_generation: u64 = 1,

        pub fn acquire(self: *Self, key: []const u8, owner: []const u8, now_ms: u64, ttl_ms: u64) LockError!u64 {
            if (self.find(key)) |index| {
                const existing = &self.locks[index];
                if (existing.active and now_ms < existing.expires_at_ms) return error.AlreadyLocked;
                existing.owner = owner;
                existing.expires_at_ms = now_ms + ttl_ms;
                existing.generation = self.takeGeneration();
                existing.active = true;
                return existing.generation;
            }

            if (self.len >= capacity) return error.TableFull;
            const generation = self.takeGeneration();
            self.locks[self.len] = .{
                .key = key,
                .owner = owner,
                .expires_at_ms = now_ms + ttl_ms,
                .generation = generation,
            };
            self.len += 1;
            return generation;
        }

        pub fn renew(self: *Self, key: []const u8, owner: []const u8, now_ms: u64, ttl_ms: u64) LockError!void {
            const index = self.find(key) orelse return error.UnknownLock;
            const lock = &self.locks[index];
            if (!lock.active or now_ms >= lock.expires_at_ms) return error.Expired;
            if (!std.mem.eql(u8, lock.owner, owner)) return error.NotOwner;
            lock.expires_at_ms = now_ms + ttl_ms;
        }

        pub fn release(self: *Self, key: []const u8, owner: []const u8) LockError!void {
            const index = self.find(key) orelse return error.UnknownLock;
            const lock = &self.locks[index];
            if (!lock.active) return error.UnknownLock;
            if (!std.mem.eql(u8, lock.owner, owner)) return error.NotOwner;
            lock.active = false;
        }

        pub fn validateFence(self: *const Self, key: []const u8, owner: []const u8, generation: u64, now_ms: u64) bool {
            const index = self.find(key) orelse return false;
            const lock = &self.locks[index];
            return lock.active and now_ms < lock.expires_at_ms and
                lock.generation == generation and std.mem.eql(u8, lock.owner, owner);
        }

        pub fn reapExpired(self: *Self, now_ms: u64) void {
            var i: usize = 0;
            while (i < self.len) : (i += 1) {
                if (self.locks[i].active and now_ms >= self.locks[i].expires_at_ms) {
                    self.locks[i].active = false;
                }
            }
        }

        fn takeGeneration(self: *Self) u64 {
            const generation = self.next_generation;
            self.next_generation +%= 1;
            if (self.next_generation == 0) self.next_generation = 1;
            return generation;
        }

        fn find(self: *const Self, key: []const u8) ?usize {
            var i: usize = 0;
            while (i < self.len) : (i += 1) {
                if (std.mem.eql(u8, self.locks[i].key, key)) return i;
            }
            return null;
        }
    };
}

test "expired lock can be reacquired and stale fencing token becomes invalid" {
    var table = LeaseLockTable(4){};
    const old_generation = try table.acquire("invoice:1", "worker-a", 0, 10);
    try std.testing.expect(table.validateFence("invoice:1", "worker-a", old_generation, 5));

    table.reapExpired(10);
    const new_generation = try table.acquire("invoice:1", "worker-b", 10, 10);
    try std.testing.expect(new_generation != old_generation);
    try std.testing.expect(!table.validateFence("invoice:1", "worker-a", old_generation, 11));
    try std.testing.expect(table.validateFence("invoice:1", "worker-b", new_generation, 11));
}

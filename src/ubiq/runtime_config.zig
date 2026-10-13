const std = @import("std");
const features = @import("features.zig");

pub const ConfigError = error{
    InvalidJson,
    InvalidRoot,
    InvalidProfile,
    InvalidFeature,
    InvalidFeatures,
    UnknownOverride,
    InvalidOverride,
    OutOfMemory,
} || std.mem.Allocator.Error;

pub const Override = struct {
    name: []const u8,
    value: []const u8,
};

pub const Loaded = struct {
    allocator: std.mem.Allocator,
    profile: features.Profile,
    enabled: []features.Feature,

    pub fn deinit(self: *Loaded) void {
        self.allocator.free(self.enabled);
        self.* = undefined;
    }

    pub fn view(self: *const Loaded) features.Config {
        return .{ .profile = self.profile, .enabled = self.enabled };
    }
};

/// Loads the stable JSON subset of the runtime configuration contract.
/// YAML/TOML must be decoded by an explicit format adapter before this API;
/// this function never guesses a format or silently ignores malformed input.
pub fn loadJson(
    allocator: std.mem.Allocator,
    source: []const u8,
    overrides: []const Override,
) ConfigError!Loaded {
    const parsed = std.json.parseFromSlice(std.json.Value, allocator, source, .{}) catch
        return error.InvalidJson;
    defer parsed.deinit();

    if (parsed.value != .object) return error.InvalidRoot;
    const object = parsed.value.object;

    var profile = features.Profile.minimal;
    if (object.get("profile")) |value| {
        if (value != .string) return error.InvalidProfile;
        profile = std.meta.stringToEnum(features.Profile, value.string) orelse
            return error.InvalidProfile;
    }

    const feature_value = object.get("enabled");
    var selected: []const features.Feature = features.defaults(profile);
    var selected_owned = false;
    if (feature_value) |value| {
        if (value != .array) return error.InvalidFeatures;
        const result = try allocator.alloc(features.Feature, value.array.items.len);
        errdefer allocator.free(result);
        for (value.array.items, 0..) |item, index| {
            if (item != .string) return error.InvalidFeatures;
            result[index] = std.meta.stringToEnum(features.Feature, item.string) orelse
                return error.InvalidFeature;
        }
        selected = result;
        selected_owned = true;
    }

    // Overrides are deliberately supplied by the host. This keeps environment
    // access outside the pure loader and makes precedence deterministic/testable.
    var profile_override: ?features.Profile = null;
    var feature_override: ?[]const u8 = null;
    for (overrides) |override| {
        if (std.mem.eql(u8, override.name, "UBIQ_PROFILE")) {
            profile_override = std.meta.stringToEnum(features.Profile, override.value) orelse
                return error.InvalidOverride;
        } else if (std.mem.eql(u8, override.name, "UBIQ_FEATURES")) {
            feature_override = override.value;
        } else {
            return error.UnknownOverride;
        }
    }

    if (profile_override) |value| {
        profile = value;
        if (feature_value == null and feature_override == null) selected = features.defaults(profile);
    }
    if (feature_override) |csv| {
        var count: usize = 0;
        var iterator = std.mem.splitScalar(u8, csv, ',');
        while (iterator.next()) |name| {
            if (std.mem.trim(u8, name, " \t\r\n").len == 0) return error.InvalidOverride;
            count += 1;
        }
        const result = try allocator.alloc(features.Feature, count);
        errdefer allocator.free(result);
        iterator = std.mem.splitScalar(u8, csv, ',');
        var index: usize = 0;
        while (iterator.next()) |raw_name| {
            const name = std.mem.trim(u8, raw_name, " \t\r\n");
            result[index] = std.meta.stringToEnum(features.Feature, name) orelse
                return error.InvalidOverride;
            index += 1;
        }
        if (selected_owned) allocator.free(@constCast(selected));
        selected = result;
        selected_owned = true;
    }

    const enabled = try allocator.dupe(features.Feature, selected);
    if (selected_owned) allocator.free(@constCast(selected));
    var loaded = Loaded{ .allocator = allocator, .profile = profile, .enabled = enabled };
    loaded.view().validate() catch {
        loaded.deinit();
        return error.InvalidFeatures;
    };
    return loaded;
}

test "JSON loader resolves defaults and applies explicit environment overrides" {
    const allocator = std.testing.allocator;
    var loaded = try loadJson(allocator, "{\"profile\":\"minimal\"}", &.{
        .{ .name = "UBIQ_PROFILE", .value = "secure" },
    });
    defer loaded.deinit();
    try std.testing.expectEqual(features.Profile.secure, loaded.profile);
    try loaded.view().validate();
    try std.testing.expect(loaded.view().isEnabled(.crypto));
}

test "JSON loader rejects unknown features and invalid dependencies" {
    const allocator = std.testing.allocator;
    try std.testing.expectError(error.InvalidFeature, loadJson(
        allocator,
        "{\"profile\":\"custom\",\"enabled\":[\"made_up\"]}",
        &.{},
    ));
    try std.testing.expectError(error.InvalidFeatures, loadJson(
        allocator,
        "{\"profile\":\"custom\",\"enabled\":[\"ubiq\",\"transport_nats\"]}",
        &.{},
    ));
}

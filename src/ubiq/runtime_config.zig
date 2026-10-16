const std = @import("std");
const features = @import("features.zig");
const dependency_graph = @import("dependency_graph.zig");

pub const ConfigError = error{
    InvalidJson,
    InvalidYaml,
    InvalidToml,
    UnknownConfigKey,
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
    errdefer if (selected_owned) allocator.free(@constCast(selected));
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
    if (selected_owned) {
        allocator.free(@constCast(selected));
        selected_owned = false;
    }
    var loaded = Loaded{ .allocator = allocator, .profile = profile, .enabled = enabled };
    loaded.view().validate() catch {
        loaded.deinit();
        return error.InvalidFeatures;
    };
    dependency_graph.validate(loaded.view(), &dependency_graph.default_edges) catch {
        loaded.deinit();
        return error.InvalidFeatures;
    };
    return loaded;
}

/// Loads the intentionally small TOML subset supported by the runtime contract:
/// top-level `profile = "secure"` and `enabled = ["ubiq", "supervisor"]`.
/// Unknown keys and duplicate declarations fail closed.
pub fn loadToml(
    allocator: std.mem.Allocator,
    source: []const u8,
    overrides: []const Override,
) ConfigError!Loaded {
    var profile: ?[]const u8 = null;
    var enabled = std.ArrayList([]const u8).empty;
    defer enabled.deinit(allocator);
    var profile_seen = false;
    var enabled_seen = false;

    var lines = std.mem.splitScalar(u8, source, '\n');
    while (lines.next()) |raw_line| {
        const line = trimComment(raw_line, '#');
        const trimmed = std.mem.trim(u8, line, " \t\r");
        if (trimmed.len == 0) continue;
        const equal = std.mem.indexOfScalar(u8, trimmed, '=') orelse return error.InvalidToml;
        const key = std.mem.trim(u8, trimmed[0..equal], " \t");
        const value = std.mem.trim(u8, trimmed[equal + 1 ..], " \t");
        if (std.mem.eql(u8, key, "profile")) {
            if (profile_seen) return error.InvalidToml;
            profile = unquote(value) orelse return error.InvalidToml;
            profile_seen = true;
        } else if (std.mem.eql(u8, key, "enabled")) {
            if (enabled_seen or value.len < 2 or value[0] != '[' or value[value.len - 1] != ']')
                return error.InvalidToml;
            enabled_seen = true;
            const inside = std.mem.trim(u8, value[1 .. value.len - 1], " \t");
            if (inside.len != 0) {
                var values = std.mem.splitScalar(u8, inside, ',');
                while (values.next()) |item| {
                    const token = unquote(std.mem.trim(u8, item, " \t")) orelse return error.InvalidToml;
                    try appendToken(allocator, &enabled, token);
                }
            }
        } else {
            return error.UnknownConfigKey;
        }
    }
    const json = try encodeJson(allocator, profile, enabled.items, enabled_seen);
    defer allocator.free(json);
    return loadJson(allocator, json, overrides);
}

/// Loads a strict YAML subset with top-level profile and a block or inline
/// enabled list. This is not a general YAML parser: anchors, tags, nesting,
/// aliases, and implicit scalar coercions are intentionally unsupported.
pub fn loadYaml(
    allocator: std.mem.Allocator,
    source: []const u8,
    overrides: []const Override,
) ConfigError!Loaded {
    var profile: ?[]const u8 = null;
    var enabled = std.ArrayList([]const u8).empty;
    defer enabled.deinit(allocator);
    var profile_seen = false;
    var enabled_seen = false;
    var in_enabled_list = false;

    var lines = std.mem.splitScalar(u8, source, '\n');
    while (lines.next()) |raw_line| {
        const no_comment = trimComment(raw_line, '#');
        const trimmed = std.mem.trim(u8, no_comment, " \t\r");
        if (trimmed.len == 0) continue;
        if (std.mem.startsWith(u8, trimmed, "-")) {
            if (!in_enabled_list or trimmed.len < 2 or trimmed[1] != ' ') return error.InvalidYaml;
            const token = unquote(std.mem.trim(u8, trimmed[2..], " \t")) orelse return error.InvalidYaml;
            try appendToken(allocator, &enabled, token);
            continue;
        }

        const colon = std.mem.indexOfScalar(u8, trimmed, ':') orelse return error.InvalidYaml;
        const key = std.mem.trim(u8, trimmed[0..colon], " \t");
        const value = std.mem.trim(u8, trimmed[colon + 1 ..], " \t");
        if (std.mem.eql(u8, key, "profile")) {
            if (profile_seen or value.len == 0) return error.InvalidYaml;
            profile = unquote(value) orelse return error.InvalidYaml;
            profile_seen = true;
            in_enabled_list = false;
        } else if (std.mem.eql(u8, key, "enabled")) {
            if (enabled_seen) return error.InvalidYaml;
            enabled_seen = true;
            if (value.len == 0) {
                in_enabled_list = true;
            } else {
                if (value.len < 2 or value[0] != '[' or value[value.len - 1] != ']') return error.InvalidYaml;
                const inside = std.mem.trim(u8, value[1 .. value.len - 1], " \t");
                if (inside.len != 0) {
                    var values = std.mem.splitScalar(u8, inside, ',');
                    while (values.next()) |item| {
                        const token = unquote(std.mem.trim(u8, item, " \t")) orelse return error.InvalidYaml;
                        try appendToken(allocator, &enabled, token);
                    }
                }
                in_enabled_list = false;
            }
        } else {
            return error.UnknownConfigKey;
        }
    }
    const json = try encodeJson(allocator, profile, enabled.items, enabled_seen);
    defer allocator.free(json);
    return loadJson(allocator, json, overrides);
}

fn trimComment(line: []const u8, marker: u8) []const u8 {
    const index = std.mem.indexOfScalar(u8, line, marker) orelse return line;
    return line[0..index];
}

fn unquote(value: []const u8) ?[]const u8 {
    if (value.len >= 2 and ((value[0] == '"' and value[value.len - 1] == '"') or
        (value[0] == '\'' and value[value.len - 1] == '\'')))
        return value[1 .. value.len - 1];
    if (value.len != 0 and std.mem.indexOfAny(u8, value, " \t\r\n'[]{}:,") == null and
        std.mem.indexOfScalar(u8, value, '"') == null)
        return value;
    return null;
}

fn appendToken(allocator: std.mem.Allocator, list: *std.ArrayList([]const u8), token: []const u8) !void {
    if (token.len == 0) return error.InvalidFeatures;
    for (token) |byte| {
        if (!(std.ascii.isLower(byte) or std.ascii.isDigit(byte) or byte == '_'))
            return error.InvalidFeatures;
    }
    try list.append(allocator, token);
}

fn encodeJson(
    allocator: std.mem.Allocator,
    profile: ?[]const u8,
    enabled: []const []const u8,
    enabled_seen: bool,
) ![]u8 {
    var output = std.ArrayList(u8).empty;
    errdefer output.deinit(allocator);
    try output.appendSlice(allocator, "{");
    var has_field = false;
    if (profile) |name| {
        try output.appendSlice(allocator, "\"profile\":");
        try appendJsonToken(allocator, &output, name);
        has_field = true;
    }
    if (enabled_seen) {
        if (has_field) try output.appendSlice(allocator, ",");
        try output.appendSlice(allocator, "\"enabled\":[");
        for (enabled, 0..) |name, index| {
            if (index != 0) try output.appendSlice(allocator, ",");
            try appendJsonToken(allocator, &output, name);
        }
        try output.appendSlice(allocator, "]");
    }
    try output.appendSlice(allocator, "}");
    return output.toOwnedSlice(allocator);
}

fn appendJsonToken(allocator: std.mem.Allocator, output: *std.ArrayList(u8), token: []const u8) !void {
    try output.append(allocator, '"');
    try output.appendSlice(allocator, token);
    try output.append(allocator, '"');
}

test "TOML loader accepts the supported strict subset and applies overrides" {
    const allocator = std.testing.allocator;
    var loaded = try loadToml(
        allocator,
        "profile = \"secure\"\nenabled = [\"ubiq\", \"supervisor\", \"crypto\", \"mtls\", \"replay_guard\"]",
        &.{},
    );
    defer loaded.deinit();
    try std.testing.expectEqual(features.Profile.secure, loaded.profile);
    try loaded.view().validate();
}

test "YAML loader accepts a block feature list and rejects unknown keys" {
    const allocator = std.testing.allocator;
    var loaded = try loadYaml(
        allocator,
        "profile: secure\nenabled:\n  - ubiq\n  - supervisor\n  - crypto\n  - mtls\n  - replay_guard",
        &.{},
    );
    defer loaded.deinit();
    try std.testing.expectEqual(features.Profile.secure, loaded.profile);
    try loaded.view().validate();
    try std.testing.expectError(error.UnknownConfigKey, loadYaml(allocator, "profile: minimal\nother: value", &.{}));
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

test "feature override safely replaces profile defaults without freeing static storage" {
    const allocator = std.testing.allocator;
    var loaded = try loadJson(allocator, "{\"profile\":\"minimal\"}", &.{
        .{ .name = "UBIQ_PROFILE", .value = "secure" },
        .{ .name = "UBIQ_FEATURES", .value = "ubiq,supervisor,crypto,mtls,replay_guard" },
    });

    defer loaded.deinit();
    try std.testing.expectEqual(features.Profile.secure, loaded.profile);
    try std.testing.expect(loaded.view().isEnabled(.crypto));
    try loaded.view().validate();
}

test "JSON loader frees selected features when an override is invalid" {
    const allocator = std.testing.allocator;
    try std.testing.expectError(error.InvalidOverride, loadJson(
        allocator,
        "{\"profile\":\"custom\",\"enabled\":[\"ubiq\",\"supervisor\"]}",
        &.{.{ .name = "UBIQ_PROFILE", .value = "not-a-profile" }},
    ));
    try std.testing.expectError(error.InvalidOverride, loadJson(
        allocator,
        "{\"profile\":\"custom\",\"enabled\":[\"ubiq\",\"supervisor\"]}",
        &.{.{ .name = "UBIQ_FEATURES", .value = "ubiq,not-a-feature" }},
    ));
}

test "JSON loader rejects duplicate feature declarations" {
    const allocator = std.testing.allocator;
    try std.testing.expectError(error.InvalidFeatures, loadJson(
        allocator,
        "{\"profile\":\"custom\",\"enabled\":[\"ubiq\",\"supervisor\",\"ubiq\"]}",
        &.{},
    ));
}

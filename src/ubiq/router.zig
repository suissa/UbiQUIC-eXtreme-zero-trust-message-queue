const std = @import("std");
const capability = @import("capability.zig");

pub const RoutingError = error{
    NoEligibleTransport,
    InvalidRequirement,
    InvalidCandidate,
};

pub const Requirements = struct {
    capabilities: capability.Requirements = .{},
    max_payload_bytes: usize = 0,
    require_security_profile: bool = false,
    require_mtls: bool = false,
    require_proof_of_possession: bool = false,
    require_replay_protection: bool = false,
    hard_deadline_ms: u64 = 0,
    semantic_address: []const u8 = "",
};

pub const Metrics = struct {
    latency_ms: u64 = 0,
    queue_depth: u32 = 0,
    cost_microunits: u64 = 0,
    available: bool = true,
    freshness_ms: u64 = 0,
};

pub const Candidate = struct {
    transport: capability.Transport,
    metrics: Metrics = .{},
    security_ok: bool = true,
    payload_limit: usize = 0,
    priority: u16 = 0,
};

pub const Policy = struct {
    prefer_low_latency: bool = true,
    prefer_low_cost: bool = false,
    max_queue_depth: u32 = 0,
    max_metric_age_ms: u64 = 0,
    hysteresis_ms: u64 = 0,
};

pub const Decision = struct {
    transport: capability.Transport,
    score: u128,
    considered: usize,
    reason: []const u8,
};

fn eligible(candidate: Candidate, requirements: Requirements, now_age_ms: u64, policy: Policy) bool {
    if (!candidate.metrics.available or !candidate.security_ok) return false;
    if (policy.max_queue_depth != 0 and candidate.metrics.queue_depth > policy.max_queue_depth) return false;
    if (policy.max_metric_age_ms != 0 and now_age_ms > policy.max_metric_age_ms) return false;
    if (requirements.max_payload_bytes != 0 and
        (candidate.payload_limit == 0 or candidate.payload_limit < requirements.max_payload_bytes)) return false;
    if (!capability.profile(candidate.transport).capabilities.satisfies(requirements.capabilities)) return false;
    if (requirements.require_security_profile and !candidate.security_ok) return false;
    if (requirements.require_mtls or requirements.require_proof_of_possession or requirements.require_replay_protection) {
        if (!candidate.security_ok) return false;
    }
    var considered: usize = 0;
    var selected: ?Candidate = null;
    var selected_score: u128 = 0;
    for (candidates) |candidate| {
        considered += 1;
        if (!eligible(candidate, requirements, now_age_ms, policy)) continue;
        const candidate_score = score(candidate, policy);
        if (selected == null or candidate_score > selected_score) {
            selected = candidate;
            selected_score = candidate_score;
        }
    }
    const winner = selected orelse return error.NoEligibleTransport;
    return .{
        .transport = winner.transport,
        .score = selected_score,
        .considered = considered,
        .reason = "required capabilities/security passed; policy ranking selected candidate",
    };
}

test "selection rejects insecure fallback before cost ranking" {
    const candidates = [_]Candidate{
        .{ .transport = .quic, .metrics = .{ .latency_ms = 5, .cost_microunits = 100 }, .security_ok = false },
        .{ .transport = .nats_jetstream, .metrics = .{ .latency_ms = 20, .cost_microunits = 1 }, .security_ok = true },
    };
    const result = try select(&candidates, .{ .capabilities = .{ .durable = true }, .require_security_profile = true }, .{}, 0);
    try std.testing.expectEqual(capability.Transport.nats_jetstream, result.transport);
}

test "hard deadline is filtered before latency ranking" {
    const candidates = [_]Candidate{
        .{ .transport = .nats_jetstream, .metrics = .{ .latency_ms = 50 }, .security_ok = true },
        .{ .transport = .quic, .metrics = .{ .latency_ms = 5 }, .security_ok = true },
    };
    const result = try select(&candidates, .{ .hard_deadline_ms = 10 }, .{}, 0);
    try std.testing.expectEqual(capability.Transport.quic, result.transport);
}

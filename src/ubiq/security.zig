const std = @import("std");
const capability = @import("capability.zig");

pub const ProofOfPossession = enum {
    none,
    dpop_http,
    ubiq_pop,
};

pub const KeyEstablishment = enum {
    x25519,
    hybrid_x25519_ml_kem,
};

pub const Signature = enum {
    ed25519,
    ml_dsa,
    hybrid_ed25519_ml_dsa,
};

pub const Aead = enum {
    xchacha20_poly1305,
    aes_gcm_siv,
};

pub const SecurityProfile = struct {
    peer_mtls: bool = true,
    proof_of_possession: ProofOfPossession = .ubiq_pop,
    key_establishment: KeyEstablishment = .hybrid_x25519_ml_kem,
    signature: Signature = .hybrid_ed25519_ml_dsa,
    aead: Aead = .xchacha20_poly1305,
    replay_protection: bool = true,
    linear_auto_destroy: bool = true,

    pub fn validateForTransport(self: SecurityProfile, transport: capability.Transport) bool {
        if (self.proof_of_possession == .dpop_http) {
            return transport == .rest or transport == .websocket or transport == .sse;
        }
        return true;
    }
};

pub const LinearCapabilityError = error{
    Expired,
    AlreadyConsumed,
    WrongHolder,
};

/// LinearAutoDestroy is modeled as a one-shot execution capability.
/// Audit evidence is not deleted; only the authority to execute is consumed.
pub const LinearCapability = struct {
    event_id: []const u8,
    holder: []const u8,
    expires_at_ms: u64,
    consumed: bool = false,

    pub fn consume(self: *LinearCapability, holder: []const u8, now_ms: u64) LinearCapabilityError!void {
        if (self.consumed) return error.AlreadyConsumed;
        if (now_ms >= self.expires_at_ms) return error.Expired;
        if (!std.mem.eql(u8, self.holder, holder)) return error.WrongHolder;
        self.consumed = true;
    }
};

/// Cryptographic primitives intentionally stay behind a provider boundary.
/// The runtime MUST NOT emulate ML-KEM/ML-DSA/DPoP by hashing labels or payloads.
/// A production provider must supply reviewed implementations and real keys.
pub const CryptoProvider = struct {
    name: []const u8,
    supports_ml_kem: bool,
    supports_ml_dsa: bool,
    supports_mtls: bool,
    supports_message_signing: bool,

    pub fn satisfies(self: CryptoProvider, profile: SecurityProfile) bool {
        if (profile.peer_mtls and !self.supports_mtls) return false;
        if (profile.key_establishment == .hybrid_x25519_ml_kem and !self.supports_ml_kem) return false;
        if ((profile.signature == .ml_dsa or profile.signature == .hybrid_ed25519_ml_dsa) and !self.supports_ml_dsa) return false;
        if (!self.supports_message_signing) return false;
        return true;
    }
};

test "DPoP remains HTTP-bound while UbiQ PoP is transport-neutral" {
    const dpop = SecurityProfile{ .proof_of_possession = .dpop_http };
    try std.testing.expect(dpop.validateForTransport(.rest));
    try std.testing.expect(!dpop.validateForTransport(.kafka));

    const ubiq_pop = SecurityProfile{ .proof_of_possession = .ubiq_pop };
    try std.testing.expect(ubiq_pop.validateForTransport(.kafka));
    try std.testing.expect(ubiq_pop.validateForTransport(.quic));
}

test "linear capability can be consumed only once" {
    var token = LinearCapability{
        .event_id = "evt-1",
        .holder = "worker-a",
        .expires_at_ms = 100,
    };
    try token.consume("worker-a", 10);
    try std.testing.expectError(error.AlreadyConsumed, token.consume("worker-a", 11));
}

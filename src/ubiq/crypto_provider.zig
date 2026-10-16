const std = @import("std");
const security = @import("security.zig");

pub const Error = error{
    MissingProvider,
    MissingX25519,
    MissingMlKem,
    MissingEd25519,
    MissingMlDsa,
    MissingAead,
    MissingPeerIdentityVerifier,
    MissingUbiqPopVerifier,
    MissingDpopVerifier,
    MissingSecretDestroyer,
    MissingKeyRotation,
    InvalidProviderName,
};

pub const KeyAgreementFn = *const fn (context: *anyopaque, private_key: []const u8, peer_public_key: []const u8, shared_secret_out: []u8) anyerror!usize;
pub const KemEncapsulateFn = *const fn (context: *anyopaque, public_key: []const u8, ciphertext_out: []u8, shared_secret_out: []u8) anyerror!struct { ciphertext_len: usize, secret_len: usize };
pub const KemDecapsulateFn = *const fn (context: *anyopaque, private_key: []const u8, ciphertext: []const u8, shared_secret_out: []u8) anyerror!usize;
pub const SignFn = *const fn (context: *anyopaque, private_key: []const u8, message: []const u8, signature_out: []u8) anyerror!usize;
pub const VerifyFn = *const fn (context: *anyopaque, public_key: []const u8, message: []const u8, signature: []const u8) anyerror!void;
pub const SealFn = *const fn (context: *anyopaque, key: []const u8, nonce: []const u8, aad: []const u8, plaintext: []const u8, ciphertext_out: []u8) anyerror!usize;
pub const OpenFn = *const fn (context: *anyopaque, key: []const u8, nonce: []const u8, aad: []const u8, ciphertext: []const u8, plaintext_out: []u8) anyerror!usize;
pub const VerifyPeerFn = *const fn (context: *anyopaque, peer_identity: []const u8, channel_binding: []const u8) anyerror!void;
pub const VerifyProofFn = *const fn (
    context: *anyopaque,
    proof: []const u8,
    message: []const u8,
    holder_identity: []const u8,
    channel_binding: []const u8,
    now_ms: u64,
) anyerror!void;
pub const DestroySecretFn = *const fn (context: *anyopaque, secret: []u8) void;
pub const RotateKeyFn = *const fn (context: *anyopaque, key_id: []const u8) anyerror!void;

/// Cryptographic operations must be backed by a reviewed implementation.
/// A provider cannot advertise an algorithm merely by setting a capability
/// boolean: validation checks that its operation is actually wired.
pub const VTable = struct {
    x25519: ?KeyAgreementFn = null,
    ml_kem_encapsulate: ?KemEncapsulateFn = null,
    ml_kem_decapsulate: ?KemDecapsulateFn = null,
    ed25519_sign: ?SignFn = null,
    ed25519_verify: ?VerifyFn = null,
    ml_dsa_sign: ?SignFn = null,
    ml_dsa_verify: ?VerifyFn = null,
    xchacha20_poly1305_seal: ?SealFn = null,
    xchacha20_poly1305_open: ?OpenFn = null,
    aes_gcm_siv_seal: ?SealFn = null,
    aes_gcm_siv_open: ?OpenFn = null,
    verify_peer_identity: ?VerifyPeerFn = null,
    verify_ubiq_pop: ?VerifyProofFn = null,
    verify_dpop: ?VerifyProofFn = null,
    destroy_secret: ?DestroySecretFn = null,
    rotate_key: ?RotateKeyFn = null,
};

pub const Provider = struct {
    name: []const u8,
    context: *anyopaque,
    vtable: VTable,

    pub fn validate(self: Provider, profile: security.SecurityProfile) Error!void {
        if (self.name.len == 0) return error.InvalidProviderName;
        if (self.vtable.verify_peer_identity == null) return error.MissingPeerIdentityVerifier;
        if (self.vtable.x25519 == null) return error.MissingX25519;
        if (self.vtable.rotate_key == null) return error.MissingKeyRotation;
        if (profile.linear_auto_destroy and self.vtable.destroy_secret == null)
            return error.MissingSecretDestroyer;
        switch (profile.proof_of_possession) {
            .none => {},
            .ubiq_pop => if (self.vtable.verify_ubiq_pop == null) return error.MissingUbiqPopVerifier,
            .dpop_http => if (self.vtable.verify_dpop == null) return error.MissingDpopVerifier,
        }
        if (profile.key_establishment == .hybrid_x25519_ml_kem and
            (self.vtable.ml_kem_encapsulate == null or self.vtable.ml_kem_decapsulate == null))
            return error.MissingMlKem;

        switch (profile.signature) {
            .ed25519 => {
                if (self.vtable.ed25519_sign == null or self.vtable.ed25519_verify == null)
                    return error.MissingEd25519;
            },
            .ml_dsa => {
                if (self.vtable.ml_dsa_sign == null or self.vtable.ml_dsa_verify == null)
                    return error.MissingMlDsa;
            },
            .hybrid_ed25519_ml_dsa => {
                if (self.vtable.ed25519_sign == null or self.vtable.ed25519_verify == null)
                    return error.MissingEd25519;
                if (self.vtable.ml_dsa_sign == null or self.vtable.ml_dsa_verify == null)
                    return error.MissingMlDsa;
            },
        }

        switch (profile.aead) {
            .xchacha20_poly1305 => {
                if (self.vtable.xchacha20_poly1305_seal == null or
                    self.vtable.xchacha20_poly1305_open == null) return error.MissingAead;
            },
            .aes_gcm_siv => {
                if (self.vtable.aes_gcm_siv_seal == null or
                    self.vtable.aes_gcm_siv_open == null) return error.MissingAead;
            },
        }
    }
};

test "provider refuses a security profile when no real operations are wired" {
    var context: u8 = 0;
    const provider = Provider{
        .name = "unconfigured",
        .context = @ptrCast(&context),
        .vtable = .{},
    };
    try std.testing.expectError(error.MissingPeerIdentityVerifier, provider.validate(.{}));
}

test "provider rejects empty names before reporting algorithm capabilities" {
    var context: u8 = 0;
    const provider = Provider{
        .name = "",
        .context = @ptrCast(&context),
        .vtable = .{},
    };
    try std.testing.expectError(error.InvalidProviderName, provider.validate(.{}));
}

test "default profile requires real UbiQ PoP verification before accepting a provider" {
    var context: u8 = 0;
    const provider = Provider{
        .name = "classical-only",
        .context = @ptrCast(&context),
        .vtable = .{
            .verify_peer_identity = struct {
                fn call(_: *anyopaque, _: []const u8, _: []const u8) anyerror!void {}
            }.call,
            .x25519 = struct {
                fn call(_: *anyopaque, _: []const u8, _: []const u8, _: []u8) anyerror!usize {
                    return error.Unsupported;
                }
            }.call,
            .rotate_key = struct {
                fn call(_: *anyopaque, _: []const u8) anyerror!void {}
            }.call,
            .destroy_secret = struct {
                fn call(_: *anyopaque, secret: []u8) void {
                    @memset(secret, 0);
                }
            }.call,
        },
    };
    try std.testing.expectError(error.MissingUbiqPopVerifier, provider.validate(.{}));
}

test "HTTP DPoP profile cannot validate without a dedicated DPoP verifier" {
    var context: u8 = 0;
    const provider = Provider{
        .name = "no-dpop",
        .context = @ptrCast(&context),
        .vtable = .{
            .verify_peer_identity = struct {
                fn call(_: *anyopaque, _: []const u8, _: []const u8) anyerror!void {}
            }.call,
            .x25519 = struct {
                fn call(_: *anyopaque, _: []const u8, _: []const u8, _: []u8) anyerror!usize {
                    return error.Unsupported;
                }
            }.call,
            .rotate_key = struct {
                fn call(_: *anyopaque, _: []const u8) anyerror!void {}
            }.call,
            .destroy_secret = struct {
                fn call(_: *anyopaque, secret: []u8) void {
                    @memset(secret, 0);
                }
            }.call,
        },
    };
    const profile = security.SecurityProfile{ .proof_of_possession = .dpop_http };
    try std.testing.expectError(error.MissingDpopVerifier, provider.validate(profile));
}

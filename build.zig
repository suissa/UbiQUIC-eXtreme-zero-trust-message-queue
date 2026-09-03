const std = @import("std");

pub fn build(b: *std.Build) void {
    const target = b.standardTargetOptions(.{});
    const optimize = b.standardOptimizeOption(.{});

    const exe = b.addExecutable(.{
        .name = "ubiq",
        .root_module = b.createModule(.{
            .root_source_file = b.path("src/main.zig"),
            .target = target,
            .optimize = optimize,
        }),
    });
    b.installArtifact(exe);

    const run_cmd = b.addRunArtifact(exe);
    run_cmd.step.dependOn(b.getInstallStep());
    if (b.args) |args| run_cmd.addArgs(args);

    const run_step = b.step("run", "Run the UbiQ semantic runtime demo");
    run_step.dependOn(&run_cmd.step);

    const tests = b.addTest(.{
        .root_module = b.createModule(.{
            .root_source_file = b.path("src/ubiq/root.zig"),
            .target = target,
            .optimize = optimize,
        }),
    });
    const run_tests = b.addRunArtifact(tests);
    const test_step = b.step("test", "Run UbiQ semantic runtime tests");
    test_step.dependOn(&run_tests.step);

    const nats_integration_exe = b.addExecutable(.{
        .name = "ubiq-nats-integration",
        .root_module = b.createModule(.{
            .root_source_file = b.path("src/integration/nats_jetstream.zig"),
            .target = target,
            .optimize = optimize,
        }),
    });
    const nats_integration_run = b.addRunArtifact(nats_integration_exe);
    const nats_integration_step = b.step("nats-integration", "Run live NATS Core and JetStream integration test");
    nats_integration_step.dependOn(&nats_integration_run.step);
}

const std = @import("std");

pub fn build(b: *std.Build) void {
    const target = b.standardTargetOptions(.{});
    const optimize = b.standardOptimizeOption(.{});

    // Producer executable
    const producer = b.addExecutable(.{
        .name = "producer",
        .root_module = b.createModule(.{
            .root_source_file = b.path("src/producer.zig"),
            .target = target,
            .optimize = optimize,
        }),
    });
    b.installArtifact(producer);

    // Worker executable
    const worker = b.addExecutable(.{
        .name = "worker",
        .root_module = b.createModule(.{
            .root_source_file = b.path("src/worker.zig"),
            .target = target,
            .optimize = optimize,
        }),
    });
    b.installArtifact(worker);

    // Benchmark Producer executable
    const bench_producer = b.addExecutable(.{
        .name = "benchmark-producer",
        .root_module = b.createModule(.{
            .root_source_file = b.path("src/benchmark_producer.zig"),
            .target = target,
            .optimize = optimize,
        }),
    });
    b.installArtifact(bench_producer);

    // Run steps
    const run_producer_cmd = b.addRunArtifact(producer);
    run_producer_cmd.step.dependOn(b.getInstallStep());
    if (b.args) |args| {
        run_producer_cmd.addArgs(args);
    }
    const run_producer_step = b.step("run-producer", "Run the producer");
    run_producer_step.dependOn(&run_producer_cmd.step);

    const run_worker_cmd = b.addRunArtifact(worker);
    run_worker_cmd.step.dependOn(b.getInstallStep());
    if (b.args) |args| {
        run_worker_cmd.addArgs(args);
    }
    const run_worker_step = b.step("run-worker", "Run the worker");
    run_worker_step.dependOn(&run_worker_cmd.step);

    const run_bench_producer_cmd = b.addRunArtifact(bench_producer);
    run_bench_producer_cmd.step.dependOn(b.getInstallStep());
    if (b.args) |args| {
        run_bench_producer_cmd.addArgs(args);
    }
    const run_bench_producer_step = b.step("run-benchmark-producer", "Run the benchmark producer");
    run_bench_producer_step.dependOn(&run_bench_producer_cmd.step);
}

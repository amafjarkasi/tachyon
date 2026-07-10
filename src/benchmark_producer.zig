const std = @import("std");
const NatsClient = @import("nats_client.zig").NatsClient;
const Config = @import("nats_client.zig").Config;

const Job = struct {
    id: []const u8,
    email: []const u8,
    subject: []const u8,
    body: []const u8,
};

pub fn main(init: std.process.Init) !void {
    const io = init.io;
    var gpa = std.heap.DebugAllocator(.{}){};
    defer _ = gpa.deinit();
    const allocator = gpa.allocator();

    // Default connection configuration
    var config = Config{};

    // Resolve connection config from environment variables (Juicy Main)
    if (init.environ_map.get("NATS_HOST")) |val| config.host = val;
    if (init.environ_map.get("NATS_PORT")) |val| {
        config.port = try std.fmt.parseInt(u16, val, 10);
    }
    if (init.environ_map.get("NATS_USER")) |val| config.username = val;
    if (init.environ_map.get("NATS_PASS")) |val| config.password = val;
    if (init.environ_map.get("NATS_TLS")) |val| {
        config.use_tls = std.mem.eql(u8, val, "true");
    }

    // Default values
    var count: usize = 50000;

    // Parse command line arguments
    const args = try init.minimal.args.toSlice(init.arena.allocator());
    var i_arg: usize = 1;
    while (i_arg < args.len) : (i_arg += 1) {
        if (std.mem.eql(u8, args[i_arg], "--jobs") or std.mem.eql(u8, args[i_arg], "-j")) {
            if (i_arg + 1 < args.len) {
                i_arg += 1;
                count = try std.fmt.parseInt(usize, args[i_arg], 10);
            }
        }
    }

    std.debug.print("Connecting to NATS server...\n", .{});
    var client = try NatsClient.connect(io, allocator, config);
    defer client.deinit();
    std.debug.print("Connected!\n", .{});

    // Ensure Stream and Consumers exist
    try client.setupJetStream("JOBS", &[_][]const u8{ "jobs.high.*", "jobs.low.*" }, 0);
    try client.setupConsumer("JOBS", "WORKER_HIGH", "jobs.high.*", 5);
    try client.setupConsumer("JOBS", "WORKER_LOW", "jobs.low.*", 5);
    try client.flush();

    // Prepare JSON payload
    const my_job = Job{
        .id = "job_bench",
        .email = "bench@example.com",
        .subject = "Antigravity Load Test",
        .body = "Fast background job processing load testing with raw TCP socket.",
    };

    var payload_buf = std.Io.Writer.Allocating.init(allocator);
    defer payload_buf.deinit();
    try payload_buf.writer.print("{f}", .{std.json.fmt(my_job, .{})});
    const payload = try payload_buf.toOwnedSlice();
    defer allocator.free(payload);

    std.debug.print("Starting benchmark enqueuing of {d} jobs...\n", .{count});

    const start_time = std.Io.Timestamp.now(io, .awake);

    var i: usize = 0;
    while (i < count) : (i += 1) {
        if (i % 5 == 0) {
            try client.publish("jobs.low.email", null, payload);
        } else {
            try client.publish("jobs.high.email", null, payload);
        }
    }

    const end_time = std.Io.Timestamp.now(io, .awake);
    const elapsed_duration = start_time.durationTo(end_time);
    const elapsed_ms = @as(f64, @floatFromInt(elapsed_duration.toMilliseconds()));
    const rate = @as(f64, @floatFromInt(count)) / (elapsed_ms / 1000.0);

    std.debug.print("Enqueued {d} jobs in {d:.2} ms ({d:.2} jobs/sec)\n", .{
        count,
        elapsed_ms,
        rate,
    });
}

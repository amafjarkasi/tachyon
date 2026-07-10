const std = @import("std");
const NatsClient = @import("nats_client.zig").NatsClient;
const Config = @import("nats_client.zig").Config;

pub fn main(init: std.process.Init) !void {
    const io = init.io;
    var gpa = std.heap.DebugAllocator(.{}){};
    defer _ = gpa.deinit();
    const allocator = gpa.allocator();

    var config = Config{};

    if (init.environ_map.get("NATS_HOST")) |val| config.host = val;
    if (init.environ_map.get("NATS_PORT")) |val| {
        config.port = try std.fmt.parseInt(u16, val, 10);
    }
    if (init.environ_map.get("NATS_USER")) |val| config.username = val;
    if (init.environ_map.get("NATS_PASS")) |val| config.password = val;
    if (init.environ_map.get("NATS_TLS")) |val| {
        config.use_tls = std.mem.eql(u8, val, "true");
    }

    // Default: all HIGH (best for single_consumer_mode benches).
    // Set BENCH_SPLIT_LOW=true for classic 80/20 high/low priority mix.
    const split_low = if (init.environ_map.get("BENCH_SPLIT_LOW")) |v|
        std.mem.eql(u8, v, "true") or std.mem.eql(u8, v, "1")
    else
        false;

    var count: usize = 50000;

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

    try client.setupJetStream("JOBS", &[_][]const u8{ "jobs.high.*", "jobs.low.*" }, 0);
    try client.setupConsumer("JOBS", "WORKER_HIGH", "jobs.high.*", 5);
    try client.setupConsumer("JOBS", "WORKER_LOW", "jobs.low.*", 5);
    try client.flush();

    // Unique job.id per message so in-process dedup does not skip work.
    const prefix = "{\"id\":\"job_";
    const suffix = "\",\"email\":\"bench@example.com\",\"subject\":\"Antigravity Load Test\",\"body\":\"Fast background job processing load testing with raw TCP socket.\"}";
    var id_digits: [12]u8 = undefined;
    var payload_buf: [256]u8 = undefined;
    @memcpy(payload_buf[0..prefix.len], prefix);
    const id_off = prefix.len;
    const suffix_off = id_off + 12;
    @memcpy(payload_buf[suffix_off .. suffix_off + suffix.len], suffix);
    const payload_len = suffix_off + suffix.len;

    if (split_low) {
        std.debug.print("Starting benchmark enqueuing of {d} jobs (80%% high / 20%% low)...\n", .{count});
    } else {
        std.debug.print("Starting benchmark enqueuing of {d} jobs (100%% high)...\n", .{count});
    }

    const start_time = std.Io.Timestamp.now(io, .awake);

    var i: usize = 0;
    while (i < count) : (i += 1) {
        var n = i;
        var d: usize = 12;
        while (d > 0) {
            d -= 1;
            id_digits[d] = '0' + @as(u8, @intCast(n % 10));
            n /= 10;
        }
        @memcpy(payload_buf[id_off .. id_off + 12], &id_digits);
        const payload = payload_buf[0..payload_len];

        // Buffer publishes; flush every 64 to cut syscall overhead.
        if (split_low and i % 5 == 0) {
            try client.publishRaw("jobs.low.email", null, payload, false);
        } else {
            try client.publishRaw("jobs.high.email", null, payload, false);
        }
        if ((i + 1) % 64 == 0) {
            try client.flushWrites();
        }
    }
    try client.flushWrites();

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

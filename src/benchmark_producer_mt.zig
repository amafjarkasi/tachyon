const std = @import("std");
const NatsClient = @import("nats_client.zig").NatsClient;
const Config = @import("nats_client.zig").Config;

const PubCtx = struct {
    io: std.Io,
    allocator: std.mem.Allocator,
    config: Config,
    start_id: usize,
    n: usize,
};

/// Multi-connection benchmark publisher.
/// Usage: benchmark-producer-mt --jobs 150000 --publishers 4
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

    var count: usize = 50000;
    var publishers: usize = 4;

    const args = try init.minimal.args.toSlice(init.arena.allocator());
    var i_arg: usize = 1;
    while (i_arg < args.len) : (i_arg += 1) {
        if ((std.mem.eql(u8, args[i_arg], "--jobs") or std.mem.eql(u8, args[i_arg], "-j")) and i_arg + 1 < args.len) {
            i_arg += 1;
            count = try std.fmt.parseInt(usize, args[i_arg], 10);
        } else if ((std.mem.eql(u8, args[i_arg], "--publishers") or std.mem.eql(u8, args[i_arg], "-p")) and i_arg + 1 < args.len) {
            i_arg += 1;
            publishers = try std.fmt.parseInt(usize, args[i_arg], 10);
        }
    }
    if (publishers == 0) publishers = 1;

    {
        var client = try NatsClient.connect(io, allocator, config);
        defer client.deinit();
        try client.setupJetStream("JOBS", &[_][]const u8{ "jobs.high.*", "jobs.low.*" }, 0);
        try client.setupConsumer("JOBS", "WORKER_HIGH", "jobs.high.*", 5);
        try client.setupConsumer("JOBS", "WORKER_LOW", "jobs.low.*", 5);
        try client.flush();
    }

    const per = count / publishers;
    const rem = count % publishers;

    var threads = try allocator.alloc(std.Thread, publishers);
    defer allocator.free(threads);

    std.debug.print("Publishing {d} jobs via {d} connections...\n", .{ count, publishers });
    const t0 = std.Io.Timestamp.now(io, .awake);

    var p: usize = 0;
    var id_base: usize = 0;
    while (p < publishers) : (p += 1) {
        const n = per + if (p == 0) rem else 0;
        const ctx = try allocator.create(PubCtx);
        ctx.* = .{
            .io = io,
            .allocator = allocator,
            .config = config,
            .start_id = id_base,
            .n = n,
        };
        id_base += n;
        threads[p] = try std.Thread.spawn(.{}, publishWorker, .{ctx});
    }
    for (threads) |t| t.join();

    const elapsed_ms = @as(f64, @floatFromInt(t0.durationTo(std.Io.Timestamp.now(io, .awake)).toMilliseconds()));
    const rate = @as(f64, @floatFromInt(count)) / (elapsed_ms / 1000.0);
    std.debug.print("Enqueued {d} jobs in {d:.2} ms ({d:.2} jobs/sec) with {d} publishers\n", .{ count, elapsed_ms, rate, publishers });
}

fn publishWorker(ctx: *PubCtx) void {
    defer ctx.allocator.destroy(ctx);

    var client = NatsClient.connect(ctx.io, ctx.allocator, ctx.config) catch return;
    defer client.deinit();

    const prefix = "{\"id\":\"job_";
    const suffix = "\",\"email\":\"bench@example.com\",\"subject\":\"Antigravity Load Test\",\"body\":\"Fast background job processing load testing with raw TCP socket.\"}";
    var id_digits: [12]u8 = undefined;
    var payload_buf: [256]u8 = undefined;
    @memcpy(payload_buf[0..prefix.len], prefix);
    const id_off = prefix.len;
    const suffix_off = id_off + 12;
    @memcpy(payload_buf[suffix_off .. suffix_off + suffix.len], suffix);
    const payload_len = suffix_off + suffix.len;

    var i: usize = 0;
    while (i < ctx.n) : (i += 1) {
        var n = ctx.start_id + i;
        var d: usize = 12;
        while (d > 0) {
            d -= 1;
            id_digits[d] = '0' + @as(u8, @intCast(n % 10));
            n /= 10;
        }
        @memcpy(payload_buf[id_off .. id_off + 12], &id_digits);
        const payload = payload_buf[0..payload_len];
        if (i % 5 == 0) {
            client.publish("jobs.low.email", null, payload) catch break;
        } else {
            client.publish("jobs.high.email", null, payload) catch break;
        }
    }
}

const std = @import("std");
const NatsClient = @import("nats_client.zig").NatsClient;
const Config = @import("nats_client.zig").Config;

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

    std.debug.print("Connecting to NATS server...\n", .{});
    var client = try NatsClient.connect(io, allocator, config);
    defer client.deinit();
    std.debug.print("Connected!\n", .{});

    // Setup JetStream stream & consumers
    std.debug.print("Initializing JetStream Stream and Consumers...\n", .{});
    try client.setupJetStream("JOBS", &[_][]const u8{ "jobs.high.*", "jobs.low.*" }, 0);
    try client.setupConsumer("JOBS", "WORKER_HIGH", "jobs.high.*", 5);
    try client.setupConsumer("JOBS", "WORKER_LOW", "jobs.low.*", 5);
    try client.flush();

    // Create a job payload
    const Job = struct {
        id: []const u8,
        email: []const u8,
        subject: []const u8,
        body: []const u8,
    };

    const my_job = Job{
        .id = "job_12345",
        .email = "hello@example.com",
        .subject = "Welcome to Antigravity!",
        .body = "This is a background job processed entirely in Zig via NATS JetStream.",
    };

    var payload_buf = std.Io.Writer.Allocating.init(allocator);
    defer payload_buf.deinit();
    try payload_buf.writer.print("{f}", .{std.json.fmt(my_job, .{})});
    const payload = try payload_buf.toOwnedSlice();
    defer allocator.free(payload);

    std.debug.print("Enqueueing job: {s}\n", .{my_job.id});
    // HPUB with Nats-Msg-Id enables broker-side dedup when stream is configured for it;
    // workers also dedupe in-process by job.id.
    var id_hdr_buf: [64]u8 = undefined;
    const id_hdr = try std.fmt.bufPrint(&id_hdr_buf, "Nats-Msg-Id: {s}", .{my_job.id});
    try client.publishWithHeaders("jobs.high.email", null, &[_][]const u8{id_hdr}, payload);
    std.debug.print("Job enqueued successfully!\n", .{});
}

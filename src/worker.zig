const std = @import("std");
const builtin = @import("builtin");
const NatsClient = @import("nats_client.zig").NatsClient;
const Config = @import("nats_client.zig").Config;

const windows = std.os.windows;

// Windows SetConsoleCtrlHandler declaration
extern "kernel32" fn SetConsoleCtrlHandler(
    HandlerRoutine: ?*const fn (windows.DWORD) callconv(std.builtin.CallingConvention.winapi) windows.BOOL,
    Add: windows.BOOL,
) callconv(std.builtin.CallingConvention.winapi) windows.BOOL;

const Job = struct {
    id: []const u8,
    email: []const u8,
    subject: []const u8,
    body: []const u8,
};

const AppConfig = struct {
    nats_host: []const u8 = "127.0.0.1",
    nats_port: u16 = 4222,
    nats_user: ?[]const u8 = null,
    nats_pass: ?[]const u8 = null,
    nats_tls: bool = false,
    nats_ca_path: ?[]const u8 = null,
    worker_threads: usize = 4,
    worker_batch: usize = 50,
};

// Global atomic variables
var total_jobs = std.atomic.Value(usize).init(0);
var should_shutdown = std.atomic.Value(bool).init(false);
var target_threads = std.atomic.Value(usize).init(4);

const WorkerContext = struct {
    io: std.Io,
    allocator: std.mem.Allocator,
    thread_id: usize,
    batch_size: usize,
    config: Config,
};

const MetricsContext = struct {
    io: std.Io,
    allocator: std.mem.Allocator,
    total_jobs: *std.atomic.Value(usize),
};

const CTRL_C_EVENT = 0;
const CTRL_BREAK_EVENT = 1;

// Zero-allocation structured JSON logger (timestamp omitted; wire to Io.Timestamp if needed)
fn logJSON(level: []const u8, thread_id: ?usize, msg: []const u8) void {
    if (thread_id) |tid| {
        std.debug.print("{{\"level\":\"{s}\",\"thread_id\":{d},\"message\":\"{s}\"}}\n", .{ level, tid, msg });
    } else {
        std.debug.print("{{\"level\":\"{s}\",\"message\":\"{s}\"}}\n", .{ level, msg });
    }
}

fn ctrlHandler(ctrl_type: windows.DWORD) callconv(std.builtin.CallingConvention.winapi) windows.BOOL {
    if (ctrl_type == CTRL_C_EVENT or ctrl_type == CTRL_BREAK_EVENT) {
        logJSON("info", null, "Shutdown signal received. Draining workers gracefully...");
        should_shutdown.store(true, .monotonic);
        return .TRUE;
    }
    return .FALSE;
}

pub fn main(init: std.process.Init) !void {
    const io = init.io;
    var gpa = std.heap.DebugAllocator(.{}){};
    defer _ = gpa.deinit();
    const allocator = gpa.allocator();

    // Register Win32 Console Handler for graceful shutdown
    if (comptime builtin.target.os.tag == .windows) {
        _ = SetConsoleCtrlHandler(ctrlHandler, .TRUE);
    }

    // JSON configuration loader
    var parsed_config: ?std.json.Parsed(AppConfig) = null;
    defer if (parsed_config) |p| p.deinit();

    var app_config = AppConfig{};

    if (std.Io.Dir.cwd().openFile(io, "config.json", .{})) |file| {
        defer file.close(io);
        const stat = try file.stat(io);
        const file_buf = try allocator.alloc(u8, stat.size);
        defer allocator.free(file_buf);
        var file_reader = std.Io.File.Reader.init(file, io, file_buf);
        try file_reader.interface.readSliceAll(file_buf);
        parsed_config = try std.json.parseFromSlice(AppConfig, allocator, file_buf, .{});
        app_config = parsed_config.?.value;
        logJSON("info", null, "Successfully loaded configuration from config.json");
    } else |_| {
        // config.json not found, use defaults
    }

    // Default connection configuration
    var config = Config{
        .host = app_config.nats_host,
        .port = app_config.nats_port,
        .username = app_config.nats_user,
        .password = app_config.nats_pass,
        .use_tls = app_config.nats_tls,
        .ca_path = app_config.nats_ca_path,
    };

    // Resolve connection config from environment variables (Overrides config.json)
    if (init.environ_map.get("NATS_HOST")) |val| config.host = val;
    if (init.environ_map.get("NATS_PORT")) |val| {
        config.port = try std.fmt.parseInt(u16, val, 10);
    }
    if (init.environ_map.get("NATS_USER")) |val| config.username = val;
    if (init.environ_map.get("NATS_PASS")) |val| config.password = val;
    if (init.environ_map.get("NATS_TLS")) |val| {
        config.use_tls = std.mem.eql(u8, val, "true");
    }
    if (init.environ_map.get("NATS_CA")) |val| config.ca_path = val;

    // Concurrency parameters
    var num_threads = app_config.worker_threads;
    var batch_size = app_config.worker_batch;

    // Parse command line arguments (Overrides env vars and config.json)
    const args = try init.minimal.args.toSlice(init.arena.allocator());
    var i_arg: usize = 1;
    while (i_arg < args.len) : (i_arg += 1) {
        if (std.mem.eql(u8, args[i_arg], "--help") or std.mem.eql(u8, args[i_arg], "-h")) {
            std.debug.print(
                \\Tachyon Background Job Processor (Zig 0.16.0)
                \\
                \\Usage:
                \\  worker.exe [options]
                \\
                \\Options:
                \\  -t, --threads <n>    Number of concurrent worker threads (default: 4)
                \\  -b, --batch <n>      Pull consumer batch size per request (default: 50)
                \\  -h, --help           Display this help guide and exit
                \\
                \\Configuration can also be set via config.json or environment variables.
                \\
            , .{});
            return;
        } else if (std.mem.eql(u8, args[i_arg], "--threads") or std.mem.eql(u8, args[i_arg], "-t")) {
            if (i_arg + 1 < args.len) {
                i_arg += 1;
                num_threads = try std.fmt.parseInt(usize, args[i_arg], 10);
            }
        } else if (std.mem.eql(u8, args[i_arg], "--batch") or std.mem.eql(u8, args[i_arg], "-b")) {
            if (i_arg + 1 < args.len) {
                i_arg += 1;
                batch_size = try std.fmt.parseInt(usize, args[i_arg], 10);
            }
        }
    }

    logJSON("info", null, "Initializing NATS JetStream Stream & Consumers...");
    // Initialize stream and consumers once from main connection
    {
        var init_client = try NatsClient.connect(io, allocator, config);
        defer init_client.deinit();
        try init_client.setupJetStream("JOBS", &[_][]const u8{ "jobs.high.*", "jobs.low.*" });
        try init_client.setupConsumer("JOBS", "WORKER_HIGH", "jobs.high.*");
        try init_client.setupConsumer("JOBS", "WORKER_LOW", "jobs.low.*");
        try init_client.flush();
    }

    // Initialize atomic target thread count
    target_threads.store(num_threads, .monotonic);

    // Spawn Prometheus metrics server thread
    const metrics_ctx = try allocator.create(MetricsContext);
    metrics_ctx.* = .{
        .io = io,
        .allocator = allocator,
        .total_jobs = &total_jobs,
    };
    const metrics_thread = try std.Thread.spawn(.{}, metricsServerRun, .{metrics_ctx});
    metrics_thread.detach();

    logJSON("info", null, "Spawning worker threads...");

    // Use ArrayList to allow dynamic worker auto-scaling
    var active_threads = std.ArrayList(std.Thread).empty;
    defer {
        for (active_threads.items) |t| t.join();
        active_threads.deinit(allocator);
    }

    var i: usize = 0;
    while (i < num_threads) : (i += 1) {
        const ctx = try allocator.create(WorkerContext);
        ctx.* = .{
            .io = io,
            .allocator = allocator,
            .thread_id = i + 1,
            .batch_size = batch_size,
            .config = config,
        };
        const t = try std.Thread.spawn(.{}, workerRun, .{ctx});
        try active_threads.append(allocator, t);
    }

    // Monitor thread that prints throughput once a second
    logJSON("info", null, "Monitoring throughput started.");
    var last_count: usize = 0;
    const start_time = std.Io.Timestamp.now(io, .awake);
    const max_threads: usize = 8;
    const min_threads: usize = num_threads;

    while (!should_shutdown.load(.monotonic)) {
        try io.sleep(std.Io.Duration.fromSeconds(1), .awake);
        const current_count = total_jobs.load(.monotonic);
        const diff = current_count - last_count;
        last_count = current_count;

        if (current_count > 0) {
            const elapsed_duration = start_time.durationTo(std.Io.Timestamp.now(io, .awake));
            const elapsed = @as(f64, @floatFromInt(elapsed_duration.toMilliseconds())) / 1000.0;
            const avg_rate = @as(f64, @floatFromInt(current_count)) / elapsed;
            const active_count = target_threads.load(.monotonic);
            
            // Format log message
            var log_msg_buf: [128]u8 = undefined;
            const log_msg = std.fmt.bufPrint(&log_msg_buf, "Throughput: {d} jobs/sec | Avg: {d:.2} jobs/sec | Active: {d}", .{ diff, avg_rate, active_count }) catch continue;
            logJSON("info", null, log_msg);

            // Dynamic Worker Thread Auto-Scaling UP
            if (diff > 30000 and active_count < max_threads) {
                const new_id = active_count + 1;
                logJSON("info", null, "High throughput detected. Scaling worker pool UP.");
                _ = target_threads.fetchAdd(1, .monotonic);
                
                const ctx = try allocator.create(WorkerContext);
                ctx.* = .{
                    .io = io,
                    .allocator = allocator,
                    .thread_id = new_id,
                    .batch_size = batch_size,
                    .config = config,
                };
                const t = try std.Thread.spawn(.{}, workerRun, .{ctx});
                try active_threads.append(allocator, t);
            }

            // Dynamic Worker Thread Auto-Scaling DOWN
            if (diff < 5000 and active_count > min_threads) {
                _ = target_threads.fetchSub(1, .monotonic);
                logJSON("info", null, "Low throughput detected. Scaling worker pool DOWN.");
            }
        }
    }

    logJSON("info", null, "Main thread shutdown. Draining active worker threads...");
}

fn workerRun(ctx: *WorkerContext) void {
    defer ctx.allocator.destroy(ctx);
    
    // Construct local inbox for this thread
    var inbox_buf: [32]u8 = undefined;
    const inbox = std.fmt.bufPrint(&inbox_buf, "inbox.worker_t{d}", .{ctx.thread_id}) catch return;

    var backoff_ms: u32 = 1000;

    // Zero-Allocation Arena Reusability (Initialize outside the loop)
    var job_arena = std.heap.ArenaAllocator.init(ctx.allocator);
    defer job_arena.deinit();
    const job_alloc = job_arena.allocator();

    // Adaptive batching size limit
    var adaptive_batch = ctx.batch_size;

    while (!should_shutdown.load(.monotonic)) {
        // Dynamic down-scaling exit condition
        if (ctx.thread_id > target_threads.load(.monotonic)) {
            logJSON("info", ctx.thread_id, "Scale-down signal received. Exiting thread.");
            break;
        }

        // Connect local client with backoff reconnect
        var client = NatsClient.connect(ctx.io, ctx.allocator, ctx.config) catch |err| {
            var err_buf: [64]u8 = undefined;
            const err_msg = std.fmt.bufPrint(&err_buf, "Connection failed: {}. Retrying...", .{err}) catch "Connection failed. Retrying...";
            logJSON("warn", ctx.thread_id, err_msg);
            ctx.io.sleep(std.Io.Duration.fromMilliseconds(backoff_ms), .awake) catch {};
            backoff_ms = @min(backoff_ms * 2, 30000);
            continue;
        };
        // Reset backoff on successful connection
        backoff_ms = 1000;
        defer client.deinit();

        client.subscribe(inbox, "1") catch |err| {
            var err_buf: [64]u8 = undefined;
            const err_msg = std.fmt.bufPrint(&err_buf, "Subscription failed: {}. Reconnecting...", .{err}) catch "Subscription failed.";
            logJSON("warn", ctx.thread_id, err_msg);
            continue;
        };

        while (!should_shutdown.load(.monotonic)) {
            // Dynamic down-scaling exit condition
            if (ctx.thread_id > target_threads.load(.monotonic)) {
                break;
            }

            // Priority Queue Routing: Poll WORKER_HIGH first
            client.requestNext("JOBS", "WORKER_HIGH", inbox, adaptive_batch) catch |err| {
                var err_buf: [64]u8 = undefined;
                const err_msg = std.fmt.bufPrint(&err_buf, "requestNext (high) failed: {}. Reconnecting...", .{err}) catch "requestNext failed.";
                logJSON("warn", ctx.thread_id, err_msg);
                break;
            };

            // Pull batch loop
            var msg_count: usize = 0;
            var is_high_empty = false;
            var latency_sum: i64 = 0;
            var processed_in_batch: usize = 0;

            while (msg_count < adaptive_batch) : (msg_count += 1) {
                var msg = client.readMsg() catch |err| {
                    if (!should_shutdown.load(.monotonic)) {
                        var err_buf: [64]u8 = undefined;
                        const err_msg = std.fmt.bufPrint(&err_buf, "Connection lost during read: {}.", .{err}) catch "Connection lost.";
                        logJSON("warn", ctx.thread_id, err_msg);
                    }
                    break;
                };
                defer msg.deinit();

                if (std.mem.startsWith(u8, msg.payload, "NATS/1.0")) {
                    is_high_empty = true;
                    break;
                }

                if (msg.payload.len == 0) {
                    is_high_empty = true;
                    break;
                }

                // Start job execution timing (Latency tracking)
                const job_start = std.Io.Timestamp.now(ctx.io, .awake);

                // Deserialize payload using the reusable arena allocator
                const parsed = std.json.parseFromSlice(Job, job_alloc, msg.payload, .{}) catch {
                    logJSON("error", ctx.thread_id, "Job parsing failed. Routing to DLQ 'jobs.failed'.");
                    client.publish("jobs.failed", null, msg.payload) catch {};
                    client.ack(&msg) catch {};
                    _ = job_arena.reset(.retain_capacity);
                    continue;
                };
                _ = parsed;

                // Acknowledge the job
                client.ack(&msg) catch {
                    break;
                };

                // Increment atomic counter
                _ = total_jobs.fetchAdd(1, .monotonic);

                // Latency execution check
                const job_end = std.Io.Timestamp.now(ctx.io, .awake);
                const latency = job_start.durationTo(job_end);
                const latency_ms = latency.toMilliseconds();
                latency_sum += latency_ms;
                processed_in_batch += 1;

                if (latency_ms > 500) {
                    var lat_buf: [64]u8 = undefined;
                    const lat_msg = std.fmt.bufPrint(&lat_buf, "Job SLA violated: {d}ms execution time", .{latency_ms}) catch "Job SLA violated";
                    logJSON("warn", ctx.thread_id, lat_msg);
                }

                // Reset arena for the next job, retaining memory capacity (Zero Allocations!)
                _ = job_arena.reset(.retain_capacity);
            }

            // Priority Queue Routing Fallback: Poll WORKER_LOW if WORKER_HIGH returned empty
            if (is_high_empty and !should_shutdown.load(.monotonic)) {
                if (ctx.thread_id > target_threads.load(.monotonic)) {
                    break;
                }

                client.requestNext("JOBS", "WORKER_LOW", inbox, adaptive_batch) catch |err| {
                    var err_buf: [64]u8 = undefined;
                    const err_msg = std.fmt.bufPrint(&err_buf, "requestNext (low) failed: {}. Reconnecting...", .{err}) catch "requestNext failed.";
                    logJSON("warn", ctx.thread_id, err_msg);
                    break;
                };

                msg_count = 0;
                while (msg_count < adaptive_batch) : (msg_count += 1) {
                    var msg = client.readMsg() catch |err| {
                        if (!should_shutdown.load(.monotonic)) {
                            var err_buf: [64]u8 = undefined;
                            const err_msg = std.fmt.bufPrint(&err_buf, "Connection lost during read: {}.", .{err}) catch "Connection lost.";
                            logJSON("warn", ctx.thread_id, err_msg);
                        }
                        break;
                    };
                    defer msg.deinit();

                    if (std.mem.startsWith(u8, msg.payload, "NATS/1.0")) {
                        break;
                    }

                    if (msg.payload.len == 0) {
                        break;
                    }

                    const job_start = std.Io.Timestamp.now(ctx.io, .awake);

                    const parsed = std.json.parseFromSlice(Job, job_alloc, msg.payload, .{}) catch {
                        logJSON("error", ctx.thread_id, "Job parsing failed. Routing to DLQ 'jobs.failed'.");
                        client.publish("jobs.failed", null, msg.payload) catch {};
                        client.ack(&msg) catch {};
                        _ = job_arena.reset(.retain_capacity);
                        continue;
                    };
                    _ = parsed;

                    client.ack(&msg) catch {
                        break;
                    };

                    _ = total_jobs.fetchAdd(1, .monotonic);

                    const job_end = std.Io.Timestamp.now(ctx.io, .awake);
                    const latency = job_start.durationTo(job_end);
                    const latency_ms = latency.toMilliseconds();
                    latency_sum += latency_ms;
                    processed_in_batch += 1;

                    if (latency_ms > 500) {
                        var lat_buf: [64]u8 = undefined;
                        const lat_msg = std.fmt.bufPrint(&lat_buf, "Job SLA violated: {d}ms execution time", .{latency_ms}) catch "Job SLA violated";
                        logJSON("warn", ctx.thread_id, lat_msg);
                    }

                    _ = job_arena.reset(.retain_capacity);
                }
            }

            // Adaptive Batching Backpressure Feedback Loop calculation
            if (processed_in_batch > 0) {
                const avg_latency = @divFloor(latency_sum, @as(i64, @intCast(processed_in_batch)));
                if (avg_latency > 200) {
                    // Throttle down batch pulling under backpressure
                    adaptive_batch = @max(adaptive_batch / 2, 1);
                    var bp_buf: [64]u8 = undefined;
                    const bp_msg = std.fmt.bufPrint(&bp_buf, "Backpressure activated. Batch size reduced to: {d}", .{adaptive_batch}) catch "Backpressure activated.";
                    logJSON("info", ctx.thread_id, bp_msg);
                } else if (avg_latency < 50) {
                    // Recover back to default
                    adaptive_batch = @min(adaptive_batch + 10, ctx.batch_size);
                }
            }
        }
    }

    logJSON("info", ctx.thread_id, "Worker thread exited cleanly.");
}

fn metricsServerRun(ctx: *MetricsContext) void {
    defer ctx.allocator.destroy(ctx);

    const addr = std.Io.net.IpAddress.parse("127.0.0.1", 8080) catch |err| {
        var err_buf: [64]u8 = undefined;
        const err_msg = std.fmt.bufPrint(&err_buf, "Metrics Address parse error: {}", .{err}) catch "Metrics parse error.";
        logJSON("error", null, err_msg);
        return;
    };
    var server = addr.listen(ctx.io, .{ .reuse_address = true }) catch |err| {
        var err_buf: [64]u8 = undefined;
        const err_msg = std.fmt.bufPrint(&err_buf, "Metrics Listen error: {}", .{err}) catch "Metrics listen error.";
        logJSON("error", null, err_msg);
        return;
    };

    logJSON("info", null, "Metrics Server listening on http://127.0.0.1:8080/metrics");

    while (!should_shutdown.load(.monotonic)) {
        var conn = server.accept(ctx.io) catch continue;
        defer conn.close(ctx.io);

        var read_buf: [1024]u8 = undefined;
        var r = conn.reader(ctx.io, &read_buf);
        _ = r.interface.takeDelimiter('\n') catch continue;

        const count = ctx.total_jobs.load(.monotonic);
        var body_buf: [256]u8 = undefined;
        const body = std.fmt.bufPrint(&body_buf,
            \\# HELP zig_jobs_processed_total Total number of jobs processed.
            \\# TYPE zig_jobs_processed_total counter
            \\zig_jobs_processed_total {d}
            \\
        , .{count}) catch continue;

        var write_buf: [512]u8 = undefined;
        var w = conn.writer(ctx.io, &write_buf);
        w.interface.print(
            "HTTP/1.1 200 OK\r\nContent-Type: text/plain; version=0.0.4; charset=utf-8\r\nContent-Length: {d}\r\nConnection: close\r\n\r\n{s}",
            .{ body.len, body }
        ) catch continue;
        w.interface.flush() catch continue;
    }
    logJSON("info", null, "Metrics Server exited cleanly.");
}

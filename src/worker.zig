const std = @import("std");
const builtin = @import("builtin");
const NatsClient = @import("nats_client.zig").NatsClient;
const NatsConfig = @import("nats_client.zig").Config;
const AppConfig = @import("config.zig").AppConfig;
const config_mod = @import("config.zig");
const logging = @import("logging.zig");
const resilience = @import("resilience.zig");
const job_mod = @import("job.zig");
const metrics_server = @import("metrics_server.zig");

const Job = job_mod.Job;
const CircuitBreaker = resilience.CircuitBreaker;
const DedupCache = resilience.DedupCache;

const windows = std.os.windows;

// Windows SetConsoleCtrlHandler declaration
extern "kernel32" fn SetConsoleCtrlHandler(
    HandlerRoutine: ?*const fn (windows.DWORD) callconv(std.builtin.CallingConvention.winapi) windows.BOOL,
    Add: windows.BOOL,
) callconv(std.builtin.CallingConvention.winapi) windows.BOOL;

// Global atomic variables
var total_jobs = std.atomic.Value(usize).init(0);
var failed_jobs = std.atomic.Value(usize).init(0);
var should_shutdown = std.atomic.Value(bool).init(false);
var target_threads = std.atomic.Value(usize).init(4);

const WorkerContext = struct {
    io: std.Io,
    allocator: std.mem.Allocator,
    thread_id: usize,
    batch_size: usize,
    config: NatsConfig,
    stream_name: []const u8,
    consumer_high: []const u8,
    consumer_low: []const u8,
    dlq_subject: []const u8,
    max_deliver: u32,
    retry_base_ms: u32,
    retry_max_ms: u32,
    max_jobs_per_second: u32,
    job_timeout_ms: u32,
    dedup_cache_size: usize,
    circuit_failure_threshold: u32,
    circuit_open_ms: u32,
    batch_ack: bool,
};

const CTRL_C_EVENT = 0;
const CTRL_BREAK_EVENT = 1;

fn ctrlHandler(ctrl_type: windows.DWORD) callconv(std.builtin.CallingConvention.winapi) windows.BOOL {
    if (ctrl_type == CTRL_C_EVENT or ctrl_type == CTRL_BREAK_EVENT) {
        logging.logJSON("info", null, "Shutdown signal received. Draining workers gracefully...");
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

    // Register OS signal handlers for graceful shutdown
    if (comptime builtin.target.os.tag == .windows) {
        _ = SetConsoleCtrlHandler(ctrlHandler, .TRUE);
    } else {
        const posix = std.posix;
        const HandlerFn = *align(1) const fn (posix.SIG) callconv(.c) void;
        const handler: HandlerFn = struct {
            fn handle(sig: posix.SIG) callconv(.c) void {
                _ = sig;
                should_shutdown.store(true, .monotonic);
            }
        }.handle;
        const act = posix.Sigaction{
            .handler = .{ .handler = handler },
            .mask = posix.sigemptyset(),
            .flags = 0,
        };
        posix.sigaction(posix.SIG.INT, &act, null);
        posix.sigaction(posix.SIG.TERM, &act, null);
    }

    // Load config.json
    const loaded = try config_mod.loadFromFile(io, allocator);
    var app_config = loaded.app;
    var parsed_config = loaded.parsed;
    defer if (parsed_config) |*p| p.deinit();
    if (parsed_config != null) {
        logging.logJSON("info", null, "Successfully loaded configuration from config.json");
    }

    var nats_config = config_mod.natsFromApp(app_config);
    try config_mod.applyEnv(&app_config, &nats_config, init.environ_map);

    var num_threads = app_config.worker_threads;
    var batch_size = app_config.worker_batch;

    const args = try init.minimal.args.toSlice(init.arena.allocator());
    if (try config_mod.parseCli(args, &num_threads, &batch_size)) return;

    logging.logJSON("info", null, "Initializing NATS JetStream Stream & Consumers...");
    {
        var init_client = try NatsClient.connect(io, allocator, nats_config);
        defer init_client.deinit();
        try init_client.setupJetStream(app_config.stream_name, &[_][]const u8{ app_config.subject_high, app_config.subject_low }, app_config.job_ttl_seconds);
        try init_client.setupJetStream(app_config.dlq_stream, &[_][]const u8{app_config.dlq_subject}, 0);
        try init_client.setupConsumer(app_config.stream_name, app_config.consumer_high, app_config.subject_high, app_config.max_deliver);
        try init_client.setupConsumer(app_config.stream_name, app_config.consumer_low, app_config.subject_low, app_config.max_deliver);
        try init_client.flush();
    }

    target_threads.store(num_threads, .monotonic);

    const metrics_ctx = try allocator.create(metrics_server.MetricsContext);
    metrics_ctx.* = .{
        .io = io,
        .allocator = allocator,
        .total_jobs = &total_jobs,
        .failed_jobs = &failed_jobs,
        .should_shutdown = &should_shutdown,
    };
    const metrics_thread = try std.Thread.spawn(.{}, metrics_server.run, .{metrics_ctx});
    metrics_thread.detach();

    logging.logJSON("info", null, "Spawning worker threads...");

    var active_threads = std.ArrayList(std.Thread).empty;
    defer {
        for (active_threads.items) |t| t.join();
        active_threads.deinit(allocator);
    }

    var i: usize = 0;
    while (i < num_threads) : (i += 1) {
        const ctx = try makeWorkerContext(allocator, io, i + 1, batch_size, nats_config, app_config);
        const t = try std.Thread.spawn(.{}, workerRun, .{ctx});
        try active_threads.append(allocator, t);
    }

    logging.logJSON("info", null, "Monitoring throughput started.");
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
            const fails = failed_jobs.load(.monotonic);

            var log_msg_buf: [160]u8 = undefined;
            const log_msg = std.fmt.bufPrint(&log_msg_buf, "Throughput: {d} jobs/sec | Avg: {d:.2} | Active: {d} | Failed: {d}", .{ diff, avg_rate, active_count, fails }) catch continue;
            logging.logJSON("info", null, log_msg);

            if (diff > 30000 and active_count < max_threads) {
                const new_id = active_count + 1;
                logging.logJSON("info", null, "High throughput detected. Scaling worker pool UP.");
                _ = target_threads.fetchAdd(1, .monotonic);

                const ctx = try makeWorkerContext(allocator, io, new_id, batch_size, nats_config, app_config);
                const t = try std.Thread.spawn(.{}, workerRun, .{ctx});
                try active_threads.append(allocator, t);
            }

            if (diff < 5000 and active_count > min_threads) {
                _ = target_threads.fetchSub(1, .monotonic);
                logging.logJSON("info", null, "Low throughput detected. Scaling worker pool DOWN.");
            }
        }
    }

    logging.logJSON("info", null, "Main thread shutdown. Draining active worker threads...");
}

fn makeWorkerContext(allocator: std.mem.Allocator, io: std.Io, thread_id: usize, batch_size: usize, nats_config: NatsConfig, app_config: AppConfig) !*WorkerContext {
    const ctx = try allocator.create(WorkerContext);
    ctx.* = .{
        .io = io,
        .allocator = allocator,
        .thread_id = thread_id,
        .batch_size = batch_size,
        .config = nats_config,
        .stream_name = app_config.stream_name,
        .consumer_high = app_config.consumer_high,
        .consumer_low = app_config.consumer_low,
        .dlq_subject = app_config.dlq_subject,
        .max_deliver = app_config.max_deliver,
        .retry_base_ms = app_config.retry_base_ms,
        .retry_max_ms = app_config.retry_max_ms,
        .max_jobs_per_second = app_config.max_jobs_per_second,
        .job_timeout_ms = app_config.job_timeout_ms,
        .dedup_cache_size = app_config.dedup_cache_size,
        .circuit_failure_threshold = app_config.circuit_failure_threshold,
        .circuit_open_ms = app_config.circuit_open_ms,
        .batch_ack = app_config.batch_ack,
    };
    return ctx;
}

const JobOutcome = enum {
    continue_batch,
    break_batch,
};

fn handleJob(
    client: *NatsClient,
    ctx: *WorkerContext,
    msg: *const NatsClient.Msg,
    job_alloc: std.mem.Allocator,
    job_arena: *std.heap.ArenaAllocator,
    latency_sum: *i64,
    processed_in_batch: *usize,
    dedup: *DedupCache,
    circuit: *CircuitBreaker,
    defer_flush: *bool,
) JobOutcome {
    // Skip wall-clock work when timeouts are disabled (pure throughput path).
    const track_time = ctx.job_timeout_ms > 0;
    const job_start = if (track_time) std.Io.Timestamp.now(ctx.io, .awake) else null;

    // Circuit is closed with zero failures almost always — allow() is free then.
    // Only sample clock when the breaker is not fully healthy.
    if (circuit.state != .closed or circuit.consecutive_failures > 0) {
        const now_ms = std.Io.Timestamp.now(ctx.io, .awake).toMilliseconds();
        if (!circuit.allow(now_ms)) {
            const delay = resilience.computeBackoffNs(1, ctx.retry_base_ms, ctx.retry_max_ms);
            client.nack(msg, delay) catch {};
            _ = job_arena.reset(.retain_capacity);
            return .continue_batch;
        }
    }

    const parsed = std.json.parseFromSlice(Job, job_alloc, msg.payload, .{}) catch {
        logging.logJSON("error", ctx.thread_id, "Job parsing failed. Routing to DLQ.");
        const fail_now = std.Io.Timestamp.now(ctx.io, .awake).toMilliseconds();
        client.publish(ctx.dlq_subject, null, msg.payload) catch {};
        client.term(msg) catch {
            client.ack(msg) catch {};
        };
        _ = failed_jobs.fetchAdd(1, .monotonic);
        circuit.onFailure(fail_now);
        _ = job_arena.reset(.retain_capacity);
        return .continue_batch;
    };

    const job = parsed.value;

    // Dedup: silent ACK (no per-job log — destroys throughput under load)
    if (dedup.enabled() and dedup.contains(job.id)) {
        if (ctx.batch_ack) {
            client.ackBuffered(msg) catch return .break_batch;
            defer_flush.* = true;
        } else {
            client.ack(msg) catch return .break_batch;
        }
        _ = job_arena.reset(.retain_capacity);
        return .continue_batch;
    }

    // No per-job +WPI: one extra PUB/flush per message dominated the bench.
    // Long handlers can still send progress via the processJob progress callback later.

    job_mod.processJob(job, ctx.thread_id, ctx.job_timeout_ms, ctx.io, null) catch |err| {
        const fail_now = std.Io.Timestamp.now(ctx.io, .awake).toMilliseconds();
        const attempt = if (msg.delivery_count == 0) @as(u32, 1) else msg.delivery_count;
        if (attempt >= ctx.max_deliver) {
            logging.logJSON("error", ctx.thread_id, "Max deliveries reached; routing to DLQ + TERM.");
            client.publish(ctx.dlq_subject, null, msg.payload) catch {};
            client.term(msg) catch {
                client.ack(msg) catch {};
            };
            _ = failed_jobs.fetchAdd(1, .monotonic);
            circuit.onFailure(fail_now);
        } else {
            const delay = resilience.computeBackoffNs(attempt, ctx.retry_base_ms, ctx.retry_max_ms);
            client.nack(msg, delay) catch {};
            var err_buf: [96]u8 = undefined;
            const err_msg = std.fmt.bufPrint(&err_buf, "Job failed ({s}) attempt={d}; NACKed with backoff.", .{ @errorName(err), attempt }) catch "Job failed; NACKed.";
            logging.logJSON("warn", ctx.thread_id, err_msg);
            circuit.onFailure(fail_now);
        }
        _ = job_arena.reset(.retain_capacity);
        return .continue_batch;
    };

    if (track_time) {
        if (job_start) |start| {
            const elapsed = start.durationTo(std.Io.Timestamp.now(ctx.io, .awake)).toMilliseconds();
            if (elapsed > ctx.job_timeout_ms) {
                const fail_now = std.Io.Timestamp.now(ctx.io, .awake).toMilliseconds();
                const attempt = if (msg.delivery_count == 0) @as(u32, 1) else msg.delivery_count;
                const delay = resilience.computeBackoffNs(attempt, ctx.retry_base_ms, ctx.retry_max_ms);
                client.nack(msg, delay) catch {};
                logging.logJSON("warn", ctx.thread_id, "Job exceeded timeout; NACKed.");
                circuit.onFailure(fail_now);
                _ = job_arena.reset(.retain_capacity);
                return .continue_batch;
            }
            latency_sum.* += elapsed;
            if (elapsed > 500) {
                var lat_buf: [64]u8 = undefined;
                const lat_msg = std.fmt.bufPrint(&lat_buf, "Job SLA violated: {d}ms execution time", .{elapsed}) catch "Job SLA violated";
                logging.logJSON("warn", ctx.thread_id, lat_msg);
            }
        }
    }

    dedup.remember(job.id);

    if (ctx.batch_ack) {
        client.ackBuffered(msg) catch return .break_batch;
        defer_flush.* = true;
    } else {
        client.ack(msg) catch return .break_batch;
    }

    circuit.onSuccess();
    _ = total_jobs.fetchAdd(1, .monotonic);
    processed_in_batch.* += 1;

    if (ctx.max_jobs_per_second > 0) {
        const sleep_ms = resilience.rateLimitSleepMs(ctx.max_jobs_per_second);
        ctx.io.sleep(std.Io.Duration.fromMilliseconds(sleep_ms), .awake) catch {};
    }

    _ = job_arena.reset(.retain_capacity);
    return .continue_batch;
}

fn pullConsumerBatch(
    client: *NatsClient,
    ctx: *WorkerContext,
    consumer: []const u8,
    inbox: []const u8,
    adaptive_batch: usize,
    job_alloc: std.mem.Allocator,
    job_arena: *std.heap.ArenaAllocator,
    latency_sum: *i64,
    processed_in_batch: *usize,
    dedup: *DedupCache,
    circuit: *CircuitBreaker,
) enum { empty, ok, reconnect } {
    client.requestNext(ctx.stream_name, consumer, inbox, adaptive_batch) catch return .reconnect;

    var msg_count: usize = 0;
    var saw_empty = false;
    var need_flush = false;

    while (msg_count < adaptive_batch) : (msg_count += 1) {
        var msg = client.readMsg() catch return .reconnect;
        defer msg.deinit();

        if (msg.is_status or msg.payload.len == 0) {
            saw_empty = true;
            break;
        }
        if (std.mem.startsWith(u8, msg.payload, "NATS/1.0")) {
            saw_empty = true;
            break;
        }

        const outcome = handleJob(client, ctx, &msg, job_alloc, job_arena, latency_sum, processed_in_batch, dedup, circuit, &need_flush);
        if (outcome == .break_batch) return .reconnect;
    }

    if (need_flush) {
        client.flushWrites() catch {};
    }

    if (saw_empty and msg_count <= 1) return .empty;
    return .ok;
}

fn workerRun(ctx: *WorkerContext) void {
    defer ctx.allocator.destroy(ctx);

    var inbox_buf: [32]u8 = undefined;
    const inbox = std.fmt.bufPrint(&inbox_buf, "inbox.worker_t{d}", .{ctx.thread_id}) catch return;

    var backoff_ms: u32 = 1000;

    var job_arena = std.heap.ArenaAllocator.init(ctx.allocator);
    defer job_arena.deinit();
    const job_alloc = job_arena.allocator();

    var dedup = DedupCache.init(ctx.allocator, ctx.dedup_cache_size);
    defer dedup.deinit();

    var circuit = CircuitBreaker{
        .failure_threshold = ctx.circuit_failure_threshold,
        .open_ms = ctx.circuit_open_ms,
    };

    var adaptive_batch = ctx.batch_size;

    while (!should_shutdown.load(.monotonic)) {
        if (ctx.thread_id > target_threads.load(.monotonic)) {
            logging.logJSON("info", ctx.thread_id, "Scale-down signal received. Exiting thread.");
            break;
        }

        var client = NatsClient.connect(ctx.io, ctx.allocator, ctx.config) catch |err| {
            var err_buf: [64]u8 = undefined;
            const err_msg = std.fmt.bufPrint(&err_buf, "Connection failed: {}. Retrying...", .{err}) catch "Connection failed. Retrying...";
            logging.logJSON("warn", ctx.thread_id, err_msg);
            const sleep_ms = resilience.withJitter(backoff_ms, ctx.thread_id +% backoff_ms);
            ctx.io.sleep(std.Io.Duration.fromMilliseconds(sleep_ms), .awake) catch {};
            backoff_ms = @min(backoff_ms * 2, 30000);
            continue;
        };
        backoff_ms = 1000;
        defer client.deinit();

        client.subscribe(inbox, "1") catch |err| {
            var err_buf: [64]u8 = undefined;
            const err_msg = std.fmt.bufPrint(&err_buf, "Subscription failed: {}. Reconnecting...", .{err}) catch "Subscription failed.";
            logging.logJSON("warn", ctx.thread_id, err_msg);
            continue;
        };

        while (!should_shutdown.load(.monotonic)) {
            if (ctx.thread_id > target_threads.load(.monotonic)) {
                break;
            }

            var latency_sum: i64 = 0;
            var processed_in_batch: usize = 0;

            const high = pullConsumerBatch(&client, ctx, ctx.consumer_high, inbox, adaptive_batch, job_alloc, &job_arena, &latency_sum, &processed_in_batch, &dedup, &circuit);
            if (high == .reconnect) break;

            if (high == .empty and !should_shutdown.load(.monotonic)) {
                if (ctx.thread_id > target_threads.load(.monotonic)) break;
                const low = pullConsumerBatch(&client, ctx, ctx.consumer_low, inbox, adaptive_batch, job_alloc, &job_arena, &latency_sum, &processed_in_batch, &dedup, &circuit);
                if (low == .reconnect) break;
            }

            if (processed_in_batch > 0) {
                const avg_latency = @divFloor(latency_sum, @as(i64, @intCast(processed_in_batch)));
                if (avg_latency > 200) {
                    adaptive_batch = @max(adaptive_batch / 2, 1);
                    var bp_buf: [64]u8 = undefined;
                    const bp_msg = std.fmt.bufPrint(&bp_buf, "Backpressure activated. Batch size reduced to: {d}", .{adaptive_batch}) catch "Backpressure activated.";
                    logging.logJSON("info", ctx.thread_id, bp_msg);
                } else if (avg_latency < 50) {
                    adaptive_batch = @min(adaptive_batch + 10, ctx.batch_size);
                }
            }
        }
    }

    logging.logJSON("info", ctx.thread_id, "Worker thread exited cleanly.");
}

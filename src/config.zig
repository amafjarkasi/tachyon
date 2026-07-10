const std = @import("std");
const NatsConfig = @import("nats_client.zig").Config;

/// Application configuration loaded from config.json, then overridden by env / CLI.
pub const AppConfig = struct {
    nats_host: []const u8 = "127.0.0.1",
    nats_port: u16 = 4222,
    nats_user: ?[]const u8 = null,
    nats_pass: ?[]const u8 = null,
    nats_tls: bool = false,
    nats_ca_path: ?[]const u8 = null,
    worker_threads: usize = 4,
    worker_batch: usize = 50,
    stream_name: []const u8 = "JOBS",
    consumer_high: []const u8 = "WORKER_HIGH",
    consumer_low: []const u8 = "WORKER_LOW",
    subject_high: []const u8 = "jobs.high.*",
    subject_low: []const u8 = "jobs.low.*",
    dlq_subject: []const u8 = "jobs.failed",
    dlq_stream: []const u8 = "DEAD_LETTERS",
    max_deliver: u32 = 5,
    retry_base_ms: u32 = 1000,
    retry_max_ms: u32 = 30000,
    job_ttl_seconds: u64 = 0,
    max_jobs_per_second: u32 = 0,
    job_timeout_ms: u32 = 5000,
    dedup_cache_size: usize = 10000,
    circuit_failure_threshold: u32 = 10,
    circuit_open_ms: u32 = 5000,
    batch_ack: bool = true,
    /// JetStream pull expires in nanoseconds (default 250ms). Lower = snappier empty polls.
    pull_expires_ns: u64 = 250_000_000,
    /// When true, skip JSON parse and treat payload as opaque (throughput microbench).
    bench_skip_json: bool = false,
    /// Idle sleep after both high/low queues return empty (ms). 0 = busy spin.
    empty_poll_sleep_ms: u32 = 1,
};

pub const LoadedConfig = struct {
    app: AppConfig,
    nats: NatsConfig,
    /// Owns JSON-parsed strings when config.json was loaded; null if defaults only.
    parsed: ?std.json.Parsed(AppConfig) = null,

    pub fn deinit(self: *LoadedConfig) void {
        if (self.parsed) |*p| p.deinit();
    }
};

/// Load config.json if present. Caller must deinit the returned LoadedConfig.parsed field.
pub fn loadFromFile(io: std.Io, allocator: std.mem.Allocator) !struct { app: AppConfig, parsed: ?std.json.Parsed(AppConfig) } {
    if (std.Io.Dir.cwd().openFile(io, "config.json", .{})) |file| {
        defer file.close(io);
        const stat = try file.stat(io);
        const file_buf = try allocator.alloc(u8, stat.size);
        defer allocator.free(file_buf);
        var file_reader = std.Io.File.Reader.init(file, io, file_buf);
        try file_reader.interface.readSliceAll(file_buf);
        const parsed = try std.json.parseFromSlice(AppConfig, allocator, file_buf, .{});
        return .{ .app = parsed.value, .parsed = parsed };
    } else |_| {
        return .{ .app = AppConfig{}, .parsed = null };
    }
}

/// Apply environment variable overrides onto app + build NatsConfig.
pub fn applyEnv(app: *AppConfig, nats: *NatsConfig, environ: anytype) !void {
    if (environ.get("NATS_HOST")) |val| nats.host = val;
    if (environ.get("NATS_PORT")) |val| {
        nats.port = try std.fmt.parseInt(u16, val, 10);
    }
    if (environ.get("NATS_USER")) |val| nats.username = val;
    if (environ.get("NATS_PASS")) |val| nats.password = val;
    if (environ.get("NATS_TLS")) |val| {
        nats.use_tls = std.mem.eql(u8, val, "true");
    }
    if (environ.get("NATS_CA")) |val| nats.ca_path = val;

    if (environ.get("STREAM_NAME")) |val| app.stream_name = val;
    if (environ.get("CONSUMER_HIGH")) |val| app.consumer_high = val;
    if (environ.get("CONSUMER_LOW")) |val| app.consumer_low = val;
    if (environ.get("SUBJECT_HIGH")) |val| app.subject_high = val;
    if (environ.get("SUBJECT_LOW")) |val| app.subject_low = val;
    if (environ.get("DLQ_SUBJECT")) |val| app.dlq_subject = val;
    if (environ.get("DLQ_STREAM")) |val| app.dlq_stream = val;
    if (environ.get("MAX_DELIVER")) |val| {
        app.max_deliver = try std.fmt.parseInt(u32, val, 10);
    }
    if (environ.get("JOB_TTL_SECONDS")) |val| {
        app.job_ttl_seconds = try std.fmt.parseInt(u64, val, 10);
    }
    if (environ.get("MAX_JOBS_PER_SECOND")) |val| {
        app.max_jobs_per_second = try std.fmt.parseInt(u32, val, 10);
    }
    if (environ.get("JOB_TIMEOUT_MS")) |val| {
        app.job_timeout_ms = try std.fmt.parseInt(u32, val, 10);
    }
    if (environ.get("PULL_EXPIRES_NS")) |val| {
        app.pull_expires_ns = try std.fmt.parseInt(u64, val, 10);
    }
    if (environ.get("BENCH_SKIP_JSON")) |val| {
        app.bench_skip_json = std.mem.eql(u8, val, "true") or std.mem.eql(u8, val, "1");
    }
    if (environ.get("EMPTY_POLL_SLEEP_MS")) |val| {
        app.empty_poll_sleep_ms = try std.fmt.parseInt(u32, val, 10);
    }
    if (environ.get("DEDUP_CACHE_SIZE")) |val| {
        app.dedup_cache_size = try std.fmt.parseInt(usize, val, 10);
    }
}

pub fn natsFromApp(app: AppConfig) NatsConfig {
    return .{
        .host = app.nats_host,
        .port = app.nats_port,
        .username = app.nats_user,
        .password = app.nats_pass,
        .use_tls = app.nats_tls,
        .ca_path = app.nats_ca_path,
    };
}

/// Parse CLI flags. Returns true if --help was requested (caller should exit).
pub fn parseCli(args: []const []const u8, num_threads: *usize, batch_size: *usize) !bool {
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
            return true;
        } else if (std.mem.eql(u8, args[i_arg], "--threads") or std.mem.eql(u8, args[i_arg], "-t")) {
            if (i_arg + 1 < args.len) {
                i_arg += 1;
                num_threads.* = try std.fmt.parseInt(usize, args[i_arg], 10);
            }
        } else if (std.mem.eql(u8, args[i_arg], "--batch") or std.mem.eql(u8, args[i_arg], "-b")) {
            if (i_arg + 1 < args.len) {
                i_arg += 1;
                batch_size.* = try std.fmt.parseInt(usize, args[i_arg], 10);
            }
        }
    }
    return false;
}

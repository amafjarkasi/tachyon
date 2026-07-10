# Build stage: compile the Tachyon worker binary
FROM ghcr.io/nickel-lang/zig:0.16.0 AS builder

WORKDIR /app

# Copy all source files
COPY build.zig build.zig.zon ./
COPY src/ ./src/

# Build the worker binary in ReleaseFast mode (zero debug overhead)
RUN zig build -Doptimize=ReleaseFast

# ------------------------------------------------------------------
# Runtime stage: minimal image with just the compiled binary
FROM debian:bookworm-slim AS runtime

RUN apt-get update && apt-get install -y --no-install-recommends \
    ca-certificates \
 && rm -rf /var/lib/apt/lists/*

# Create a non-root user for security
RUN useradd --system --no-create-home --shell /sbin/nologin tachyon

WORKDIR /app

# Copy the compiled binary from the builder stage
COPY --from=builder /app/zig-out/bin/worker /app/worker

# Optional: copy a default config.json into the image
# COPY config.json.example /app/config.json

# Ensure binary is executable
RUN chmod +x /app/worker && chown tachyon:tachyon /app/worker

USER tachyon

# Expose the Prometheus metrics port
EXPOSE 8080

# Environment variable defaults (override at runtime via -e or K8s ConfigMap)
ENV NATS_HOST=127.0.0.1
ENV NATS_PORT=4222
ENV NATS_TLS=false

# Entrypoint: run with 4 threads and batch size 100 by default
ENTRYPOINT ["/app/worker"]
CMD ["--threads", "4", "--batch", "100"]

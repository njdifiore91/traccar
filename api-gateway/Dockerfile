# Stage 1: Build stage
# Using Eclipse Temurin OpenJDK 17 as the base image for building
FROM eclipse-temurin:17-jdk AS builder

# Set working directory
WORKDIR /app

# Copy source code
COPY . .

# Build the application
# Skip tests to speed up the build process in the container
RUN ./gradlew :api-gateway:build -x test --no-daemon

# Stage 2: Runtime stage
# Using Alpine-based JRE for a smaller footprint in production
FROM eclipse-temurin:17-jre-alpine

# Add metadata labels
LABEL maintainer="Traccar Team <support@traccar.org>"
LABEL org.opencontainers.image.title="Traccar API Gateway"
LABEL org.opencontainers.image.description="API Gateway service for Traccar GPS tracking system"
LABEL org.opencontainers.image.licenses="Apache-2.0"

# Set working directory
WORKDIR /app

# Install necessary utilities for health checks and basic troubleshooting
RUN apk --no-cache add wget curl tzdata

# Create a non-root user to run the application (security best practice)
RUN addgroup -S traccar && adduser -S -G traccar traccar

# Create necessary directories with proper permissions
RUN mkdir -p /app/logs /app/config /app/data \
    && chown -R traccar:traccar /app

# Copy the built artifact from the builder stage
COPY --from=builder --chown=traccar:traccar /app/api-gateway/build/libs/*.jar /app/api-gateway.jar

# Copy default configuration
# This provides a base configuration that can be overridden via volume mounts
COPY --chown=traccar:traccar api-gateway/conf/default.xml /app/config/

# Create a directory for temporary files
RUN mkdir -p /app/tmp && chown -R traccar:traccar /app/tmp

# Expose ports for REST API and WebSocket connections
# Port 8082 is used for both HTTP API and WebSocket connections
EXPOSE 8082

# Set environment variables for Java and application configuration
# These memory settings are optimized for API Gateway operations
ENV JAVA_OPTS="-Xms512m -Xmx1g \
    -XX:+UseG1GC \
    -XX:+UseStringDeduplication \
    -XX:+ParallelRefProcEnabled \
    -XX:MaxGCPauseMillis=200 \
    -XX:+HeapDumpOnOutOfMemoryError \
    -XX:HeapDumpPath=/app/logs/heapdump.hprof \
    -Djava.io.tmpdir=/app/tmp \
    -Duser.timezone=UTC"

# Default configuration file path (can be overridden at runtime)
ENV CONFIG_FILE="/app/config/default.xml"

# Additional environment variables with defaults
ENV SERVER_PORT=8082 \
    LOG_LEVEL=INFO \
    MAX_CONNECTIONS=2000

# Health check configuration
# Regularly checks if the service is responsive
HEALTHCHECK --interval=30s --timeout=5s --start-period=60s --retries=3 \
    CMD wget --no-verbose --tries=1 --spider http://localhost:8082/api/health || exit 1

# Switch to non-root user for security
USER traccar

# Create a volume for persistent data and configuration
VOLUME ["/app/logs", "/app/config", "/app/data"]

# Set the entrypoint
# This allows for configuration via environment variables and command line arguments
ENTRYPOINT ["sh", "-c", "java $JAVA_OPTS -jar /app/api-gateway.jar $CONFIG_FILE"]

# Add a default command that can be overridden
CMD [""]
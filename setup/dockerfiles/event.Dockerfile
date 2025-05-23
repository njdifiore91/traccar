# ============================================================================
# Event Processing Service Dockerfile
# ============================================================================
# This Dockerfile builds the Traccar Event Processing Service container image.
# It implements a multi-stage build pattern for optimized size and security.
#
# The Event Processing Service is responsible for:
# - Consuming position data from the message broker
# - Detecting events based on position data (geofence, speed, etc.)
# - Publishing detected events to the message broker
# - Implementing complex event processing with stateful operations
#
# Build with:
#   Standard:  docker build -f event.Dockerfile -t traccar/event-service:latest .
#   Alpine:    docker build -f event.Dockerfile --target alpine -t traccar/event-service:alpine .
# ============================================================================

# ===== Build Stage =====
FROM eclipse-temurin:17-jdk AS builder

# Set working directory
WORKDIR /app

# Copy gradle files first for better layer caching
COPY event-service/gradlew event-service/
COPY event-service/gradle event-service/gradle/
COPY event-service/build.gradle event-service/settings.gradle event-service/

# Download dependencies (will be cached if no changes)
WORKDIR /app/event-service
RUN ./gradlew dependencies --no-daemon

# Copy source code
COPY event-service/src /app/event-service/src/
COPY common/src/main/java/org/traccar/model /app/common/src/main/java/org/traccar/model/
COPY common/src/main/java/org/traccar/proto /app/common/src/main/java/org/traccar/proto/
COPY common/src/main/java/org/traccar/messaging /app/common/src/main/java/org/traccar/messaging/

# Build the application
RUN ./gradlew build -x test --no-daemon

# ===== Custom JRE Creation Stage =====
FROM eclipse-temurin:17-jdk AS jre-builder

# Create a custom JRE using jlink to reduce image size
RUN jlink \
    --add-modules java.base,java.logging,java.naming,java.desktop,java.management,java.security.jgss,java.instrument,jdk.unsupported,java.sql,java.xml,jdk.crypto.ec,java.net.http,java.compiler,jdk.management \
    --strip-debug \
    --no-man-pages \
    --no-header-files \
    --compress=2 \
    --output /jre-minimal

# ===== Runtime Stage (Debian) =====
FROM debian:bullseye-slim AS standard

# Set environment variables
ENV JAVA_HOME=/opt/java/openjdk \
    PATH="/opt/java/openjdk/bin:$PATH" \
    SERVICE_NAME="event-service" \
    SERVICE_PORT=8082 \
    SPRING_PROFILES_ACTIVE="prod" \
    TZ=UTC

# Copy the custom JRE from the jre-builder stage
COPY --from=jre-builder /jre-minimal $JAVA_HOME

# Set working directory
WORKDIR /app

# Create a non-root user to run the application
RUN set -eux; \
    apt-get update; \
    apt-get install -y --no-install-recommends \
        curl \
        tzdata \
        ca-certificates; \
    groupadd -r traccar --gid=1000; \
    useradd -r -g traccar --uid=1000 -s /bin/false -d /app traccar; \
    apt-get clean; \
    rm -rf /var/lib/apt/lists/*; \
    mkdir -p /app/config /app/logs; \
    chown -R traccar:traccar /app

# Copy the built application from the builder stage
COPY --from=builder /app/event-service/build/libs/*.jar /app/event-service.jar

# Switch to non-root user
USER traccar:traccar

# Expose the service port
EXPOSE ${SERVICE_PORT}

# Set health check
HEALTHCHECK --interval=30s --timeout=5s --start-period=60s --retries=3 \
  CMD curl -f http://localhost:${SERVICE_PORT}/actuator/health/liveness || exit 1

# Set entry point with proper memory settings and GC configuration
ENTRYPOINT ["java", \
    "-Xms512m", \
    "-Xmx1g", \
    "-XX:+UseG1GC", \
    "-XX:MaxGCPauseMillis=200", \
    "-XX:+HeapDumpOnOutOfMemoryError", \
    "-XX:HeapDumpPath=/app/logs/heapdump.hprof", \
    "-Djava.security.egd=file:/dev/./urandom", \
    "-Duser.timezone=UTC", \
    "-Dfile.encoding=UTF-8", \
    "-jar", "/app/event-service.jar"]

# Default command line arguments
CMD ["--spring.config.location=file:/app/config/application.yml"]

# ===== Runtime Stage (Alpine) =====
FROM alpine:3.18 AS alpine

# Set environment variables
ENV JAVA_HOME=/opt/java/openjdk \
    PATH="/opt/java/openjdk/bin:$PATH" \
    SERVICE_NAME="event-service" \
    SERVICE_PORT=8082 \
    SPRING_PROFILES_ACTIVE="prod" \
    TZ=UTC

# Copy the custom JRE from the jre-builder stage
COPY --from=jre-builder /jre-minimal $JAVA_HOME

# Set working directory
WORKDIR /app

# Create a non-root user to run the application
RUN set -eux; \
    apk add --no-cache \
        curl \
        tzdata \
        ca-certificates; \
    addgroup -g 1000 -S traccar; \
    adduser -u 1000 -S traccar -G traccar -h /app -s /sbin/nologin; \
    mkdir -p /app/config /app/logs; \
    chown -R traccar:traccar /app

# Copy the built application from the builder stage
COPY --from=builder /app/event-service/build/libs/*.jar /app/event-service.jar

# Switch to non-root user
USER traccar:traccar

# Expose the service port
EXPOSE ${SERVICE_PORT}

# Set health check
HEALTHCHECK --interval=30s --timeout=5s --start-period=60s --retries=3 \
  CMD curl -f http://localhost:${SERVICE_PORT}/actuator/health/liveness || exit 1

# Set entry point with proper memory settings and GC configuration
ENTRYPOINT ["java", \
    "-Xms512m", \
    "-Xmx1g", \
    "-XX:+UseG1GC", \
    "-XX:MaxGCPauseMillis=200", \
    "-XX:+HeapDumpOnOutOfMemoryError", \
    "-XX:HeapDumpPath=/app/logs/heapdump.hprof", \
    "-Djava.security.egd=file:/dev/./urandom", \
    "-Duser.timezone=UTC", \
    "-Dfile.encoding=UTF-8", \
    "-jar", "/app/event-service.jar"]

# Default command line arguments
CMD ["--spring.config.location=file:/app/config/application.yml"]

# ===== Common Metadata =====
# Apply labels to all images
FROM standard

# Metadata labels following OCI Image Specification
LABEL org.opencontainers.image.title="Traccar Event Processing Service" \
      org.opencontainers.image.description="Analyzes position data to detect events such as geofence transitions, speed violations, and device status changes" \
      org.opencontainers.image.vendor="Traccar" \
      org.opencontainers.image.version="1.0.0" \
      org.opencontainers.image.licenses="Apache-2.0" \
      org.opencontainers.image.source="https://github.com/traccar/traccar" \
      org.opencontainers.image.documentation="https://www.traccar.org/documentation/" \
      org.opencontainers.image.authors="Traccar Team <support@traccar.org>" \
      maintainer="Traccar" \
      service="event-processing" \
      component="event-service" \
      tier="backend"
# Multi-stage build for Protocol Service
#
# This Dockerfile implements a multi-stage build pattern with separate build and runtime stages
# for the Protocol Service, which handles device connections and protocol decoding for 200+ protocols.

# ===== Dependencies Stage =====
# This stage prepares all dependencies which change less frequently than code
FROM eclipse-temurin:17-jdk AS dependencies

# Set working directory for build
WORKDIR /build

# Copy only pom.xml files to cache dependencies
COPY protocol-service/pom.xml ./
COPY common/pom.xml ../common/pom.xml

# Download dependencies (will be cached if pom files don't change)
RUN --mount=type=cache,target=/root/.m2 \
    mvn dependency:go-offline -B

# ===== Build Stage =====
FROM dependencies AS build

# Copy source code
COPY protocol-service/src ./src/
COPY common/src ../common/src/

# Build the application
RUN --mount=type=cache,target=/root/.m2 \
    mvn clean package -DskipTests \
    && echo "Build completed successfully"

# ===== JRE Creation Stage =====
# Create a custom minimal JRE using jlink to reduce image size
FROM eclipse-temurin:17-jdk AS jre-build

# Only include modules required by the Protocol Service
RUN jlink \
    --add-modules java.base,java.logging,java.xml,java.sql,java.naming,java.desktop,java.management,java.security.jgss,java.instrument,java.net.http,jdk.unsupported \
    --strip-debug \
    --no-man-pages \
    --no-header-files \
    --compress=2 \
    --output /jre-minimal

# ===== Runtime Stage (Debian-based) =====
FROM debian:bullseye-slim AS debian-runtime

# Install minimal required packages and clean up in the same layer to reduce image size
RUN apt-get update && apt-get install -y --no-install-recommends \
    curl \
    ca-certificates \
    tzdata \
    && rm -rf /var/lib/apt/lists/* \
    && echo "Installed minimal packages"

# Set non-root user for enhanced security
RUN groupadd -r traccar && useradd -r -g traccar -u 1000 -s /sbin/nologin traccar

# Set working directory
WORKDIR /opt/traccar

# Copy custom JRE from jre-build stage
COPY --from=jre-build /jre-minimal /opt/traccar/jre

# Copy application jar and dependencies from build stage
COPY --from=build /build/target/protocol-service.jar /opt/traccar/
COPY --from=build /build/target/lib/ /opt/traccar/lib/

# Create necessary directories with proper permissions in a single layer
RUN mkdir -p /opt/traccar/logs /opt/traccar/conf \
    && chown -R traccar:traccar /opt/traccar \
    && chmod -R 755 /opt/traccar/jre/bin \
    && chmod 644 /opt/traccar/protocol-service.jar

# Set environment variables
ENV JAVA_HOME=/opt/traccar/jre \
    PATH=/opt/traccar/jre/bin:$PATH \
    TRACCAR_HOME=/opt/traccar \
    TRACCAR_CONFIG=/opt/traccar/conf/config.yml

# ===== Runtime Stage (Alpine-based) =====
FROM alpine:3.17 AS alpine-runtime

# Install minimal required packages in a single layer
RUN apk --no-cache add \
    curl \
    ca-certificates \
    tzdata \
    && echo "Installed minimal packages"

# Set non-root user for enhanced security
RUN addgroup -S traccar && adduser -S -G traccar -u 1000 traccar

# Set working directory
WORKDIR /opt/traccar

# Copy custom JRE from jre-build stage
COPY --from=jre-build /jre-minimal /opt/traccar/jre

# Copy application jar and dependencies from build stage
COPY --from=build /build/target/protocol-service.jar /opt/traccar/
COPY --from=build /build/target/lib/ /opt/traccar/lib/

# Create necessary directories with proper permissions in a single layer
RUN mkdir -p /opt/traccar/logs /opt/traccar/conf \
    && chown -R traccar:traccar /opt/traccar \
    && chmod -R 755 /opt/traccar/jre/bin \
    && chmod 644 /opt/traccar/protocol-service.jar

# Set environment variables
ENV JAVA_HOME=/opt/traccar/jre \
    PATH=/opt/traccar/jre/bin:$PATH \
    TRACCAR_HOME=/opt/traccar \
    TRACCAR_CONFIG=/opt/traccar/conf/config.yml

# ===== Final Stage (default to Debian, can be overridden with --target=alpine-runtime) =====
FROM debian-runtime

# Expose ports for various protocols
# Common GPS device protocols
EXPOSE 5000-5150/tcp
EXPOSE 5000-5150/udp

# Specific protocol ports with comments for clarity
EXPOSE 5004/tcp  # TK103 protocol
EXPOSE 5005/tcp  # GPS103 protocol
EXPOSE 5013/tcp  # ST910 protocol
EXPOSE 5027/tcp  # Teltonika protocol
EXPOSE 5040/tcp  # Meitrack protocol
EXPOSE 5055/tcp  # OsmAnd protocol
EXPOSE 8082/tcp  # Health check and management API

# Health check endpoint for container orchestration
# This allows Kubernetes to monitor the container's health
HEALTHCHECK --interval=30s --timeout=5s --start-period=60s --retries=3 \
    CMD curl -f http://localhost:8082/actuator/health/liveness || exit 1

# Switch to non-root user for enhanced security
USER traccar

# Set volume for configuration and logs
# This allows for persistent storage and external configuration
VOLUME ["/opt/traccar/conf", "/opt/traccar/logs"]

# Command to run the application with optimized JVM settings
# These settings are tuned for the Protocol Service's requirements
CMD ["java", "-jar", \
     # Set timezone to UTC for consistent timestamps
     "-Duser.timezone=UTC", \
     # Prefer IPv4 for better compatibility with various networks
     "-Djava.net.preferIPv4Stack=true", \
     # Memory settings optimized for protocol handling
     "-Xms512m", "-Xmx512m", \
     # Use G1 Garbage Collector for better performance with large heaps
     "-XX:+UseG1GC", \
     # Limit GC pauses to improve responsiveness
     "-XX:MaxGCPauseMillis=200", \
     # Create heap dumps on OOM for troubleshooting
     "-XX:+HeapDumpOnOutOfMemoryError", \
     "-XX:HeapDumpPath=/opt/traccar/logs/", \
     # Main application jar
     "/opt/traccar/protocol-service.jar"]
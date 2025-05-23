# Multi-stage build for Traccar API Gateway Service
# This Dockerfile implements a multi-stage build pattern to create an optimized container image
# for the API Gateway Service, which routes client requests, handles authentication, and serves
# REST API and WebSocket connections.

# Stage 1: Build environment with full JDK
# This stage uses a full JDK image to compile and package the application
FROM eclipse-temurin:17-jdk AS builder

# Build arguments
ARG APP_USER=traccar
ARG APP_UID=1000
ARG APP_GID=1000

# Set working directory
WORKDIR /build

# Copy the API Gateway service source code
# Only copying what's needed for the build to optimize build cache usage
COPY api-gateway/src ./src
COPY api-gateway/pom.xml ./
COPY common/src/main/java/org/traccar/model ./common/src/main/java/org/traccar/model

# Build the application
# Using mount cache to speed up builds by caching Maven dependencies
RUN --mount=type=cache,target=/root/.m2 \
    mvn clean package -DskipTests

# Stage 2: Runtime environment with JRE slim
# This stage uses a minimal JRE image for standard deployments
FROM eclipse-temurin:17-jre-jammy AS runtime

# Build arguments
ARG APP_USER=traccar
ARG APP_UID=1000
ARG APP_GID=1000
ARG JAVA_OPTS="-Xms512m -Xmx512m"

# Set environment variables
# These can be overridden at runtime
ENV JAVA_OPTS=${JAVA_OPTS} \
    LOG_LEVEL=INFO \
    CONFIG_FILE=/opt/traccar/conf/config.xml \
    DISCOVERY_TYPE=kubernetes \
    DISCOVERY_HOST=localhost \
    DISCOVERY_PORT=8500 \
    BROKER_TYPE=rabbitmq \
    BROKER_HOST=localhost \
    BROKER_PORT=5672 \
    SERVICE_PORT=8080

# Create non-root user and required directories
# Running as non-root is a security best practice
RUN groupadd -g ${APP_GID} ${APP_USER} && \
    useradd -u ${APP_UID} -g ${APP_GID} -s /bin/bash -m ${APP_USER} && \
    mkdir -p /opt/traccar/conf /opt/traccar/logs /opt/traccar/data /opt/traccar/web && \
    chown -R ${APP_USER}:${APP_USER} /opt/traccar

# Install minimal required packages
# Only installing essential tools to reduce attack surface
RUN apt-get update && \
    apt-get install -y --no-install-recommends \
        ca-certificates \
        curl \
        tzdata && \
    apt-get clean && \
    rm -rf /var/lib/apt/lists/*

# Set working directory
WORKDIR /opt/traccar

# Copy application from builder stage
# Only copying the compiled artifacts, not the source code
COPY --from=builder --chown=${APP_USER}:${APP_USER} /build/target/api-gateway.jar /opt/traccar/
COPY --from=builder --chown=${APP_USER}:${APP_USER} /build/target/lib /opt/traccar/lib

# Copy configuration and web assets
COPY --chown=${APP_USER}:${APP_USER} api-gateway/src/main/resources/config.xml /opt/traccar/conf/
COPY --chown=${APP_USER}:${APP_USER} api-gateway/src/main/resources/web /opt/traccar/web/

# Expose service port
EXPOSE ${SERVICE_PORT}

# Switch to non-root user
USER ${APP_USER}

# Health check
# Regular health checks allow container orchestration systems to monitor application health
HEALTHCHECK --interval=30s --timeout=5s --start-period=60s --retries=3 \
    CMD curl -f http://localhost:${SERVICE_PORT}/api/health || exit 1

# Set entrypoint
ENTRYPOINT ["sh", "-c", "java ${JAVA_OPTS} -jar /opt/traccar/api-gateway.jar"]

# Stage 3: Alpine-based minimal runtime
# This stage provides an even smaller footprint for production deployments
FROM eclipse-temurin:17-jre-alpine AS alpine

# Build arguments
ARG APP_USER=traccar
ARG APP_UID=1000
ARG APP_GID=1000
ARG JAVA_OPTS="-Xms512m -Xmx512m"

# Set environment variables
# These can be overridden at runtime
ENV JAVA_OPTS=${JAVA_OPTS} \
    LOG_LEVEL=INFO \
    CONFIG_FILE=/opt/traccar/conf/config.xml \
    DISCOVERY_TYPE=kubernetes \
    DISCOVERY_HOST=localhost \
    DISCOVERY_PORT=8500 \
    BROKER_TYPE=rabbitmq \
    BROKER_HOST=localhost \
    BROKER_PORT=5672 \
    SERVICE_PORT=8080

# Create non-root user and required directories
# Running as non-root is a security best practice
RUN addgroup -g ${APP_GID} ${APP_USER} && \
    adduser -u ${APP_UID} -G ${APP_USER} -s /bin/sh -D ${APP_USER} && \
    mkdir -p /opt/traccar/conf /opt/traccar/logs /opt/traccar/data /opt/traccar/web && \
    chown -R ${APP_USER}:${APP_USER} /opt/traccar

# Install minimal required packages
# Only installing essential tools to reduce attack surface
RUN apk add --no-cache \
    ca-certificates \
    curl \
    tzdata && \
    rm -rf /var/cache/apk/*

# Set working directory
WORKDIR /opt/traccar

# Copy application from builder stage
# Only copying the compiled artifacts, not the source code
COPY --from=builder --chown=${APP_USER}:${APP_USER} /build/target/api-gateway.jar /opt/traccar/
COPY --from=builder --chown=${APP_USER}:${APP_USER} /build/target/lib /opt/traccar/lib

# Copy configuration and web assets
COPY --chown=${APP_USER}:${APP_USER} api-gateway/src/main/resources/config.xml /opt/traccar/conf/
COPY --chown=${APP_USER}:${APP_USER} api-gateway/src/main/resources/web /opt/traccar/web/

# Expose service port
EXPOSE ${SERVICE_PORT}

# Switch to non-root user
USER ${APP_USER}

# Health check
# Regular health checks allow container orchestration systems to monitor application health
HEALTHCHECK --interval=30s --timeout=5s --start-period=60s --retries=3 \
    CMD wget -q -O /dev/null http://localhost:${SERVICE_PORT}/api/health || exit 1

# Set entrypoint
ENTRYPOINT ["sh", "-c", "java ${JAVA_OPTS} -jar /opt/traccar/api-gateway.jar"]
# Multi-stage build for Traccar Reporting Service
# Stage 1: Build environment
FROM eclipse-temurin:17-jdk AS builder

# Set working directory
WORKDIR /app

# Copy gradle files first for better layer caching
COPY gradlew gradle.properties settings.gradle build.gradle ./
COPY gradle ./gradle

# Download dependencies (this layer will be cached unless gradle files change)
RUN ./gradlew dependencies --no-daemon

# Copy source code
COPY src ./src

# Build the application
RUN ./gradlew build --no-daemon -x test

# Stage 2: Runtime environment
FROM eclipse-temurin:17-jre-alpine

# Set working directory
WORKDIR /app

# Create non-root user for security
RUN addgroup -S traccar && adduser -S -G traccar traccar

# Set environment variables for JVM optimization in containers
ENV JAVA_OPTS="-XX:+UseContainerSupport -XX:MaxRAMPercentage=75.0 -XX:InitialRAMPercentage=50.0 -XX:+ExitOnOutOfMemoryError -Djava.security.egd=file:/dev/urandom -Duser.timezone=UTC"

# Set additional environment variables for configuration
ENV SERVICE_PORT=8082 \
    CONFIG_FILE=/app/conf/reporting.xml \
    LOG_LEVEL=INFO

# Create directory for configuration and data
RUN mkdir -p /app/conf /app/data /app/logs \
    && chown -R traccar:traccar /app

# Copy the built application from the builder stage
COPY --from=builder --chown=traccar:traccar /app/build/libs/*.jar /app/reporting-service.jar

# Copy default configuration
COPY --chown=traccar:traccar conf/* /app/conf/

# Expose the service port
EXPOSE ${SERVICE_PORT}

# Set volume for persistent data and configuration
VOLUME ["/app/conf", "/app/data", "/app/logs"]

# Switch to non-root user
USER traccar

# Health check configuration
HEALTHCHECK --interval=30s --timeout=5s --start-period=60s --retries=3 \
    CMD wget --no-verbose --tries=1 --spider http://localhost:${SERVICE_PORT}/api/v1/health || exit 1

# Set proper signal handling for graceful shutdown
ENTRYPOINT ["sh", "-c", "exec java $JAVA_OPTS -Dlogback.configurationFile=/app/conf/logback.xml -jar /app/reporting-service.jar ${CONFIG_FILE}"]
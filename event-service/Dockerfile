# Build stage
FROM eclipse-temurin:17-jdk AS build

WORKDIR /app

# Copy gradle files first for better layer caching
COPY gradlew ./
COPY gradle ./gradle/
COPY build.gradle ./
COPY settings.gradle ./

# Make gradlew executable
RUN chmod +x ./gradlew

# Download dependencies (will be cached if no changes)
RUN ./gradlew dependencies

# Copy source code
COPY src ./src/

# Build the application
RUN ./gradlew build -x test

# Create a custom JRE using jlink for smaller image size
RUN jlink \
    --add-modules java.base,java.logging,java.naming,java.desktop,java.management,java.security.jgss,java.instrument,jdk.unsupported,java.sql,java.net.http,jdk.crypto.ec \
    --strip-debug \
    --no-man-pages \
    --no-header-files \
    --compress=2 \
    --output /jre-minimal

# Runtime stage
FROM alpine:3.18

WORKDIR /app

# Install required packages
RUN apk add --no-cache tzdata ca-certificates curl && \
    addgroup -S traccar && \
    adduser -S -G traccar traccar

# Copy custom JRE from build stage
COPY --from=build /jre-minimal /opt/jre-minimal

# Set PATH to include custom JRE
ENV PATH="/opt/jre-minimal/bin:${PATH}"

# Copy the built application
COPY --from=build /app/build/libs/*.jar /app/event-service.jar

# Create directory for configuration and logs with proper permissions
RUN mkdir -p /app/logs /app/config && \
    chown -R traccar:traccar /app

# Switch to non-root user
USER traccar

# Expose ports
EXPOSE 8080 9090

# Set JVM options for containers
ENV JAVA_OPTS="-XX:+UseContainerSupport -XX:MaxRAMPercentage=75.0 -XX:InitialRAMPercentage=50.0 -XX:+ExitOnOutOfMemoryError -Djava.security.egd=file:/dev/urandom -Dspring.config.location=file:/app/config/"

# Health check
HEALTHCHECK --interval=30s --timeout=5s --start-period=60s --retries=3 CMD curl -f http://localhost:8080/actuator/health/liveness || exit 1

# Copy default configuration if not provided externally
COPY --chown=traccar:traccar config/* /app/config/

# Start the application
ENTRYPOINT ["sh", "-c", "java $JAVA_OPTS -jar /app/event-service.jar"]
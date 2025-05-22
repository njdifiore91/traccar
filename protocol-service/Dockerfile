# Multi-stage build for Protocol Service

# Build stage
FROM eclipse-temurin:17-jdk AS build

WORKDIR /app

# Copy Maven wrapper and pom.xml
COPY mvnw mvnw.cmd ./
COPY .mvn ./.mvn/
COPY pom.xml ./

# Download dependencies (this layer will be cached)
RUN ./mvnw dependency:go-offline -B

# Copy source code
COPY src ./src/

# Build the application
RUN ./mvnw package -DskipTests

# Runtime stage
FROM eclipse-temurin:17-jre-alpine

LABEL maintainer="Traccar <support@traccar.org>"
LABEL description="Traccar Protocol Service for handling device connections and protocol implementations"

WORKDIR /app

# Create non-root user
RUN addgroup -S traccar && adduser -S -G traccar traccar

# Copy the built artifact from the build stage
COPY --from=build /app/target/protocol-service.jar ./

# Install required packages
RUN apk --no-cache add curl tzdata

# Copy configuration files
COPY src/main/resources/config.yml ./config/

# Create necessary directories with proper permissions
RUN mkdir -p /app/logs /app/data /app/config && \
    chown -R traccar:traccar /app

# Set environment variables
ENV JAVA_OPTS="-Xms512m -Xmx1g -XX:+UseG1GC -XX:+HeapDumpOnOutOfMemoryError -XX:HeapDumpPath=/app/logs/"
ENV CONFIG_FILE="/app/config/config.yml"
ENV PROTOCOL_SERVICE_HOME="/app"
ENV TZ="UTC"

# Expose ports for various protocols (5000-5100 range)
EXPOSE 5000-5100

# Expose health check port
EXPOSE 8080

# Switch to non-root user
USER traccar

# Health check configuration
HEALTHCHECK --interval=30s --timeout=5s --start-period=60s --retries=3 \
    CMD curl -f http://localhost:8080/actuator/health/liveness || exit 1

# Set the entrypoint
ENTRYPOINT ["sh", "-c", "java $JAVA_OPTS -jar protocol-service.jar $CONFIG_FILE"]
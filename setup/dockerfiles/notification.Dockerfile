# Stage 1: Build environment
FROM eclipse-temurin:17-jdk AS builder

# Set working directory
WORKDIR /app

# Copy gradle files first for better layer caching
COPY notification-service/gradlew notification-service/
COPY notification-service/gradle notification-service/gradle/
COPY notification-service/build.gradle notification-service/settings.gradle notification-service/

# Copy source code
COPY notification-service/src notification-service/src/
COPY common/src/main/java/org/traccar/model common/src/main/java/org/traccar/model/
COPY common/src/main/java/org/traccar/proto common/src/main/java/org/traccar/proto/
COPY common/src/main/java/org/traccar/messaging common/src/main/java/org/traccar/messaging/

# Build the application
WORKDIR /app/notification-service
RUN ./gradlew clean build -x test --no-daemon

# Stage 2: Runtime environment (Slim variant)
FROM eclipse-temurin:17-jre-slim AS slim-image

# Add label metadata
LABEL org.opencontainers.image.title="Traccar Notification Service"
LABEL org.opencontainers.image.description="Notification service for Traccar GPS tracking system"
LABEL org.opencontainers.image.vendor="Traccar"
LABEL org.opencontainers.image.version="1.0.0"
LABEL org.opencontainers.image.licenses="Apache-2.0"
LABEL org.opencontainers.image.source="https://github.com/traccar/traccar"

# Set working directory
WORKDIR /app

# Install necessary packages and clean up
RUN apt-get update && \
    apt-get install -y --no-install-recommends curl ca-certificates && \
    apt-get clean && \
    rm -rf /var/lib/apt/lists/* && \
    groupadd -r traccar && \
    useradd -r -g traccar -d /app -s /sbin/nologin -c "Traccar user" traccar && \
    mkdir -p /app/logs /app/data /app/conf /app/templates && \
    chown -R traccar:traccar /app

# Copy the built artifact from the builder stage
COPY --from=builder --chown=traccar:traccar /app/notification-service/build/libs/*.jar /app/notification-service.jar

# Copy configuration files and templates
COPY --chown=traccar:traccar notification-service/src/main/resources/application.yml /app/conf/
COPY --chown=traccar:traccar notification-service/src/main/resources/templates/ /app/templates/

# Expose ports
EXPOSE 8080

# Set environment variables
ENV JAVA_OPTS="-Xms256m -Xmx512m -XX:+UseG1GC -XX:+HeapDumpOnOutOfMemoryError -XX:HeapDumpPath=/app/logs/"
ENV SPRING_CONFIG_LOCATION=file:/app/conf/application.yml
ENV NOTIFICATION_TEMPLATES_PATH=/app/templates

# Set health check
HEALTHCHECK --interval=30s --timeout=5s --start-period=60s --retries=3 \
  CMD curl -f http://localhost:8080/actuator/health/liveness || exit 1

# Switch to non-root user
USER traccar

# Set volume for persistent data
VOLUME ["/app/logs", "/app/data", "/app/conf", "/app/templates"]

# Run the application
ENTRYPOINT ["sh", "-c"]
CMD ["java $JAVA_OPTS -jar /app/notification-service.jar"]

# Stage 3: Alpine-based minimal production image
FROM eclipse-temurin:17-jre-alpine AS alpine-image

# Add label metadata
LABEL org.opencontainers.image.title="Traccar Notification Service (Alpine)"
LABEL org.opencontainers.image.description="Notification service for Traccar GPS tracking system - Alpine variant"
LABEL org.opencontainers.image.vendor="Traccar"
LABEL org.opencontainers.image.version="1.0.0"
LABEL org.opencontainers.image.licenses="Apache-2.0"
LABEL org.opencontainers.image.source="https://github.com/traccar/traccar"

# Set working directory
WORKDIR /app

# Install necessary packages and clean up
RUN apk add --no-cache curl ca-certificates tzdata && \
    addgroup -S traccar && \
    adduser -S -G traccar -h /app -s /sbin/nologin -g "Traccar user" traccar && \
    mkdir -p /app/logs /app/data /app/conf /app/templates && \
    chown -R traccar:traccar /app

# Copy the built artifact from the builder stage
COPY --from=builder --chown=traccar:traccar /app/notification-service/build/libs/*.jar /app/notification-service.jar

# Copy configuration files and templates
COPY --chown=traccar:traccar notification-service/src/main/resources/application.yml /app/conf/
COPY --chown=traccar:traccar notification-service/src/main/resources/templates/ /app/templates/

# Expose ports
EXPOSE 8080

# Set environment variables
ENV JAVA_OPTS="-Xms256m -Xmx512m -XX:+UseG1GC -XX:+HeapDumpOnOutOfMemoryError -XX:HeapDumpPath=/app/logs/"
ENV SPRING_CONFIG_LOCATION=file:/app/conf/application.yml
ENV NOTIFICATION_TEMPLATES_PATH=/app/templates

# Set health check
HEALTHCHECK --interval=30s --timeout=5s --start-period=60s --retries=3 \
  CMD wget -q --spider http://localhost:8080/actuator/health/liveness || exit 1

# Switch to non-root user
USER traccar

# Set volume for persistent data
VOLUME ["/app/logs", "/app/data", "/app/conf", "/app/templates"]

# Run the application
ENTRYPOINT ["sh", "-c"]
CMD ["java $JAVA_OPTS -jar /app/notification-service.jar"]

# Default to slim image
FROM slim-image
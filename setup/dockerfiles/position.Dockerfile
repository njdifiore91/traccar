# Position Processing Service Dockerfile
# Multi-stage build for the Position Processing Service which processes, validates, and enriches position data

# ===== Build Stage =====
FROM eclipse-temurin:17-jdk-jammy AS build

# Set working directory
WORKDIR /app

# Copy Maven wrapper and POM file
COPY position-service/.mvn/ .mvn
COPY position-service/mvnw position-service/pom.xml ./

# Download dependencies (this layer will be cached unless pom.xml changes)
RUN ./mvnw dependency:go-offline

# Copy source code
COPY position-service/src ./src

# Build the application
RUN ./mvnw clean package -DskipTests

# ===== Create optimized JRE using jlink (for Alpine variant) =====
FROM eclipse-temurin:17-jdk-alpine AS jre-build

# Create a custom JRE
RUN jlink \
    --add-modules java.base,java.logging,java.xml,java.sql,java.naming,java.desktop,java.management,java.security.jgss,java.instrument,jdk.unsupported \
    --strip-debug \
    --no-man-pages \
    --no-header-files \
    --compress=2 \
    --output /javaruntime

# ===== Standard Runtime Stage =====
FROM eclipse-temurin:17-jre-jammy-slim AS standard

# Add labels for better maintainability
LABEL org.opencontainers.image.title="Traccar Position Processing Service"
LABEL org.opencontainers.image.description="Processes, validates, and enriches position data from the message broker"
LABEL org.opencontainers.image.vendor="Traccar"

# Create a non-root user to run the application
RUN groupadd -r traccar && useradd -r -g traccar -u 1000 -s /bin/bash traccar

# Set working directory
WORKDIR /opt/traccar/position-service

# Copy the built artifact from the build stage
COPY --from=build /app/target/*.jar /opt/traccar/position-service/position-service.jar

# Copy configuration files if needed
COPY position-service/config/ /opt/traccar/position-service/config/

# Install curl for health checks
RUN apt-get update && apt-get install -y --no-install-recommends curl && \
    apt-get clean && \
    rm -rf /var/lib/apt/lists/*

# Set proper permissions
RUN chown -R traccar:traccar /opt/traccar/position-service && \
    chmod -R 755 /opt/traccar/position-service

# Expose the service port
EXPOSE 8080

# Set user to non-root
USER traccar

# Configure health check
HEALTHCHECK --interval=30s --timeout=5s --start-period=60s --retries=3 \
  CMD curl -f http://localhost:8080/actuator/health || exit 1

# Set environment variables
ENV JAVA_OPTS="-Xms256m -Xmx512m -XX:+UseG1GC -XX:+UseContainerSupport"

# Run the application
ENTRYPOINT ["java", "$JAVA_OPTS", "-jar", "/opt/traccar/position-service/position-service.jar"]

# ===== Alpine Runtime Stage =====
FROM alpine:3.18 AS alpine

# Add labels for better maintainability
LABEL org.opencontainers.image.title="Traccar Position Processing Service (Alpine)"
LABEL org.opencontainers.image.description="Processes, validates, and enriches position data from the message broker"
LABEL org.opencontainers.image.vendor="Traccar"

# Install necessary packages
RUN apk add --no-cache tzdata curl ca-certificates && \
    addgroup -S traccar && adduser -S -u 1000 -G traccar traccar

# Set working directory
WORKDIR /opt/traccar/position-service

# Copy custom JRE from jlink stage
COPY --from=jre-build /javaruntime /opt/java/openjdk

# Copy the built artifact from the build stage
COPY --from=build /app/target/*.jar /opt/traccar/position-service/position-service.jar

# Copy configuration files if needed
COPY position-service/config/ /opt/traccar/position-service/config/

# Set proper permissions
RUN chown -R traccar:traccar /opt/traccar/position-service && \
    chmod -R 755 /opt/traccar/position-service

# Expose the service port
EXPOSE 8080

# Set user to non-root
USER traccar

# Configure health check
HEALTHCHECK --interval=30s --timeout=5s --start-period=60s --retries=3 \
  CMD curl -f http://localhost:8080/actuator/health || exit 1

# Set environment variables
ENV PATH="/opt/java/openjdk/bin:$PATH"
ENV JAVA_OPTS="-Xms256m -Xmx512m -XX:+UseG1GC -XX:+UseContainerSupport"

# Run the application
ENTRYPOINT ["java", "$JAVA_OPTS", "-jar", "/opt/traccar/position-service/position-service.jar"]

# Default to standard image
FROM standard
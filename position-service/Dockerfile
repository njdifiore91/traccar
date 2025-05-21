# Multi-stage build for Position Processing Service

# Build stage
FROM eclipse-temurin:17-jdk-alpine AS build

# Set working directory
WORKDIR /app

# Copy maven configuration files
COPY pom.xml ./
COPY mvnw ./
COPY .mvn ./.mvn

# Download dependencies (this layer will be cached unless pom.xml changes)
RUN ./mvnw dependency:go-offline -B

# Copy source code
COPY src ./src

# Build the application
RUN ./mvnw package -DskipTests

# Create a custom JRE using jlink to reduce image size
RUN jlink \
    --add-modules java.base,java.logging,java.xml,java.sql,java.naming,java.desktop,java.management,java.security.jgss,java.instrument,jdk.unsupported \
    --strip-debug \
    --no-man-pages \
    --no-header-files \
    --compress=2 \
    --output /customjre

# Runtime stage
FROM alpine:3.18

# Add labels for better maintainability
LABEL org.opencontainers.image.title="Traccar Position Service"
LABEL org.opencontainers.image.description="Position Processing Service for Traccar GPS Tracking System"
LABEL org.opencontainers.image.vendor="Traccar"

# Set environment variables
ENV JAVA_HOME=/opt/java/openjdk
ENV PATH="${JAVA_HOME}/bin:${PATH}"

# Install required packages
RUN apk add --no-cache tzdata curl bash && \
    mkdir -p /opt/java/openjdk

# Copy custom JRE from build stage
COPY --from=build /customjre $JAVA_HOME

# Create a non-root user to run the application
RUN addgroup -S traccar && adduser -S traccar -G traccar

# Create directories and set permissions
RUN mkdir -p /opt/traccar/position-service /opt/traccar/logs /opt/traccar/config && \
    chown -R traccar:traccar /opt/traccar

# Set working directory
WORKDIR /opt/traccar/position-service

# Copy the built artifact from the build stage
COPY --from=build --chown=traccar:traccar /app/target/*.jar /opt/traccar/position-service/position-service.jar

# Copy configuration files
COPY --chown=traccar:traccar src/main/resources/config.yml /opt/traccar/config/

# Expose ports
EXPOSE 8080 9090

# Switch to non-root user
USER traccar

# Set health check
HEALTHCHECK --interval=30s --timeout=5s --start-period=60s --retries=3 \
  CMD curl -f http://localhost:8080/actuator/health/liveness || exit 1

# Set JVM options optimized for containerized environment
ENV JAVA_OPTS="-XX:+UseContainerSupport -XX:MaxRAMPercentage=75.0 -XX:InitialRAMPercentage=50.0 -XX:+UseG1GC -XX:+HeapDumpOnOutOfMemoryError -XX:HeapDumpPath=/opt/traccar/logs/heap-dump.hprof -Djava.security.egd=file:/dev/./urandom -Dspring.config.location=file:/opt/traccar/config/"

# Run the application
ENTRYPOINT ["sh", "-c", "exec java $JAVA_OPTS -jar position-service.jar"]
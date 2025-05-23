# Multi-stage build for Traccar Reporting Service
# Stage 1: Build environment with full JDK
FROM eclipse-temurin:17-jdk AS builder

# Build arguments
ARG APP_USER=traccar
ARG APP_UID=1000
ARG APP_GID=1000

# Set working directory
WORKDIR /build

# Copy the reporting service source code
COPY reporting-service/src ./src
COPY reporting-service/pom.xml ./
COPY common/src/main/java/org/traccar/model ./common/src/main/java/org/traccar/model

# Build the application
RUN --mount=type=cache,target=/root/.m2 \
    mvn clean package -DskipTests

# Stage 2: Runtime environment with JRE slim
FROM eclipse-temurin:17-jre-jammy AS runtime

# Build arguments
ARG APP_USER=traccar
ARG APP_UID=1000
ARG APP_GID=1000
ARG JAVA_OPTS="-Xms512m -Xmx512m"

# Set environment variables
ENV JAVA_OPTS=${JAVA_OPTS} \
    LOG_LEVEL=INFO \
    CONFIG_FILE=/opt/traccar/conf/config.xml \
    DISCOVERY_TYPE=kubernetes \
    DISCOVERY_HOST=localhost \
    DISCOVERY_PORT=8500 \
    BROKER_TYPE=rabbitmq \
    BROKER_HOST=localhost \
    BROKER_PORT=5672 \
    SERVICE_PORT=8086

# Create non-root user and required directories
RUN groupadd -g ${APP_GID} ${APP_USER} && \
    useradd -u ${APP_UID} -g ${APP_GID} -s /bin/bash -m ${APP_USER} && \
    mkdir -p /opt/traccar/conf /opt/traccar/logs /opt/traccar/data /opt/traccar/templates && \
    chown -R ${APP_USER}:${APP_USER} /opt/traccar

# Install minimal required packages
RUN apt-get update && \
    apt-get install -y --no-install-recommends curl tzdata ca-certificates && \
    apt-get clean && \
    rm -rf /var/lib/apt/lists/*

# Set working directory
WORKDIR /opt/traccar

# Copy application from builder stage
COPY --from=builder --chown=${APP_USER}:${APP_USER} /build/target/reporting-service.jar /opt/traccar/
COPY --from=builder --chown=${APP_USER}:${APP_USER} /build/target/lib /opt/traccar/lib

# Copy configuration and templates
COPY --chown=${APP_USER}:${APP_USER} reporting-service/src/main/resources/config.xml /opt/traccar/conf/
COPY --chown=${APP_USER}:${APP_USER} reporting-service/src/main/resources/templates /opt/traccar/templates/

# Expose service port
EXPOSE ${SERVICE_PORT}

# Switch to non-root user
USER ${APP_USER}

# Health check
HEALTHCHECK --interval=30s --timeout=5s --start-period=60s --retries=3 \
    CMD curl -f http://localhost:${SERVICE_PORT}/health || exit 1

# Set entrypoint
ENTRYPOINT ["sh", "-c", "java ${JAVA_OPTS} -jar /opt/traccar/reporting-service.jar"]

# Stage 3: Alpine-based minimal runtime
FROM eclipse-temurin:17-jre-alpine AS alpine

# Build arguments
ARG APP_USER=traccar
ARG APP_UID=1000
ARG APP_GID=1000
ARG JAVA_OPTS="-Xms512m -Xmx512m"

# Set environment variables
ENV JAVA_OPTS=${JAVA_OPTS} \
    LOG_LEVEL=INFO \
    CONFIG_FILE=/opt/traccar/conf/config.xml \
    DISCOVERY_TYPE=kubernetes \
    DISCOVERY_HOST=localhost \
    DISCOVERY_PORT=8500 \
    BROKER_TYPE=rabbitmq \
    BROKER_HOST=localhost \
    BROKER_PORT=5672 \
    SERVICE_PORT=8086

# Create non-root user and required directories
RUN addgroup -g ${APP_GID} ${APP_USER} && \
    adduser -u ${APP_UID} -G ${APP_USER} -s /bin/sh -D ${APP_USER} && \
    mkdir -p /opt/traccar/conf /opt/traccar/logs /opt/traccar/data /opt/traccar/templates && \
    chown -R ${APP_USER}:${APP_USER} /opt/traccar

# Install minimal required packages
RUN apk add --no-cache curl tzdata ca-certificates && \
    rm -rf /var/cache/apk/*

# Set working directory
WORKDIR /opt/traccar

# Copy application from builder stage
COPY --from=builder --chown=${APP_USER}:${APP_USER} /build/target/reporting-service.jar /opt/traccar/
COPY --from=builder --chown=${APP_USER}:${APP_USER} /build/target/lib /opt/traccar/lib

# Copy configuration and templates
COPY --chown=${APP_USER}:${APP_USER} reporting-service/src/main/resources/config.xml /opt/traccar/conf/
COPY --chown=${APP_USER}:${APP_USER} reporting-service/src/main/resources/templates /opt/traccar/templates/

# Expose service port
EXPOSE ${SERVICE_PORT}

# Switch to non-root user
USER ${APP_USER}

# Health check
HEALTHCHECK --interval=30s --timeout=5s --start-period=60s --retries=3 \
    CMD wget -q -O /dev/null http://localhost:${SERVICE_PORT}/health || exit 1

# Set entrypoint
ENTRYPOINT ["sh", "-c", "java ${JAVA_OPTS} -jar /opt/traccar/reporting-service.jar"]
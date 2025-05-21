# Build stage
FROM maven:3.9-eclipse-temurin-17 AS build
WORKDIR /app

# Copy the source code
COPY pom.xml .
# Download dependencies first to leverage Docker cache
RUN mvn dependency:go-offline

COPY src ./src

# Build the application
RUN mvn clean package -DskipTests

# Runtime stage
FROM eclipse-temurin:17-jre-jammy

# Set up a non-root user for security
RUN groupadd -r traccar && useradd -r -g traccar traccar

# Set working directory
WORKDIR /app

# Install curl for health checks
RUN apt-get update && apt-get install -y --no-install-recommends curl && \
    apt-get clean && \
    rm -rf /var/lib/apt/lists/*

# Copy the built JAR from the build stage
COPY --from=build /app/target/notification-service-*.jar /app/notification-service.jar

# Create directories with appropriate permissions
RUN mkdir -p /app/logs /app/config && chown -R traccar:traccar /app

# Set environment variables
# JVM configuration
ENV JAVA_OPTS="-Xms512m -Xmx1g -XX:+UseG1GC -XX:+HeapDumpOnOutOfMemoryError -XX:HeapDumpPath=/app/logs/"

# OpenTelemetry configuration for distributed tracing
ENV OTEL_SERVICE_NAME="notification-service"
ENV OTEL_TRACES_SAMPLER="parentbased_traceidratio"
ENV OTEL_TRACES_SAMPLER_ARG="0.1"
ENV OTEL_PROPAGATORS="tracecontext,baggage,b3"
ENV OTEL_EXPORTER_OTLP_ENDPOINT="http://otel-collector:4317"
ENV OTEL_RESOURCE_ATTRIBUTES="deployment.environment=production"

# Logging configuration
ENV LOG_LEVEL="INFO"
ENV LOG_FORMAT="json"
ENV LOG_INCLUDE_EXCEPTION_STACKTRACE="true"

# Metrics configuration
ENV MANAGEMENT_ENDPOINTS_WEB_EXPOSURE_INCLUDE="metrics,health,info,prometheus"
ENV MANAGEMENT_SERVER_PORT="8081"
ENV METRICS_NAMESPACE="traccar"
ENV MANAGEMENT_METRICS_EXPORT_PROMETHEUS_ENABLED="true"

# Health check configuration
ENV MANAGEMENT_ENDPOINT_HEALTH_SHOW_DETAILS="when_authorized"
ENV MANAGEMENT_ENDPOINT_HEALTH_GROUP_LIVENESS_INCLUDE="livenessState"
ENV MANAGEMENT_ENDPOINT_HEALTH_GROUP_READINESS_INCLUDE="readinessState,db,messageBroker"

# Kubernetes probe configuration
ENV MANAGEMENT_HEALTH_LIVENESSSTATE_ENABLED="true"
ENV MANAGEMENT_HEALTH_READINESSSTATE_ENABLED="true"

# Notification service specific configuration
# Default notification channels (can be overridden at runtime)
ENV NOTIFICATOR_TYPES="web,mail,sms,firebase,traccar,telegram,pushover"

# Email notification configuration
ENV MAIL_HOST=""
ENV MAIL_PORT="587"
ENV MAIL_USERNAME=""
ENV MAIL_PASSWORD=""
ENV MAIL_FROM=""
ENV MAIL_AUTH="true"
ENV MAIL_STARTTLS_ENABLE="true"
ENV MAIL_SSL_ENABLE="false"

# SMS notification configuration
ENV SMS_PROVIDER=""
ENV SMS_AUTHORIZATION=""

# Firebase notification configuration
ENV FIREBASE_SERVICE_ACCOUNT=""

# Telegram notification configuration
ENV TELEGRAM_BOT_TOKEN=""

# Pushover notification configuration
ENV PUSHOVER_USER_KEY=""
ENV PUSHOVER_APP_TOKEN=""

# Circuit breaker configuration (Resilience4j)
ENV RESILIENCE4J_CIRCUITBREAKER_INSTANCES_DEFAULT_FAILURERATE_THRESHOLD="50"
ENV RESILIENCE4J_CIRCUITBREAKER_INSTANCES_DEFAULT_WAIT_DURATION_IN_OPEN_STATE="60s"
ENV RESILIENCE4J_CIRCUITBREAKER_INSTANCES_DEFAULT_SLIDING_WINDOW_SIZE="100"
ENV RESILIENCE4J_CIRCUITBREAKER_INSTANCES_DEFAULT_PERMITTED_CALLS_IN_HALF_OPEN_STATE="10"
ENV RESILIENCE4J_CIRCUITBREAKER_INSTANCES_DEFAULT_SLIDING_WINDOW_TYPE="COUNT_BASED"
ENV RESILIENCE4J_CIRCUITBREAKER_INSTANCES_DEFAULT_AUTOMATIC_TRANSITION_FROM_OPEN_TO_HALF_OPEN_ENABLED="true"

# Rate limiter configuration
ENV RESILIENCE4J_RATELIMITER_INSTANCES_DEFAULT_LIMIT_FOR_PERIOD="100"
ENV RESILIENCE4J_RATELIMITER_INSTANCES_DEFAULT_LIMIT_REFRESH_PERIOD="1s"
ENV RESILIENCE4J_RATELIMITER_INSTANCES_DEFAULT_TIMEOUT_DURATION="0s"

# Service discovery configuration
# Consul service discovery
ENV SPRING_CLOUD_CONSUL_ENABLED="false"
ENV SPRING_CLOUD_CONSUL_HOST="consul"
ENV SPRING_CLOUD_CONSUL_PORT="8500"
ENV SPRING_CLOUD_CONSUL_DISCOVERY_INSTANCE_ID="${HOSTNAME:notification-service}"
ENV SPRING_CLOUD_CONSUL_DISCOVERY_HEALTHCHECKPATH="/actuator/health"

# Kubernetes service discovery
ENV SPRING_CLOUD_KUBERNETES_ENABLED="true"
ENV SPRING_CLOUD_KUBERNETES_DISCOVERY_ENABLED="true"
ENV SPRING_CLOUD_KUBERNETES_CONFIG_ENABLED="true"
ENV SPRING_CLOUD_KUBERNETES_SECRETS_ENABLED="true"

# Redis cache configuration (optional)
ENV SPRING_REDIS_ENABLED="false"
ENV SPRING_REDIS_HOST="redis"
ENV SPRING_REDIS_PORT="6379"

# Message broker configuration
# RabbitMQ configuration
ENV SPRING_RABBITMQ_HOST="rabbitmq"
ENV SPRING_RABBITMQ_PORT="5672"
ENV SPRING_RABBITMQ_USERNAME="guest"
ENV SPRING_RABBITMQ_PASSWORD="guest"
ENV SPRING_RABBITMQ_VIRTUAL_HOST="/"
ENV SPRING_RABBITMQ_SSL_ENABLED="false"

# Kafka configuration (alternative to RabbitMQ)
ENV SPRING_KAFKA_ENABLED="false"
ENV SPRING_KAFKA_BOOTSTRAP_SERVERS="kafka:9092"
ENV SPRING_KAFKA_CONSUMER_GROUP_ID="notification-service"
ENV SPRING_KAFKA_CONSUMER_AUTO_OFFSET_RESET="earliest"
ENV SPRING_KAFKA_CONSUMER_KEY_DESERIALIZER="org.apache.kafka.common.serialization.StringDeserializer"
ENV SPRING_KAFKA_CONSUMER_VALUE_DESERIALIZER="org.springframework.kafka.support.serializer.JsonDeserializer"
ENV SPRING_KAFKA_PRODUCER_KEY_SERIALIZER="org.apache.kafka.common.serialization.StringSerializer"
ENV SPRING_KAFKA_PRODUCER_VALUE_SERIALIZER="org.springframework.kafka.support.serializer.JsonSerializer"

# Message processing configuration
ENV MESSAGE_PROCESSING_DROP_THRESHOLD="0.1"
ENV MESSAGE_PROCESSING_BATCH_SIZE="100"
ENV MESSAGE_PROCESSING_MAX_RETRY_ATTEMPTS="3"

# Template configuration
ENV SPRING_VELOCITY_RESOURCE_LOADER_PATH="classpath:/templates/"
ENV SPRING_VELOCITY_PROPERTIES_INPUT_ENCODING="UTF-8"
ENV SPRING_VELOCITY_PROPERTIES_OUTPUT_ENCODING="UTF-8"
ENV TEMPLATE_CACHE_ENABLED="true"
ENV TEMPLATE_CACHE_TTL="3600"

# Expose ports
# - 8080: Main application port
# - 8081: Management port for metrics and health checks
EXPOSE 8080 8081

# Add labels for better container management
LABEL org.opencontainers.image.title="Traccar Notification Service"
LABEL org.opencontainers.image.description="Handles multi-channel notification delivery for Traccar GPS tracking system"
LABEL org.opencontainers.image.vendor="Traccar"
LABEL org.opencontainers.image.version="1.0.0"
LABEL org.opencontainers.image.url="https://www.traccar.org/"
LABEL org.opencontainers.image.documentation="https://www.traccar.org/documentation/"

# Volume for external configuration
VOLUME ["/app/config"]

# Switch to non-root user
USER traccar

# Health check configuration - checks both liveness and readiness
HEALTHCHECK --interval=30s --timeout=5s --start-period=60s --retries=3 \
  CMD curl -f http://localhost:8081/actuator/health/live && \
      curl -f http://localhost:8081/actuator/health/ready || exit 1

# Set the entrypoint
ENTRYPOINT ["sh", "-c", "java $JAVA_OPTS -jar notification-service.jar --spring.config.additional-location=file:/app/config/"]
#!/bin/bash

# Traccar Setup Script
# Supports both monolithic and microservices deployment options

set -e

# Default values
DEPLOYMENT_MODE="monolithic"
CONTAINER_ORCHESTRATION="none"
MONITORING="basic"
PRESERVECONFIG=0
INSTALL_DIR="/opt/traccar"
KUBERNETES_NAMESPACE="traccar"
HELM_RELEASE_NAME="traccar"
HELM_VALUES_FILE=""
DOCKER_COMPOSE_FILE=""

# Display help information
show_help() {
    echo "Traccar Setup Script"
    echo "Usage: $0 [OPTIONS]"
    echo ""
    echo "Options:"
    echo "  -m, --mode MODE           Deployment mode: 'monolithic' or 'microservices' (default: monolithic)"
    echo "  -o, --orchestration TYPE  Container orchestration: 'none', 'kubernetes', 'docker-compose' (default: none)"
    echo "  -i, --install-dir DIR     Installation directory for monolithic mode (default: /opt/traccar)"
    echo "  -n, --namespace NAME      Kubernetes namespace for microservices mode (default: traccar)"
    echo "  -r, --release-name NAME  Helm release name for Kubernetes deployment (default: traccar)"
    echo "  -v, --values-file FILE    Custom Helm values file for Kubernetes deployment"
    echo "  -c, --compose-file FILE   Custom Docker Compose file for docker-compose deployment"
    echo "  -h, --help               Display this help message"
    echo ""
    echo "Examples:"
    echo "  $0                                   # Install monolithic version as a systemd service"
    echo "  $0 --mode microservices --orchestration kubernetes  # Deploy microservices on Kubernetes"
    echo "  $0 --mode microservices --orchestration docker-compose  # Deploy microservices with Docker Compose"
    echo ""
}

# Parse command line arguments
while [[ $# -gt 0 ]]; do
    key="$1"
    case $key in
        -m|--mode)
            DEPLOYMENT_MODE="$2"
            shift
            shift
            ;;
        -o|--orchestration)
            CONTAINER_ORCHESTRATION="$2"
            shift
            shift
            ;;
        -i|--install-dir)
            INSTALL_DIR="$2"
            shift
            shift
            ;;
        -n|--namespace)
            KUBERNETES_NAMESPACE="$2"
            shift
            shift
            ;;
        -r|--release-name)
            HELM_RELEASE_NAME="$2"
            shift
            shift
            ;;
        -v|--values-file)
            HELM_VALUES_FILE="$2"
            shift
            shift
            ;;
        -c|--compose-file)
            DOCKER_COMPOSE_FILE="$2"
            shift
            shift
            ;;
        -h|--help)
            show_help
            exit 0
            ;;
        *)
            echo "Unknown option: $1"
            show_help
            exit 1
            ;;
    esac
done

# Validate deployment mode
if [[ "$DEPLOYMENT_MODE" != "monolithic" && "$DEPLOYMENT_MODE" != "microservices" ]]; then
    echo "Error: Invalid deployment mode. Must be 'monolithic' or 'microservices'."
    exit 1
fi

# Validate container orchestration
if [[ "$CONTAINER_ORCHESTRATION" != "none" && "$CONTAINER_ORCHESTRATION" != "kubernetes" && "$CONTAINER_ORCHESTRATION" != "docker-compose" ]]; then
    echo "Error: Invalid container orchestration. Must be 'none', 'kubernetes', or 'docker-compose'."
    exit 1
fi

# Check for incompatible options
if [[ "$DEPLOYMENT_MODE" == "monolithic" && "$CONTAINER_ORCHESTRATION" != "none" && "$CONTAINER_ORCHESTRATION" != "docker-compose" ]]; then
    echo "Error: Monolithic mode only supports 'none' or 'docker-compose' orchestration."
    exit 1
fi

# Check for required tools
check_required_tools() {
    case "$CONTAINER_ORCHESTRATION" in
        kubernetes)
            command -v kubectl >/dev/null 2>&1 || { echo "Error: kubectl is required but not installed."; exit 1; }
            command -v helm >/dev/null 2>&1 || { echo "Error: helm is required but not installed."; exit 1; }
            ;;
        docker-compose)
            command -v docker >/dev/null 2>&1 || { echo "Error: docker is required but not installed."; exit 1; }
            command -v docker-compose >/dev/null 2>&1 || { echo "Error: docker-compose is required but not installed."; exit 1; }
            ;;
    esac
}

# Install monolithic version as a systemd service
install_monolithic_systemd() {
    echo "Installing Traccar as a systemd service..."
    
    # Preserve existing configuration if present
    if [ -f "$INSTALL_DIR/conf/traccar.xml" ]; then
        cp "$INSTALL_DIR/conf/traccar.xml" "$INSTALL_DIR/conf/traccar.xml.saved"
        PRESERVECONFIG=1
    fi

    # Create installation directory
    mkdir -p "$INSTALL_DIR"
    
    # Copy files to installation directory
    cp -r * "$INSTALL_DIR"
    chmod -R go+rX "$INSTALL_DIR"

    # Restore saved configuration if exists
    if [ ${PRESERVECONFIG} -eq 1 ] && [ -f "$INSTALL_DIR/conf/traccar.xml.saved" ]; then
        mv -f "$INSTALL_DIR/conf/traccar.xml.saved" "$INSTALL_DIR/conf/traccar.xml"
    fi

    # Install systemd service
    mv "$INSTALL_DIR/traccar.service" /etc/systemd/system
    chmod 664 /etc/systemd/system/traccar.service

    # Configure health check if not already present
    if ! grep -q "<entry key='web.healthCheck'>true</entry>" "$INSTALL_DIR/conf/traccar.xml"; then
        sed -i '/<\/properties>/i \	<entry key="web.healthCheck">true</entry>' "$INSTALL_DIR/conf/traccar.xml"
    fi

    # Enable and start the service
    systemctl daemon-reload
    systemctl enable traccar.service
    
    echo "Traccar has been installed as a systemd service."
    echo "You can start it with: systemctl start traccar.service"
}

# Deploy monolithic version with Docker Compose
deploy_monolithic_docker_compose() {
    echo "Deploying Traccar monolithic version with Docker Compose..."
    
    # Use custom Docker Compose file if provided, otherwise use default
    if [[ -n "$DOCKER_COMPOSE_FILE" ]]; then
        if [[ ! -f "$DOCKER_COMPOSE_FILE" ]]; then
            echo "Error: Docker Compose file not found: $DOCKER_COMPOSE_FILE"
            exit 1
        fi
        COMPOSE_FILE="$DOCKER_COMPOSE_FILE"
    else
        # Create default Docker Compose file if not exists
        COMPOSE_FILE="./docker-compose.yml"
        if [[ ! -f "$COMPOSE_FILE" ]]; then
            cat > "$COMPOSE_FILE" << EOF
version: '3'

services:
  traccar:
    image: traccar/traccar:latest
    container_name: traccar
    restart: always
    ports:
      - "8082:8082"  # Web interface
      - "5000-5150:5000-5150"  # Device protocols
    volumes:
      - ./logs:/opt/traccar/logs:rw
      - ./conf:/opt/traccar/conf:rw
      - ./data:/opt/traccar/data:rw
    healthcheck:
      test: ["CMD", "wget", "-q", "--spider", "http://localhost:8082/api/server/healthcheck"]
      interval: 30s
      timeout: 10s
      retries: 3
      start_period: 30s
EOF
            echo "Created default Docker Compose file: $COMPOSE_FILE"
        fi
    fi
    
    # Create required directories
    mkdir -p ./logs ./conf ./data
    
    # Copy configuration if exists
    if [ -f "./conf/traccar.xml" ]; then
        cp "./conf/traccar.xml" "./conf/traccar.xml.backup"
    elif [ -f "./traccar.xml" ]; then
        cp "./traccar.xml" "./conf/traccar.xml"
    fi
    
    # Start the containers
    docker-compose -f "$COMPOSE_FILE" up -d
    
    echo "Traccar has been deployed with Docker Compose."
    echo "You can access the web interface at http://localhost:8082"
}

# Deploy microservices with Kubernetes/Helm
deploy_microservices_kubernetes() {
    echo "Deploying Traccar microservices on Kubernetes..."
    
    # Check if namespace exists, create if not
    if ! kubectl get namespace "$KUBERNETES_NAMESPACE" >/dev/null 2>&1; then
        echo "Creating Kubernetes namespace: $KUBERNETES_NAMESPACE"
        kubectl create namespace "$KUBERNETES_NAMESPACE"
    fi
    
    # Prepare Helm command
    HELM_CMD="helm upgrade --install $HELM_RELEASE_NAME ./setup/helm --namespace $KUBERNETES_NAMESPACE"
    
    # Add custom values file if provided
    if [[ -n "$HELM_VALUES_FILE" ]]; then
        if [[ ! -f "$HELM_VALUES_FILE" ]]; then
            echo "Error: Helm values file not found: $HELM_VALUES_FILE"
            exit 1
        fi
        HELM_CMD="$HELM_CMD -f $HELM_VALUES_FILE"
    fi
    
    # Execute Helm command
    echo "Executing: $HELM_CMD"
    eval "$HELM_CMD"
    
    # Wait for deployment to complete
    echo "Waiting for deployments to be ready..."
    kubectl -n "$KUBERNETES_NAMESPACE" wait --for=condition=available --timeout=300s deployment -l app.kubernetes.io/part-of=traccar
    
    # Display service information
    echo "\nTraccar microservices have been deployed to Kubernetes namespace: $KUBERNETES_NAMESPACE"
    echo "API Gateway service:"
    kubectl -n "$KUBERNETES_NAMESPACE" get svc -l app.kubernetes.io/name=api-gateway
}

# Deploy microservices with Docker Compose
deploy_microservices_docker_compose() {
    echo "Deploying Traccar microservices with Docker Compose..."
    
    # Use custom Docker Compose file if provided, otherwise use default
    if [[ -n "$DOCKER_COMPOSE_FILE" ]]; then
        if [[ ! -f "$DOCKER_COMPOSE_FILE" ]]; then
            echo "Error: Docker Compose file not found: $DOCKER_COMPOSE_FILE"
            exit 1
        fi
        COMPOSE_FILE="$DOCKER_COMPOSE_FILE"
    else
        # Create default Docker Compose file if not exists
        COMPOSE_FILE="./docker-compose.yml"
        if [[ ! -f "$COMPOSE_FILE" ]]; then
            cat > "$COMPOSE_FILE" << EOF
version: '3'

services:
  # Message broker
  kafka:
    image: bitnami/kafka:latest
    container_name: traccar-kafka
    ports:
      - "9092:9092"
    environment:
      - KAFKA_CFG_NODE_ID=0
      - KAFKA_CFG_PROCESS_ROLES=controller,broker
      - KAFKA_CFG_LISTENERS=PLAINTEXT://:9092,CONTROLLER://:9093
      - KAFKA_CFG_LISTENER_SECURITY_PROTOCOL_MAP=CONTROLLER:PLAINTEXT,PLAINTEXT:PLAINTEXT
      - KAFKA_CFG_CONTROLLER_QUORUM_VOTERS=0@kafka:9093
      - KAFKA_CFG_CONTROLLER_LISTENER_NAMES=CONTROLLER
    volumes:
      - kafka_data:/bitnami/kafka
    healthcheck:
      test: ["CMD-SHELL", "kafka-topics.sh --bootstrap-server localhost:9092 --list"]
      interval: 30s
      timeout: 10s
      retries: 3
      start_period: 30s

  # Protocol Service
  protocol-service:
    image: traccar/protocol:latest
    container_name: traccar-protocol
    restart: always
    ports:
      - "5000-5150:5000-5150"  # Device protocols
    environment:
      - KAFKA_BOOTSTRAP_SERVERS=kafka:9092
      - SPRING_DATASOURCE_URL=jdbc:postgresql://postgres:5432/traccar
      - SPRING_DATASOURCE_USERNAME=traccar
      - SPRING_DATASOURCE_PASSWORD=traccar
    depends_on:
      - kafka
      - postgres
    healthcheck:
      test: ["CMD", "wget", "-q", "--spider", "http://localhost:8090/health/readiness"]
      interval: 30s
      timeout: 10s
      retries: 3
      start_period: 30s

  # Position Service
  position-service:
    image: traccar/position:latest
    container_name: traccar-position
    restart: always
    environment:
      - KAFKA_BOOTSTRAP_SERVERS=kafka:9092
      - SPRING_DATASOURCE_URL=jdbc:postgresql://postgres:5432/traccar
      - SPRING_DATASOURCE_USERNAME=traccar
      - SPRING_DATASOURCE_PASSWORD=traccar
    depends_on:
      - kafka
      - postgres
    healthcheck:
      test: ["CMD", "wget", "-q", "--spider", "http://localhost:8081/health/readiness"]
      interval: 30s
      timeout: 10s
      retries: 3
      start_period: 30s

  # Event Service
  event-service:
    image: traccar/event:latest
    container_name: traccar-event
    restart: always
    environment:
      - KAFKA_BOOTSTRAP_SERVERS=kafka:9092
      - SPRING_DATASOURCE_URL=jdbc:postgresql://postgres:5432/traccar
      - SPRING_DATASOURCE_USERNAME=traccar
      - SPRING_DATASOURCE_PASSWORD=traccar
    depends_on:
      - kafka
      - postgres
    healthcheck:
      test: ["CMD", "wget", "-q", "--spider", "http://localhost:8082/health/readiness"]
      interval: 30s
      timeout: 10s
      retries: 3
      start_period: 30s

  # Notification Service
  notification-service:
    image: traccar/notification:latest
    container_name: traccar-notification
    restart: always
    environment:
      - KAFKA_BOOTSTRAP_SERVERS=kafka:9092
      - SPRING_DATASOURCE_URL=jdbc:postgresql://postgres:5432/traccar
      - SPRING_DATASOURCE_USERNAME=traccar
      - SPRING_DATASOURCE_PASSWORD=traccar
    depends_on:
      - kafka
      - postgres
    healthcheck:
      test: ["CMD", "wget", "-q", "--spider", "http://localhost:8083/health/readiness"]
      interval: 30s
      timeout: 10s
      retries: 3
      start_period: 30s

  # API Gateway
  api-gateway:
    image: traccar/api-gateway:latest
    container_name: traccar-api-gateway
    restart: always
    ports:
      - "8080:8080"  # Web interface
    environment:
      - SPRING_DATASOURCE_URL=jdbc:postgresql://postgres:5432/traccar
      - SPRING_DATASOURCE_USERNAME=traccar
      - SPRING_DATASOURCE_PASSWORD=traccar
    depends_on:
      - protocol-service
      - position-service
      - event-service
      - notification-service
      - reporting-service
    healthcheck:
      test: ["CMD", "wget", "-q", "--spider", "http://localhost:8080/health/readiness"]
      interval: 30s
      timeout: 10s
      retries: 3
      start_period: 30s

  # Reporting Service
  reporting-service:
    image: traccar/reporting:latest
    container_name: traccar-reporting
    restart: always
    environment:
      - KAFKA_BOOTSTRAP_SERVERS=kafka:9092
      - SPRING_DATASOURCE_URL=jdbc:postgresql://postgres:5432/traccar
      - SPRING_DATASOURCE_USERNAME=traccar
      - SPRING_DATASOURCE_PASSWORD=traccar
    depends_on:
      - kafka
      - postgres
    healthcheck:
      test: ["CMD", "wget", "-q", "--spider", "http://localhost:8084/health/readiness"]
      interval: 30s
      timeout: 10s
      retries: 3
      start_period: 30s

  # Database
  postgres:
    image: postgres:14-alpine
    container_name: traccar-postgres
    restart: always
    environment:
      - POSTGRES_USER=traccar
      - POSTGRES_PASSWORD=traccar
      - POSTGRES_DB=traccar
    volumes:
      - postgres_data:/var/lib/postgresql/data
    healthcheck:
      test: ["CMD-SHELL", "pg_isready -U traccar"]
      interval: 10s
      timeout: 5s
      retries: 5

  # Prometheus for monitoring (optional)
  prometheus:
    image: prom/prometheus:latest
    container_name: traccar-prometheus
    restart: always
    ports:
      - "9090:9090"
    volumes:
      - ./prometheus.yml:/etc/prometheus/prometheus.yml
      - prometheus_data:/prometheus
    command:
      - '--config.file=/etc/prometheus/prometheus.yml'
      - '--storage.tsdb.path=/prometheus'
      - '--web.console.libraries=/usr/share/prometheus/console_libraries'
      - '--web.console.templates=/usr/share/prometheus/consoles'

  # Grafana for dashboards (optional)
  grafana:
    image: grafana/grafana:latest
    container_name: traccar-grafana
    restart: always
    ports:
      - "3000:3000"
    environment:
      - GF_SECURITY_ADMIN_USER=admin
      - GF_SECURITY_ADMIN_PASSWORD=admin
    volumes:
      - grafana_data:/var/lib/grafana
    depends_on:
      - prometheus

volumes:
  kafka_data:
  postgres_data:
  prometheus_data:
  grafana_data:
EOF
            echo "Created default Docker Compose file: $COMPOSE_FILE"
            
            # Create Prometheus config file
            cat > "./prometheus.yml" << EOF
global:
  scrape_interval: 15s
  evaluation_interval: 15s

scrape_configs:
  - job_name: 'protocol-service'
    static_configs:
      - targets: ['protocol-service:8090']

  - job_name: 'position-service'
    static_configs:
      - targets: ['position-service:8081']

  - job_name: 'event-service'
    static_configs:
      - targets: ['event-service:8082']

  - job_name: 'notification-service'
    static_configs:
      - targets: ['notification-service:8083']

  - job_name: 'api-gateway'
    static_configs:
      - targets: ['api-gateway:8080']

  - job_name: 'reporting-service'
    static_configs:
      - targets: ['reporting-service:8084']

  - job_name: 'kafka'
    static_configs:
      - targets: ['kafka:9092']
EOF
            echo "Created Prometheus configuration file: ./prometheus.yml"
        fi
    fi
    
    # Start the containers
    docker-compose -f "$COMPOSE_FILE" up -d
    
    echo "Traccar microservices have been deployed with Docker Compose."
    echo "You can access the web interface at http://localhost:8080"
    echo "Monitoring dashboard is available at http://localhost:3000 (admin/admin)"
}

# Main execution
echo "Traccar Setup"
echo "Deployment Mode: $DEPLOYMENT_MODE"
echo "Container Orchestration: $CONTAINER_ORCHESTRATION"
echo ""

# Check for required tools
check_required_tools

# Execute the appropriate installation/deployment method
case "$DEPLOYMENT_MODE" in
    monolithic)
        case "$CONTAINER_ORCHESTRATION" in
            none)
                install_monolithic_systemd
                ;;
            docker-compose)
                deploy_monolithic_docker_compose
                ;;
        esac
        ;;
    microservices)
        case "$CONTAINER_ORCHESTRATION" in
            kubernetes)
                deploy_microservices_kubernetes
                ;;
            docker-compose)
                deploy_microservices_docker_compose
                ;;
            *)
                echo "Error: Microservices deployment requires container orchestration."
                exit 1
                ;;
        esac
        ;;
    *)
        echo "Error: Invalid deployment mode."
        exit 1
        ;;
esac

# Clean up installation files if needed
if [[ "$DEPLOYMENT_MODE" == "monolithic" && "$CONTAINER_ORCHESTRATION" == "none" ]]; then
    rm -f "$INSTALL_DIR/setup.sh"
    if [ -d "../out" ]; then
        rm -r "../out"
    fi
fi

echo ""
echo "Traccar setup completed successfully!"
#!/bin/bash
# Make script executable with: chmod +x kubernetes-setup.sh

# Traccar Microservices Kubernetes Setup Script
# This script automates the deployment of Traccar microservices to a Kubernetes cluster
# Requirements:
# - kubectl installed and configured to access your cluster
# - Kubernetes 1.25+ cluster
# - Helm 3.11+ installed

set -e

# Configuration variables
TRACCAR_VERSION="1.0.0"
KUBERNETES_NAMESPACE_PREFIX="traccar"
MESSAGE_BROKER="kafka" # Options: kafka, rabbitmq
SERVICE_DISCOVERY="kubernetes" # Options: kubernetes, consul
CHART_REPO="https://traccar.github.io/helm-charts"
CHART_NAME="traccar"
RELEASE_NAME="traccar"

# Color codes for output
RED='\033[0;31m'
GREEN='\033[0;32m'
YELLOW='\033[0;33m'
BLUE='\033[0;34m'
NC='\033[0m' # No Color

# Print banner
echo -e "${BLUE}"
echo "╔════════════════════════════════════════════════════════════╗"
echo "║                                                            ║"
echo "║             Traccar Microservices Setup Script             ║"
echo "║                                                            ║"
echo "╚════════════════════════════════════════════════════════════╝"
echo -e "${NC}"

# Function to check prerequisites
check_prerequisites() {
    echo -e "${YELLOW}Checking prerequisites...${NC}"
    
    # Check kubectl
    if ! command -v kubectl &> /dev/null; then
        echo -e "${RED}Error: kubectl is not installed. Please install kubectl first.${NC}"
        exit 1
    fi
    
    # Check Kubernetes connection
    if ! kubectl cluster-info &> /dev/null; then
        echo -e "${RED}Error: Cannot connect to Kubernetes cluster. Please check your kubeconfig.${NC}"
        exit 1
    fi
    
    # Check Kubernetes version
    K8S_VERSION=$(kubectl version --short | grep 'Server Version' | awk '{print $3}' | cut -d. -f1,2 | sed 's/v//')
    if (( $(echo "$K8S_VERSION < 1.25" | bc -l) )); then
        echo -e "${RED}Error: Kubernetes version 1.25+ is required. Current version: $K8S_VERSION${NC}"
        exit 1
    fi
    
    # Check Helm
    if ! command -v helm &> /dev/null; then
        echo -e "${RED}Error: Helm is not installed. Please install Helm 3.11+ first.${NC}"
        exit 1
    fi
    
    # Check Helm version
    HELM_VERSION=$(helm version --short | cut -d. -f1,2 | sed 's/v//')
    if (( $(echo "$HELM_VERSION < 3.11" | bc -l) )); then
        echo -e "${RED}Error: Helm version 3.11+ is required. Current version: $HELM_VERSION${NC}"
        exit 1
    fi
    
    echo -e "${GREEN}All prerequisites satisfied.${NC}"
}

# Function to create namespaces
create_namespaces() {
    echo -e "${YELLOW}Creating namespaces for Traccar microservices...${NC}"
    
    # Create namespaces for each service
    kubectl create namespace ${KUBERNETES_NAMESPACE_PREFIX}-protocol --dry-run=client -o yaml | kubectl apply -f -
    kubectl create namespace ${KUBERNETES_NAMESPACE_PREFIX}-position --dry-run=client -o yaml | kubectl apply -f -
    kubectl create namespace ${KUBERNETES_NAMESPACE_PREFIX}-event --dry-run=client -o yaml | kubectl apply -f -
    kubectl create namespace ${KUBERNETES_NAMESPACE_PREFIX}-notification --dry-run=client -o yaml | kubectl apply -f -
    kubectl create namespace ${KUBERNETES_NAMESPACE_PREFIX}-api-gateway --dry-run=client -o yaml | kubectl apply -f -
    kubectl create namespace ${KUBERNETES_NAMESPACE_PREFIX}-reporting --dry-run=client -o yaml | kubectl apply -f -
    kubectl create namespace ${KUBERNETES_NAMESPACE_PREFIX}-infrastructure --dry-run=client -o yaml | kubectl apply -f -
    
    echo -e "${GREEN}Namespaces created successfully.${NC}"
}

# Function to configure Helm
configure_helm() {
    echo -e "${YELLOW}Configuring Helm...${NC}"
    
    # Add Traccar Helm repository
    helm repo add traccar ${CHART_REPO}
    helm repo update
    
    echo -e "${GREEN}Helm configured successfully.${NC}"
}

# Function to deploy message broker
deploy_message_broker() {
    echo -e "${YELLOW}Deploying message broker (${MESSAGE_BROKER})...${NC}"
    
    if [ "${MESSAGE_BROKER}" == "kafka" ]; then
        # Add Bitnami repository for Kafka
        helm repo add bitnami https://charts.bitnami.com/bitnami
        helm repo update
        
        # Deploy Kafka
        helm upgrade --install kafka bitnami/kafka \
            --namespace ${KUBERNETES_NAMESPACE_PREFIX}-infrastructure \
            --set replicaCount=3 \
            --set persistence.enabled=true \
            --set persistence.size=10Gi \
            --set zookeeper.enabled=true \
            --set zookeeper.replicaCount=3 \
            --set zookeeper.persistence.enabled=true \
            --set zookeeper.persistence.size=5Gi \
            --set metrics.kafka.enabled=true \
            --set metrics.jmx.enabled=true
    elif [ "${MESSAGE_BROKER}" == "rabbitmq" ]; then
        # Add Bitnami repository for RabbitMQ
        helm repo add bitnami https://charts.bitnami.com/bitnami
        helm repo update
        
        # Deploy RabbitMQ
        helm upgrade --install rabbitmq bitnami/rabbitmq \
            --namespace ${KUBERNETES_NAMESPACE_PREFIX}-infrastructure \
            --set replicaCount=3 \
            --set persistence.enabled=true \
            --set persistence.size=8Gi \
            --set metrics.enabled=true \
            --set clustering.enabled=true
    else
        echo -e "${RED}Error: Unsupported message broker: ${MESSAGE_BROKER}. Supported options: kafka, rabbitmq${NC}"
        exit 1
    fi
    
    echo -e "${GREEN}Message broker deployed successfully.${NC}"
}

# Function to deploy service discovery
deploy_service_discovery() {
    echo -e "${YELLOW}Setting up service discovery (${SERVICE_DISCOVERY})...${NC}"
    
    if [ "${SERVICE_DISCOVERY}" == "consul" ]; then
        # Add HashiCorp repository for Consul
        helm repo add hashicorp https://helm.releases.hashicorp.com
        helm repo update
        
        # Deploy Consul
        helm upgrade --install consul hashicorp/consul \
            --namespace ${KUBERNETES_NAMESPACE_PREFIX}-infrastructure \
            --set server.replicas=3 \
            --set server.storage=10Gi \
            --set ui.enabled=true \
            --set connectInject.enabled=true \
            --set controller.enabled=true
    elif [ "${SERVICE_DISCOVERY}" == "kubernetes" ]; then
        echo -e "${GREEN}Using native Kubernetes service discovery.${NC}"
        # No additional setup needed as we'll use Kubernetes native service discovery
    else
        echo -e "${RED}Error: Unsupported service discovery: ${SERVICE_DISCOVERY}. Supported options: kubernetes, consul${NC}"
        exit 1
    fi
    
    echo -e "${GREEN}Service discovery configured successfully.${NC}"
}

# Function to deploy Traccar microservices
deploy_traccar_microservices() {
    echo -e "${YELLOW}Deploying Traccar microservices...${NC}"
    
    # Create values file with configuration
    cat > traccar-values.yaml << EOF
global:
  imageRegistry: ""
  imageTag: "${TRACCAR_VERSION}"
  messageBroker:
    type: "${MESSAGE_BROKER}"
    host: "${MESSAGE_BROKER}.${KUBERNETES_NAMESPACE_PREFIX}-infrastructure.svc.cluster.local"
  serviceDiscovery:
    type: "${SERVICE_DISCOVERY}"

protocolService:
  enabled: true
  replicaCount: 2
  resources:
    requests:
      cpu: 500m
      memory: 512Mi
    limits:
      cpu: 1000m
      memory: 1Gi
  autoscaling:
    enabled: true
    minReplicas: 2
    maxReplicas: 10
    targetCPUUtilizationPercentage: 70
    targetMemoryUtilizationPercentage: 80

positionService:
  enabled: true
  replicaCount: 2
  resources:
    requests:
      cpu: 1000m
      memory: 1Gi
    limits:
      cpu: 2000m
      memory: 2Gi
  autoscaling:
    enabled: true
    minReplicas: 2
    maxReplicas: 10
    targetCPUUtilizationPercentage: 70
    targetMemoryUtilizationPercentage: 80

eventService:
  enabled: true
  replicaCount: 2
  resources:
    requests:
      cpu: 500m
      memory: 512Mi
    limits:
      cpu: 1000m
      memory: 1Gi
  autoscaling:
    enabled: true
    minReplicas: 2
    maxReplicas: 10
    targetCPUUtilizationPercentage: 70
    targetMemoryUtilizationPercentage: 80

notificationService:
  enabled: true
  replicaCount: 2
  resources:
    requests:
      cpu: 300m
      memory: 256Mi
    limits:
      cpu: 500m
      memory: 512Mi
  autoscaling:
    enabled: true
    minReplicas: 2
    maxReplicas: 10
    targetCPUUtilizationPercentage: 70
    targetMemoryUtilizationPercentage: 80

apiGatewayService:
  enabled: true
  replicaCount: 2
  resources:
    requests:
      cpu: 300m
      memory: 256Mi
    limits:
      cpu: 500m
      memory: 512Mi
  autoscaling:
    enabled: true
    minReplicas: 2
    maxReplicas: 10
    targetCPUUtilizationPercentage: 70
    targetMemoryUtilizationPercentage: 80
  ingress:
    enabled: true
    annotations:
      kubernetes.io/ingress.class: nginx
      nginx.ingress.kubernetes.io/ssl-redirect: "false"
    hosts:
      - host: traccar.local
        paths:
          - path: /
            pathType: Prefix

reportingService:
  enabled: true
  replicaCount: 2
  resources:
    requests:
      cpu: 500m
      memory: 512Mi
    limits:
      cpu: 1000m
      memory: 1Gi
  autoscaling:
    enabled: true
    minReplicas: 2
    maxReplicas: 10
    targetCPUUtilizationPercentage: 70
    targetMemoryUtilizationPercentage: 80

monitoring:
  enabled: true
  prometheus:
    enabled: true
  grafana:
    enabled: true
    dashboards:
      enabled: true

persistence:
  enabled: true
  storageClass: ""
  size: 10Gi
EOF
    
    # Deploy Traccar microservices using Helm
    helm upgrade --install ${RELEASE_NAME} traccar/${CHART_NAME} \
        --values traccar-values.yaml \
        --namespace ${KUBERNETES_NAMESPACE_PREFIX}-api-gateway
    
    echo -e "${GREEN}Traccar microservices deployed successfully.${NC}"
}

# Function to configure health checks and monitoring
configure_monitoring() {
    echo -e "${YELLOW}Configuring health checks and monitoring...${NC}"
    
    # Add Prometheus Operator repository
    helm repo add prometheus-community https://prometheus-community.github.io/helm-charts
    helm repo update
    
    # Deploy Prometheus Operator for monitoring
    helm upgrade --install prometheus prometheus-community/kube-prometheus-stack \
        --namespace ${KUBERNETES_NAMESPACE_PREFIX}-infrastructure \
        --set prometheus.prometheusSpec.serviceMonitorSelectorNilUsesHelmValues=false \
        --set prometheus.prometheusSpec.podMonitorSelectorNilUsesHelmValues=false
    
    # Create ServiceMonitor for Traccar services
    cat > traccar-service-monitor.yaml << EOF
apiVersion: monitoring.coreos.com/v1
kind: ServiceMonitor
metadata:
  name: traccar-services
  namespace: ${KUBERNETES_NAMESPACE_PREFIX}-infrastructure
  labels:
    release: prometheus
spec:
  selector:
    matchLabels:
      app.kubernetes.io/part-of: traccar
  namespaceSelector:
    matchNames:
    - ${KUBERNETES_NAMESPACE_PREFIX}-protocol
    - ${KUBERNETES_NAMESPACE_PREFIX}-position
    - ${KUBERNETES_NAMESPACE_PREFIX}-event
    - ${KUBERNETES_NAMESPACE_PREFIX}-notification
    - ${KUBERNETES_NAMESPACE_PREFIX}-api-gateway
    - ${KUBERNETES_NAMESPACE_PREFIX}-reporting
  endpoints:
  - port: metrics
    interval: 15s
    path: /metrics
EOF
    
    kubectl apply -f traccar-service-monitor.yaml
    
    echo -e "${GREEN}Health checks and monitoring configured successfully.${NC}"
}

# Function to verify deployment
verify_deployment() {
    echo -e "${YELLOW}Verifying deployment...${NC}"
    
    # Wait for all pods to be ready
    echo "Waiting for Protocol Service pods to be ready..."
    kubectl wait --for=condition=ready pod -l app.kubernetes.io/name=protocol-service --timeout=300s -n ${KUBERNETES_NAMESPACE_PREFIX}-protocol
    
    echo "Waiting for Position Service pods to be ready..."
    kubectl wait --for=condition=ready pod -l app.kubernetes.io/name=position-service --timeout=300s -n ${KUBERNETES_NAMESPACE_PREFIX}-position
    
    echo "Waiting for Event Service pods to be ready..."
    kubectl wait --for=condition=ready pod -l app.kubernetes.io/name=event-service --timeout=300s -n ${KUBERNETES_NAMESPACE_PREFIX}-event
    
    echo "Waiting for Notification Service pods to be ready..."
    kubectl wait --for=condition=ready pod -l app.kubernetes.io/name=notification-service --timeout=300s -n ${KUBERNETES_NAMESPACE_PREFIX}-notification
    
    echo "Waiting for API Gateway Service pods to be ready..."
    kubectl wait --for=condition=ready pod -l app.kubernetes.io/name=api-gateway-service --timeout=300s -n ${KUBERNETES_NAMESPACE_PREFIX}-api-gateway
    
    echo "Waiting for Reporting Service pods to be ready..."
    kubectl wait --for=condition=ready pod -l app.kubernetes.io/name=reporting-service --timeout=300s -n ${KUBERNETES_NAMESPACE_PREFIX}-reporting
    
    # Get service endpoints
    API_GATEWAY_URL=$(kubectl get ingress -n ${KUBERNETES_NAMESPACE_PREFIX}-api-gateway -o jsonpath='{.items[0].spec.rules[0].host}')
    
    echo -e "\n${GREEN}Deployment verification completed.${NC}"
    echo -e "\n${BLUE}Traccar microservices have been successfully deployed!${NC}"
    echo -e "\n${YELLOW}API Gateway URL: http://${API_GATEWAY_URL}${NC}"
    
    if [ "${SERVICE_DISCOVERY}" == "consul" ]; then
        CONSUL_URL=$(kubectl get ingress -n ${KUBERNETES_NAMESPACE_PREFIX}-infrastructure -l app=consul -o jsonpath='{.items[0].spec.rules[0].host}')
        echo -e "${YELLOW}Consul UI: http://${CONSUL_URL}${NC}"
    fi
    
    if [ "${MESSAGE_BROKER}" == "kafka" ]; then
        echo -e "${YELLOW}Kafka Broker: ${MESSAGE_BROKER}.${KUBERNETES_NAMESPACE_PREFIX}-infrastructure.svc.cluster.local:9092${NC}"
    elif [ "${MESSAGE_BROKER}" == "rabbitmq" ]; then
        RABBITMQ_PASSWORD=$(kubectl get secret -n ${KUBERNETES_NAMESPACE_PREFIX}-infrastructure rabbitmq -o jsonpath="{.data.rabbitmq-password}" | base64 --decode)
        echo -e "${YELLOW}RabbitMQ Management URL: http://rabbitmq.${KUBERNETES_NAMESPACE_PREFIX}-infrastructure.svc.cluster.local:15672${NC}"
        echo -e "${YELLOW}RabbitMQ Username: user${NC}"
        echo -e "${YELLOW}RabbitMQ Password: ${RABBITMQ_PASSWORD}${NC}"
    fi
    
    GRAFANA_PASSWORD=$(kubectl get secret -n ${KUBERNETES_NAMESPACE_PREFIX}-infrastructure prometheus-grafana -o jsonpath="{.data.admin-password}" | base64 --decode)
    echo -e "${YELLOW}Grafana URL: http://prometheus-grafana.${KUBERNETES_NAMESPACE_PREFIX}-infrastructure.svc.cluster.local${NC}"
    echo -e "${YELLOW}Grafana Username: admin${NC}"
    echo -e "${YELLOW}Grafana Password: ${GRAFANA_PASSWORD}${NC}"
}

# Main execution
main() {
    check_prerequisites
    create_namespaces
    configure_helm
    deploy_message_broker
    deploy_service_discovery
    deploy_traccar_microservices
    configure_monitoring
    verify_deployment
}

# Execute main function
main
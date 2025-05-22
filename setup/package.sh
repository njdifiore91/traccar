#!/usr/bin/env bash

#
# Script to create installers for various platforms and build container images for microservices.
#

cd $(dirname $0)

usage () {
  echo "Usage: $0 VERSION [PLATFORM|CONTAINER] [OPTIONS]"
  echo "Build Traccar installers or container images."
  echo
  echo "Without PLATFORM or CONTAINER provided, builds installers for all platforms."
  echo
  echo "Available platforms for traditional installers:"
  echo " * linux-64"
  echo " * linux-arm"
  echo " * windows-64"
  echo " * other"
  echo
  echo "Container options:"
  echo " * container - Build container images for all microservices"
  echo " * container:<service> - Build container image for specific service"
  echo "   Available services: protocol, position, event, notification, api-gateway, reporting"
  echo
  echo "Additional options for container builds:"
  echo " * --registry=<registry> - Container registry to push images to (e.g., ecr, gcr, acr)"
  echo " * --registry-url=<url> - Container registry URL"
  echo " * --push - Push images to registry"
  echo " * --scan - Scan images for vulnerabilities"
  echo " * --helm - Build Helm charts"
  echo " * --alpine - Build Alpine-based images"
  exit 1
}

if [[ $# -lt 1 ]]
then
  usage
fi

info () {
  echo -e "[\033[1;34mINFO\033[0m] "$1
}

ok () {
  echo -e "[\033[1;32m OK \033[0m] "$1
}

warn () {
  echo -e "[\033[1;31mWARN\033[0m] "$1
}

error () {
  echo -e "[\033[1;31mERROR\033[0m] "$1
  exit 1
}

VERSION=$1
PLATFORM=${2:-all}
PREREQ=true

# Parse additional options
REGISTRY=""
REGISTRY_URL=""
PUSH=false
SCAN=false
HELM=false
ALPINE=false

for arg in "$@"
do
  case $arg in
    --registry=*)
      REGISTRY="${arg#*=}"
      ;;
    --registry-url=*)
      REGISTRY_URL="${arg#*=}"
      ;;
    --push)
      PUSH=true
      ;;
    --scan)
      SCAN=true
      ;;
    --helm)
      HELM=true
      ;;
    --alpine)
      ALPINE=true
      ;;
  esac
shift
done

# Set registry URL based on registry type if not explicitly provided
if [ -n "$REGISTRY" ] && [ -z "$REGISTRY_URL" ]; then
  case $REGISTRY in
    ecr)
      # AWS ECR requires AWS account ID and region
      if [ -z "$AWS_ACCOUNT_ID" ] || [ -z "$AWS_REGION" ]; then
        error "AWS_ACCOUNT_ID and AWS_REGION environment variables must be set for ECR"
      fi
      REGISTRY_URL="$AWS_ACCOUNT_ID.dkr.ecr.$AWS_REGION.amazonaws.com"
      ;;
    gcr)
      # GCR requires GCP project ID
      if [ -z "$GCP_PROJECT_ID" ]; then
        error "GCP_PROJECT_ID environment variable must be set for GCR"
      fi
      REGISTRY_URL="gcr.io/$GCP_PROJECT_ID"
      ;;
    acr)
      # ACR requires ACR name
      if [ -z "$ACR_NAME" ]; then
        error "ACR_NAME environment variable must be set for ACR"
      fi
      REGISTRY_URL="$ACR_NAME.azurecr.io"
      ;;
  esac
fi

check_requirement () {
  if ! eval $2 &>/dev/null
  then
	warn "$3"
	PREREQ=false
  else
	ok "$@"
  fi
}

# Check if we're building containers or traditional installers
if [[ $PLATFORM == container* ]]; then
  # Container build requirements
  info "Checking container build requirements"
  check_requirement "Docker" "which docker" "Missing docker binary"
  
  if [ "$SCAN" = true ]; then
    check_requirement "Trivy" "which trivy" "Missing trivy binary for vulnerability scanning"
  fi
  
  if [ "$HELM" = true ]; then
    check_requirement "Helm" "which helm" "Missing helm binary for chart building"
  fi
  
  if [ "$PUSH" = true ] && [ -z "$REGISTRY_URL" ]; then
    warn "Registry URL not provided, cannot push images"
    PUSH=false
  fi
else
  # Traditional installer requirements
  info "Checking build requirements for platform: "$PLATFORM
  check_requirement "Traccar server archive" "ls ../target/tracker-server.jar" "Missing traccar archive"
  check_requirement "Zip" "which zip" "Missing zip binary"
  check_requirement "Unzip" "which unzip" "Missing unzip binary"
  if [ $PLATFORM != "other" ]; then
    check_requirement "Jlink" "which jlink" "Missing jlink binary"
  fi
  if [ $PLATFORM = "all" -o $PLATFORM = "windows-64" ]; then
    check_requirement "Inno Extractor" "which innoextract" "Missing innoextract binary"
    check_requirement "Inno Setup" "ls i*setup-*.exe" "Missing Inno Setup (http://www.jrsoftware.org/isdl.php)"
    check_requirement "Windows 64 Java" "ls OpenJDK*64_windows*.zip" "Missing Windows 64 JDK (https://adoptium.net/)"
    check_requirement "Wine" "which wine" "Missing wine binary"
  fi
  if [ $PLATFORM = "all" -o $PLATFORM = "linux-64" -o $PLATFORM = "linux-arm" ]; then
    check_requirement "Makeself" "which makeself" "Missing makeself binary"
  fi
  if [ $PLATFORM = "all" -o $PLATFORM = "linux-64" ]; then
    check_requirement "Linux 64 Java" "ls OpenJDK*x64_linux*.tar.gz" "Missing Linux 64 JDK (https://adoptium.net/)"
  fi
  if [ $PLATFORM = "all" -o $PLATFORM = "linux-arm" ]; then
    check_requirement "Linux ARM Java" "ls OpenJDK*aarch64_linux*.tar.gz" "Missing Linux ARM JDK (https://adoptium.net/)"
  fi
fi

if [ $PREREQ = false ]; then
  info "Missing build requirements, aborting..."
  exit 1
else
  info "Building..."
fi

# Traditional installer functions
prepare () {
  mkdir -p out/{conf,data,lib,logs,web,schema,templates}

  cp ../target/tracker-server.jar out
  cp ../target/lib/* out/lib
  cp ../schema/* out/schema
  cp -r ../templates/* out/templates
  cp -r ../traccar-web/build/* out/web
  cp traccar.xml out/conf

  if [ $PLATFORM = "all" -o $PLATFORM = "windows-64" ]; then
	innoextract i*setup-*.exe >/dev/null
	info "If you got any errors here try Inno Setup version 5.5.5 (or check supported versions using 'innoextract -v')"
  fi
}

cleanup () {
  info "Cleanup"
  rm -r out
  if [ $PLATFORM = "all" -o $PLATFORM = "windows-64" ]; then
	rm -r tmp
	rm -r app
  fi
}

package_other () {
  info "Building Zip archive"
  cp README.txt out
  cd out
  zip -q -r ../traccar-other-$VERSION.zip *
  cd ..
  rm out/README.txt
  ok "Created Zip archive"
}

package_windows () {
  info "Building Windows 64 installer"
  unzip -q OpenJDK*64_windows*.zip
  jlink --module-path jdk-*/jmods --add-modules java.se,jdk.charsets,jdk.crypto.ec,jdk.unsupported --output out/jre
  rm -rf jdk-*
  wine app/ISCC.exe traccar.iss >/dev/null
  rm -rf out/jre
  zip -q -j traccar-windows-64-$VERSION.zip Output/traccar-setup.exe README.txt
  rm -r Output
  ok "Created Windows 64 installer"
}

package_linux () {
  cp setup.sh out
  cp traccar.service out

  tar -xf OpenJDK*$2_linux*.tar.gz
  jlink --module-path jdk-*/jmods --add-modules java.se,jdk.charsets,jdk.crypto.ec,jdk.unsupported --output out/jre
  rm -rf jdk-*
  makeself --needroot --quiet --notemp out traccar.run "traccar" ./setup.sh
  rm -rf out/jre

  zip -q -j traccar-linux-$1-$VERSION.zip traccar.run README.txt

  rm traccar.run
  rm out/setup.sh
  rm out/traccar.service
}

package_linux_64 () {
  info "Building Linux 64 installer"
  package_linux 64 x64
  ok "Created Linux 64 installer"
}

package_linux_arm () {
  info "Building Linux ARM installer"
  package_linux arm aarch64
  ok "Created Linux ARM installer"
}

# Container build functions
build_container_image() {
  local service=$1
  local dockerfile="dockerfiles/${service}.Dockerfile"
  local image_tag="traccar/${service}:${VERSION}"
  local full_tag="${image_tag}"
  
  if [ -n "$REGISTRY_URL" ]; then
    full_tag="${REGISTRY_URL}/${image_tag}"
  fi
  
  # Check if Dockerfile exists
  if [ ! -f "$dockerfile" ]; then
    error "Dockerfile not found: $dockerfile"
  fi
  
  info "Building container image for ${service} service"
  
  # Build the image
  if [ "$ALPINE" = true ]; then
    # Build Alpine variant
    docker build -f "$dockerfile" \
      --target alpine \
      -t "${full_tag}-alpine" \
      --build-arg VERSION="$VERSION" \
      --build-arg BUILD_DATE="$(date -u +'%Y-%m-%dT%H:%M:%SZ')" \
      --build-arg VCS_REF="$(git rev-parse --short HEAD)" \
      ../ || error "Failed to build Alpine image for ${service}"
    
    ok "Built Alpine image: ${full_tag}-alpine"
    
    # Scan the Alpine image if requested
    if [ "$SCAN" = true ]; then
      info "Scanning ${service} Alpine image for vulnerabilities"
      trivy image --severity HIGH,CRITICAL "${full_tag}-alpine" || warn "Vulnerabilities found in ${service} Alpine image"
    fi
    
    # Push the Alpine image if requested
    if [ "$PUSH" = true ]; then
      info "Pushing ${service} Alpine image to registry"
      docker push "${full_tag}-alpine" || error "Failed to push ${service} Alpine image"
      ok "Pushed ${full_tag}-alpine to registry"
    fi
  fi
  
  # Build standard image
  docker build -f "$dockerfile" \
    -t "${full_tag}" \
    --build-arg VERSION="$VERSION" \
    --build-arg BUILD_DATE="$(date -u +'%Y-%m-%dT%H:%M:%SZ')" \
    --build-arg VCS_REF="$(git rev-parse --short HEAD)" \
    ../ || error "Failed to build image for ${service}"
  
  ok "Built image: ${full_tag}"
  
  # Tag as latest
  if [ -n "$REGISTRY_URL" ]; then
    docker tag "${full_tag}" "${REGISTRY_URL}/traccar/${service}:latest"
  else
    docker tag "${full_tag}" "traccar/${service}:latest"
  fi
  
  # Scan the image if requested
  if [ "$SCAN" = true ]; then
    info "Scanning ${service} image for vulnerabilities"
    trivy image --severity HIGH,CRITICAL "${full_tag}" || warn "Vulnerabilities found in ${service} image"
  fi
  
  # Push the image if requested
  if [ "$PUSH" = true ]; then
    info "Pushing ${service} image to registry"
    docker push "${full_tag}" || error "Failed to push ${service} image"
    
    if [ -n "$REGISTRY_URL" ]; then
      docker push "${REGISTRY_URL}/traccar/${service}:latest" || warn "Failed to push latest tag"
    fi
    
    ok "Pushed ${full_tag} to registry"
  fi
}

build_helm_chart() {
  local service=$1
  local chart_dir="helm/charts/${service}"
  
  # Check if chart directory exists
  if [ ! -d "$chart_dir" ]; then
    warn "Helm chart directory not found: $chart_dir"
    return
  fi
  
  info "Building Helm chart for ${service} service"
  
  # Update the appVersion in Chart.yaml to match the container version
  sed -i "s/^appVersion:.*/appVersion: \"${VERSION}\"/" "${chart_dir}/Chart.yaml"
  
  # Package the chart
  helm package "${chart_dir}" --destination ./helm/dist || error "Failed to package Helm chart for ${service}"
  
  ok "Built Helm chart for ${service}"
}

build_all_containers() {
  # Create directory for Helm chart packages if needed
  if [ "$HELM" = true ]; then
    mkdir -p ./helm/dist
  fi
  
  # Build container images for all services
  for service in protocol position event notification api-gateway reporting; do
    build_container_image "$service"
    
    if [ "$HELM" = true ]; then
      build_helm_chart "$service"
    fi
  done
  
  # Build main Helm chart if requested
  if [ "$HELM" = true ]; then
    info "Building main Traccar Helm chart"
    helm package ./helm --destination ./helm/dist || error "Failed to package main Helm chart"
    ok "Built main Traccar Helm chart"
  fi
}

# Main execution
if [[ $PLATFORM == container* ]]; then
  # Container build path
  if [ "$PLATFORM" = "container" ]; then
    # Build all container images
    build_all_containers
  else
    # Extract service name from platform string
    SERVICE=${PLATFORM#container:}
    
    # Validate service name
    case $SERVICE in
      protocol|position|event|notification|api-gateway|reporting)
        # Build container image for specific service
        build_container_image "$SERVICE"
        
        if [ "$HELM" = true ]; then
          mkdir -p ./helm/dist
          build_helm_chart "$SERVICE"
        fi
        ;;
      *)
        error "Invalid service name: $SERVICE"
        ;;
    esac
  fi
else
  # Traditional installer path
  prepare
  
  case $PLATFORM in
    all)
      package_linux_64
      package_linux_arm
      package_windows
      package_other
      ;;
    
    linux-64)
      package_linux_64
      ;;
    
    linux-arm)
      package_linux_arm
      ;;
    
    windows-64)
      package_windows
      ;;
    
    other)
      package_other
      ;;
  esac
  
  cleanup
fi

ok "Done"
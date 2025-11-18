#!/bin/bash

# GitHub Actions workflow translated to shell script
# This script replicates the Java CI with Gradle workflow from .github/workflows/build.yml

set -e  # Exit on error

# Configuration
REGISTRY="ghcr.io"
IMAGE_NAME="${GITHUB_REPOSITORY:-your-org/bucket-cleaner}"
IS_PR="${1:-false}"  # Pass "true" if running in PR context, otherwise "false"
BRANCH="${2:-main}"
TAG="${3:-}"  # Optional: version tag like v1.0.0
PLATFORMS="${4:-linux/amd64,linux/arm64}"  # Multi-platform targets (comma-separated)

echo "=========================================="
echo "Starting Build Pipeline"
echo "=========================================="
echo "Registry: $REGISTRY"
echo "Image: $IMAGE_NAME"
echo "Is PR: $IS_PR"
echo "Branch: $BRANCH"
echo "Platforms: $PLATFORMS"
echo ""

# Function to check command availability
check_command() {
    if ! command -v "$1" &> /dev/null; then
        echo "ERROR: $1 is not installed or not in PATH"
        exit 1
    fi
}

# Check prerequisites
echo "[1/6] Checking prerequisites..."
check_command "java"
check_command "docker"
check_command "git"

JAVA_VERSION=$(java -version 2>&1 | awk -F'"' '{print $2}')
echo "✓ Java version: $JAVA_VERSION (requires 17+)"
echo ""

# Step 1: Checkout (assumed already done when running this script)
echo "[2/6] Git status"
git status --short
echo ""

# Step 2: Build with Gradle
echo "[3/6] Building with Gradle..."
if [ -x "./gradlew" ]; then
    ./gradlew build
else
    echo "ERROR: gradlew not found. Are you in the project root?"
    exit 1
fi
echo "✓ Build completed"
echo ""

# Step 3: Set up Docker Buildx
echo "[4/6] Setting up Docker Buildx..."
if docker-buildx inspect builder &>/dev/null; then
    docker-buildx use builder
else
    docker-buildx create --name builder --use
fi
echo "✓ Docker Buildx ready"
echo ""

# Step 4: Login to Docker registry (skip if PR)
if [ "$IS_PR" = "false" ]; then
    echo "[5/6] Logging into Docker registry..."

    if [ -z "$DOCKER_USERNAME" ] || [ -z "$DOCKER_PASSWORD" ]; then
        echo "ERROR: DOCKER_USERNAME and DOCKER_PASSWORD environment variables required for non-PR builds"
        exit 1
    fi

    echo "$DOCKER_PASSWORD" | docker login -u "$DOCKER_USERNAME" --password-stdin "$REGISTRY"
    echo "✓ Logged into $REGISTRY"
else
    echo "[5/6] Skipping Docker login (PR build)"
fi
echo ""

# Step 5: Build and push Docker image
echo "[6/6] Building and pushing Docker image..."

# Extract metadata (tags and labels)
if [ -n "$TAG" ]; then
    # Build from tag
    TAGS="$REGISTRY/$IMAGE_NAME:${TAG#v}"
    LABELS="org.opencontainers.image.version=${TAG#v}"
else
    # Build from branch
    TAGS="$REGISTRY/$IMAGE_NAME:$BRANCH"
    LABELS="org.opencontainers.image.version=$BRANCH"
fi

LABELS="$LABELS,org.opencontainers.image.source=https://github.com/$IMAGE_NAME"
LABELS="$LABELS,org.opencontainers.image.revision=$(git rev-parse HEAD)"

PUSH_FLAG="false"
if [ "$IS_PR" = "false" ]; then
    PUSH_FLAG="true"
fi

echo "Building image with:"
echo "  Tags: $TAGS"
echo "  Platforms: $PLATFORMS"
echo "  Push: $PUSH_FLAG"

docker-buildx build \
    --platform "$PLATFORMS" \
    --push="$PUSH_FLAG" \
    --tag "$TAGS" \
    --label "$LABELS" \
    --cache-from=type=gha \
    --cache-to=type=gha,mode=max \
    ./app

echo "✓ Docker image built"

# Move cache
if [ "$PUSH_FLAG" = "true" ]; then
    rm -rf /tmp/.buildx-cache
    mv /tmp/.buildx-cache-new /tmp/.buildx-cache
fi
echo ""

# Step 6: Sign Docker image (skip if PR)
if [ "$IS_PR" = "false" ]; then
    echo "[Optional] Signing Docker image with cosign..."

    if command -v cosign &> /dev/null; then
        DIGEST=$(docker inspect --format='{{.RepoDigests}}' "$TAGS" | grep -oP 'sha256:\w+')

        if [ -n "$DIGEST" ]; then
            # Note: Requires COSIGN_KEY or keyless signing setup
            echo "Image digest: $DIGEST"
            echo "To sign, run: cosign sign --yes $TAGS@$DIGEST"
            echo "Note: This requires COSIGN_KEY environment variable or keyless setup"
        fi
    else
        echo "⚠ cosign not installed. Skipping image signing."
        echo "  Install from: https://docs.sigstore.dev/cosign/installation/"
    fi
else
    echo "[Optional] Skipping Docker image signing (PR build)"
fi
echo ""

# Step 7: Generate dependency graph (optional)
echo "[Optional] Generating and submitting dependency graph..."
if command -v gh &> /dev/null; then
    ./gradlew dependency-submission 2>/dev/null || echo "⚠ Dependency submission skipped (gh CLI not available)"
else
    echo "⚠ GitHub CLI (gh) not installed. Skipping dependency graph submission."
fi
echo ""

echo "=========================================="
echo "✓ Build Pipeline Completed Successfully"
echo "=========================================="

#!/bin/bash

# exit on error
set -e

# load environment variables
source .env

# If REPO_NAME is not set or differs, ensure it matches an existing repository
# adjust repo_name to a repository you have created in artifact registry
REPO_NAME=${REPO_NAME:-"vmhub-api"}

IMAGE_NAME="vmhub-sync" # adjust if needed
GCP_PROJECT_ID=${GCP_PROJECT_ID:-"semantc-ai"}
REGION=${REGION:-"us-central1"}

TAG=$(date +%Y%m%d_%H%M%S)
ARTIFACT_REGISTRY_URI="${REGION}-docker.pkg.dev/${GCP_PROJECT_ID}/${REPO_NAME}/${IMAGE_NAME}:${TAG}"
ARTIFACT_REGISTRY_LATEST_URI="${REGION}-docker.pkg.dev/${GCP_PROJECT_ID}/${REPO_NAME}/${IMAGE_NAME}:latest"

echo "configuring docker authentication for artifact registry..."
gcloud auth configure-docker ${REGION}-docker.pkg.dev

echo "building docker image..."
docker build --platform linux/amd64 -t ${IMAGE_NAME}:${TAG} .

echo "tagging image for artifact registry..."
docker tag ${IMAGE_NAME}:${TAG} ${ARTIFACT_REGISTRY_URI}
docker tag ${IMAGE_NAME}:${TAG} ${ARTIFACT_REGISTRY_LATEST_URI}

echo "pushing image to artifact registry..."
docker push ${ARTIFACT_REGISTRY_URI}
docker push ${ARTIFACT_REGISTRY_LATEST_URI}

echo "image built and pushed successfully!"
echo "image: ${ARTIFACT_REGISTRY_URI}"

echo "done."